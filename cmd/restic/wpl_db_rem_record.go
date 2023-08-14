package main

// remove obsolescent records from database

import (
	"context"
	"fmt"
	"golang.org/x/sync/errgroup"
	"time"

	//argparse
	"github.com/spf13/cobra"

	// sqlx for sqlite3
	"github.com/jmoiron/sqlx"

	// restic library
	"github.com/wplapper/restic/library/restic"
	"github.com/wplapper/restic/library/sqlite"

	// mapset
	"github.com/deckarep/golang-set/v2"
)

type RemoveSqLTable struct {
	table_name string
	sql        string
}

var cmdDBRem = &cobra.Command{
	Use:   "db-rem-record [flags]",
	Short: "remove rows from SQLite database",
	Long: `remove rows from SQLite database.

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
	DisableAutoGenTag: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runDBRem(cmd.Context(), cmd, globalOptions, args)
	},
}

func init() {
	cmdRoot.AddCommand(cmdDBRem)
	f := cmdDBRem.Flags()
	f.BoolVarP(&dbOptions.echo, "echo", "E", false, "echo database operations to stdout")
	f.BoolVarP(&dbOptions.timing, "timing", "T", false, "produce timings")
	f.BoolVarP(&dbOptions.rollback, "rollback", "R", false, "ROOLABCK databae operations")
	f.BoolVarP(&dbOptions.memory_use, "memory", "M", false, "show memory use")
}

func runDBRem(ctx context.Context, cmd *cobra.Command, gopts GlobalOptions, args []string) error {
	// step 0: setup global stuff
	var (
		repositoryData RepositoryData
		newComers      = InitNewcomers()
		db_name        string
	)
	init_repositoryData(&repositoryData)
	db_aggregate.repositoryData = &repositoryData

	start := time.Now()
	// need access to verbose option
	gOptions = gopts

	// step 1: open repository
	repo, err := OpenRepository(ctx, gopts)
	if err != nil {
		return err
	}
	if dbOptions.timing {
		timeMessage(dbOptions.memory_use, "%-30s %10.1f seconds\n", "open repository",
			time.Now().Sub(start).Seconds())
	}

	err = gather_base_data_repo(repo, gopts, ctx, &repositoryData, dbOptions.timing)
	if err != nil {
		return err
	}

	// step 4.1: get database name
	db_name, err = database_via_cache(repo, ctx)
	if err != nil {
		Printf("db_verify: could not copy database from backend %v\n", err)
		return err
	}

	// step 4.2: open selected database
	db_conn, err := sqlite.OpenDatabase(db_name, dbOptions.echo, gopts.Verbose, true)
	if err != nil {
		Printf("db_add_record: OpenDatabase failed, error is %v\n", err)
		return err
	}
	db_aggregate.db_conn = db_conn

	changed, err := run_db_rem_record(ctx, cmd, gopts, repo, &repositoryData,
		&db_aggregate, newComers, start)
	if err != nil {
		return err
	}

	// database must be written back to backend
	if changed {
		err = write_back_database(db_name, repo, ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

// run the heart of the 'db_rem_record' function
func run_db_rem_record(ctx context.Context, cmd *cobra.Command, gopts GlobalOptions,
	repo restic.Repository, repositoryData *RepositoryData, db_aggregate *DBAggregate,
	newComers *Newcomers, start time.Time) (bool, error) {

	var err error
	sql := "PRAGMA temp_store = memory"
	db_aggregate.db_conn.MustExec(sql)

	tx:= db_aggregate.db_conn.MustBegin()
	db_aggregate.tx = tx
	Printf("BEGIN TRANSACTION\n")

	names_and_counts := make(map[string]int)

	// the first three tables can be read in paralel
	wg, _ := errgroup.WithContext(ctx)
	wg.Go(func() error { return sqlite.Get_all_high_ids() })
	wg.Go(func() error { return readAllTablesAndCounts(tx, names_and_counts) })

	wg.Go(func() error { return ReadSnapshotTable(tx, db_aggregate) })
	wg.Go(func() error { return ReadNamesTable(tx, db_aggregate) })
	wg.Go(func() error { return ReadPackfilesTable(tx, db_aggregate) })
	res := wg.Wait()
	if res != nil {
		Printf("cmd_db_rem_record: error processing group 1\n")
		return false, err
	}
	db_aggregate.table_counts = names_and_counts

	// the last of the READs are sequential
	err = ReadIndexRepoTable(tx, db_aggregate)
	if err != nil {
		Printf("ReadIndexRepoTable failed with error %v\n", err)
		return false, err
	}

	// check for new packfiles, copy steps from command db_add_record
	if err := ForAllPackfiles(gopts, ctx, repositoryData, db_aggregate, newComers); err != nil {
		Printf("error in ForAllPackfiles %v\n", err)
		return false, err
	}

	// copy new rows from Mem_packfiles to Table_packfiles
	for ix, row := range newComers.Mem_packfiles {
		db_aggregate.Table_packfiles[ix] = row
	}

	changes_made := false
	column_names, err := GetColumnNames(db_aggregate.db_conn)
	if err != nil {
		return false, err
	}

	if len(newComers.Mem_packfiles) > 0 {
		err = InsertTable("packfiles", newComers.Mem_packfiles, tx, column_names, &changes_made)
		if err != nil {
			Printf("error in InsTab(packfiles) %v\n", err)
			return false, err
		}
	}

	// TABLE snapshots starts all the DELETE processing
	ProcessSnapshots(db_aggregate, repositoryData)

	// find old rows in TABLE index_repo
	for ix, row := range db_aggregate.Table_index_repo {
		blob := repositoryData.index_to_blob[ix]
		if _, ok := repositoryData.index_handle[blob]; !ok {
			row.Status = "delete"
			db_aggregate.Table_index_repo[ix] = row
		}
	}

	CreateDeleteBlobSummary(db_aggregate, repositoryData)

	// find old rows in TABLE packfiles
	CreateOldPackfiles(db_aggregate, repositoryData)
	// find old rows in TABLE names
	CreateOldMemNames(db_aggregate, repositoryData)

	if dbOptions.timing {
		timeMessage(dbOptions.memory_use, "%-30s %10.1f seconds\n",
			"find old rows - 4 tables", time.Now().Sub(start).Seconds())
	}

	// process index_repo changes
	err = db_upd_index_repo(db_aggregate, repositoryData)
	if err != nil {
		return false, err
	}

	changes_made, _ = modify_database_tables(db_aggregate, repositoryData)
	if dbOptions.timing {
		timeMessage(dbOptions.memory_use, "%-30s %10.1f seconds\n",
			"after  modify_database_tables", time.Now().Sub(start).Seconds())
	}
	return changes_made, nil
}

func db_upd_index_repo(db_aggregate *DBAggregate, repositoryData *RepositoryData) error {
	// if restic pruned the repository earlier, meta_blobs have been allocated to
	// new packfiles, therefore the table 'index_repo' needs updating.
	count_updates := 0
	// update index_repo.id_pack_id, packfiles.id is master here!
	for id, ih := range repositoryData.index_handle {
		id_int := repositoryData.blob_to_index[id]
		db_index_repo, ok := db_aggregate.Table_index_repo[id_int]
		if !ok {
			packID := repositoryData.index_to_blob[id_int]
			Printf("missing entry for blob %s at index %6d\n", packID.String()[:12], id_int)
			panic("update_index_repo: index_repo row not found in database")
		}

		pack_index := ih.pack_index
		pack_row, ok := db_aggregate.Table_packfiles[pack_index]
		if !ok {
			packID := repositoryData.index_to_blob[pack_index]
			Printf("missing packfile %s\n", packID.String()[:12])
			panic("update_index_repo: packfiles row not found in database")
		}

		if db_index_repo.Id_pack_id != pack_row.Id {
			db_index_repo.Status = "update"
			db_index_repo.Id_pack_id = pack_row.Id
			db_aggregate.Table_index_repo[id_int] = db_index_repo
			count_updates++
		}
	}
	return nil
}

type process_remove func(*sqlx.Tx, *bool) error

// make changes to the database. Use TEMP TABLE 'remove_snaps' to fire up
// various DELETE requests from the dependent tables 'meta_dir', 'idd_file' and
// 'contents'.
// Remove rows from TABLEs 'names', 'packfiles' and 'index_repo' which where marked
// as DELETE.
// UPDATE rows from index_repo where the packfile has been changed during the
// pruning process.
// Last UPDATE the timestamp in the database.
func modify_database_tables(db_aggregate *DBAggregate, repositoryData *RepositoryData) (bool, error) {

	var (
		changes_made = false
		err          error
		do_the_jobs  = []process_remove{
			removeSecondaryTables,
			removePrimaryTables,
			updateIndexRepoTable,
		}
	)

	tx := db_aggregate.tx
	for _, do_one_job := range do_the_jobs {
		err := do_one_job(tx, &changes_made)
		if err != nil {
			return false, err
		}
	}

	scrub_superfluous_rows(db_aggregate.tx, &changes_made)

	// update timestamp
	if changes_made {
		if err := db_update_timestamp(db_aggregate.Table_snapshots, tx,
			&changes_made); err != nil {
			Printf("update timestamp failed: error is %v\n", err)
		}
	}

	// COMMIT or ROLLBACK transaction
	if !dbOptions.rollback && changes_made {
		Printf("COMMIT\n")
		err = tx.Commit()
		if err != nil {
			Printf("COMMIT error %v\n", err)
			return false, err
		}
	} else {
		Printf("ROLLBACK\n")
		tx.Rollback()
		changes_made = false
	}
	return changes_made, nil
}

// compare snapshots on database with snapshots from repository
// if snapshot not in repository, mark it "DELETE" and create TEMP TABLE,
// fill TEMP TABLE
func ProcessSnapshots(db_aggregate *DBAggregate, repositoryData *RepositoryData) error {
	t_insert := make([]RemoveTable, 0)
	for snap_id, row := range db_aggregate.Table_snapshots {
		if _, ok := repositoryData.snap_map[snap_id]; !ok {
			t_insert = append(t_insert, RemoveTable{Id: row.Id})
		}
	}

	// create TEMP TABLE remove_snaps
	sqll := "CREATE TEMP TABLE remove_snaps (id INTEGER)"
	if dbOptions.echo {
		Printf("%s %s\n", time.Now().Format("2006-01-02 15:04:05.000"), sqll)
	}
	db_aggregate.tx.MustExec(sqll)

	// fill TEMP TABLE remove_snaps
	if len(t_insert) > 0 {
		sql := "INSERT INTO remove_snaps(id) VALUES(:id)"
		_, err := db_aggregate.tx.NamedExec(sql, t_insert)
		if err != nil {
			Printf("error INSERTing into TEMP TABLE remove_snaps: %v\n", err)
			return err
		}
	}

	var temp_table = `CREATE TEMP TABLE meta_blobs AS
    SELECT index_repo.id FROM remove_snaps
      JOIN meta_dir   ON meta_dir.id_snap_id = remove_snaps.id
      JOIN index_repo ON meta_dir.id_idd     = index_repo.id
    EXCEPT
    SELECT index_repo.id FROM snapshots
      JOIN meta_dir   ON meta_dir.id_snap_id = snapshots.id
      JOIN index_repo ON meta_dir.id_idd     = index_repo.id
      WHERE snapshots.id NOT IN (SELECT id FROM remove_snaps)`

	if dbOptions.echo {
		Printf("%s %s\n", time.Now().Format("2006-01-02 15:04:05.000"), temp_table)
	}
	db_aggregate.tx.MustExec(temp_table)
	return nil
}

// print a brief summary of changes in meta and datablobs, counts an sizes
func CreateDeleteBlobSummary(db_aggregate *DBAggregate, repositoryData *RepositoryData) {
	// create blob summary
	sum_data_blobs := 0
	sum_meta_blobs := 0
	count_meta_blobs := 0
	count_data_blobs := 0
	for _, row := range db_aggregate.Table_index_repo {
		if row.Status == "delete" {
			typ := row.Index_type[0:1]
			if typ == "t" {
				sum_meta_blobs += row.Idd_size
				count_meta_blobs++
			} else if typ == "d" {
				sum_data_blobs += row.Idd_size
				count_data_blobs++
			}
		}
	}
	Printf("\n*** Blob summary to be deleted from the database ***\n")
	Printf("bl-type count       size\n")
	Printf("%s %7d %10.3f MiB\n", "meta", count_meta_blobs, float64(sum_meta_blobs)/ONE_MEG)
	Printf("%s %7d %10.3f MiB\n", "data", count_data_blobs, float64(sum_data_blobs)/ONE_MEG)
	Printf("%s %7d %10.3f MiB\n", "sum ", count_data_blobs + count_meta_blobs,
		float64(sum_data_blobs + sum_meta_blobs)/ONE_MEG)
	Printf("\n")
}

// remove rows from TABLEs 'names', 'packfiles' and 'index_repo'
func removePrimaryTables(tx *sqlx.Tx, changes_made *bool) error {
	var sqll = []RemoveSqLTable{
		RemoveSqLTable{"names", "CREATE TEMP TABLE delete_names (id INTEGER PRIMARY KEY)"},
		RemoveSqLTable{"packfiles", "CREATE TEMP TABLE delete_packfiles (id INTEGER PRIMARY KEY)"},
		RemoveSqLTable{"index_repo", "CREATE TEMP TABLE delete_index_repo (id INTEGER PRIMARY KEY)"},
	}
	for _, sql := range sqll {
		if dbOptions.echo {
			Printf("%s %s\n", time.Now().Format("2006-01-02 15:04:05.000"), sql.sql)
		}
		tx.MustExec(sql.sql)
	}

	// DELETE FROM names
	sql := "INSERT INTO delete_names(id) VALUES(:id)"
	if dbOptions.echo {
		Printf("%s %s\n", time.Now().Format("2006-01-02 15:04:05.000"), sql)
	}
	name_stmt, err := tx.Prepare(sql)
	if err != nil {
		Printf("Error Prepare %s error is %v\n", sql, err)
		return err
	}

	count := 0
	for _, row := range db_aggregate.Table_names {
		if row.Status == "delete" {
			// individual INSERT statement INTO TEMP TABLE 'delete_names'
			_, err := name_stmt.Exec(row.Id)
			if err != nil {
				Printf("Error INSERT INFO delete_names, error is %v\n", err)
				return err
			}
			count++
		}
	}
	name_stmt.Close()
	if dbOptions.echo && count > 0 {
		Printf("%s %s %5d rows\n", time.Now().Format("2006-01-02 15:04:05.000"),
			sql, count)
	}

	// DELETE FROM packfiles
	sql = "INSERT INTO delete_packfiles(id) VALUES(:id)"
	if dbOptions.echo {
		Printf("%s %s\n", time.Now().Format("2006-01-02 15:04:05.000"), sql)
	}
	pack_stmt, err := tx.Prepare(sql)
	if err != nil {
		Printf("Error Prepare %s, error is %v\n", sql, err)
		return err
	}

	count = 0
	for _, row := range db_aggregate.Table_packfiles {
		if row.Status == "delete" {
			// individual INSERT statements into TEMP TABLE 'delete_packfiles'
			_, err := pack_stmt.Exec(row.Id)
			if err != nil {
				Printf("Error INSERT INFO delete_packfiles, error is %v\n", err)
				return err
			}
			count++
		}
	}
	pack_stmt.Close()
	if dbOptions.echo && count > 0 {
		Printf("%s %s %5d rows\n", time.Now().Format("2006-01-02 15:04:05.000"),
			sql, count)
	}

	// DELETE FROM index_repo - INSERT INTO temp TABLE
	sql = "INSERT INTO delete_index_repo(id) VALUES(:id)"
	if dbOptions.echo {
		Printf("%s %s\n", time.Now().Format("2006-01-02 15:04:05.000"), sql)
	}
	repo_stmt, err := tx.Prepare(sql)
	if err != nil {
		Printf("Error Prepare %s error is %v\n", sql, err)
		return err
	}

	count = 0
	for _, row := range db_aggregate.Table_index_repo {
		if row.Status == "delete" {
			// individual INSERT into TEMP TABLE 'delete_index_repo'
			_, err := repo_stmt.Exec(row.Id)
			if err != nil {
				Printf("Error INSERT INFO delete_index_repo, error is %v\n", err)
				return err
			}
			count++
		}
	}
	repo_stmt.Close()
	if dbOptions.echo && count > 0 {
		Printf("%s %s\n", time.Now().Format("2006-01-02 15:04:05.000"), sql)
	}

	var del_stmts = []RemoveSqLTable{
		RemoveSqLTable{"names", "DELETE FROM names WHERE id IN (SELECT id FROM delete_names)"},
		RemoveSqLTable{"packfiles", "DELETE FROM packfiles WHERE id IN (SELECT id FROM delete_packfiles)"},
		RemoveSqLTable{"index_repo", "DELETE FROM index_repo WHERE id IN (SELECT id FROM delete_index_repo)"},
	}
	for _, del_stmt := range del_stmts {
		r := tx.MustExec(del_stmt.sql)
		if dbOptions.echo {
			Printf("%s %s\n", time.Now().Format("2006-01-02 15:04:05.000"), del_stmt.sql)
		}
		count, _ := r.RowsAffected()
		if count > 0 {
			*changes_made = true
			Printf("DELETE %-15s %5d rows deleted.\n", del_stmt.table_name, count)
		}
	}
	return nil
}

// remove rows from TABLEs which are defined by contents of TEMP TABLEs
func removeSecondaryTables(tx *sqlx.Tx, changes_made *bool) error {
	var sqld = []RemoveSqLTable{
		RemoveSqLTable{"snapshots",
			`DELETE FROM snapshots WHERE snapshots.id IN (SELECT id FROM remove_snaps)`},
		RemoveSqLTable{"meta_dir",
			`DELETE FROM meta_dir WHERE meta_dir.id_snap_id IN (SELECT id FROM remove_snaps)`},
		RemoveSqLTable{"contents",
			`DELETE FROM contents WHERE contents.id_blob IN (SELECT id FROM meta_blobs)`},
		RemoveSqLTable{"idd_file",
			`DELETE FROM idd_file WHERE idd_file.id_blob IN (SELECT id FROM meta_blobs)`},
		RemoveSqLTable{"dir_path_id",
			`DELETE FROM dir_path_id WHERE dir_path_id.id IN (SELECT id FROM meta_blobs)`},
	}

	for _, sql := range sqld {
		if dbOptions.echo {
			Printf("%s %s\n", time.Now().Format("2006-01-02 15:04:05.000"), sql.sql)
		}
		r:= tx.MustExec(sql.sql)
		count, _ := r.RowsAffected()
		if count > 0 {
			*changes_made = true
			Printf("DELETE %-15s %5d rows deleted.\n", sql.table_name, count)
		}
	}
	return nil
}

// UPDATE rows in TABLE index_repo which have been moved to new packfiles
func updateIndexRepoTable(tx *sqlx.Tx, changes_made *bool) error {
	// we need a TEMP TABLE which contains the move blobs -> packfiles relationship
	sql := "CREATE TEMP TABLE temp_update_ix_repo (id INTEGER PRIMARY KEY, id_pack_id INTEGER)"
	if dbOptions.echo {
		Printf("%s %s\n", time.Now().Format("2006-01-02 15:04:05.000"), sql)
	}
	tx.MustExec(sql)

	// prepare INSERT into TEMP TABLE
	sql = "INSERT INTO temp_update_ix_repo(id, id_pack_id) VALUES(:id, :id_pack_id)"
	if dbOptions.echo {
		Printf("%s %s\n", time.Now().Format("2006-01-02 15:04:05.000"), sql)
	}
	stmt, err := tx.Prepare(sql)
	if err != nil {
		Printf("error Prepare INSERT temp_update_ix_repo: %v\n", err)
		return err
	}

	count := int64(0)
	for _, row := range db_aggregate.Table_index_repo {
		if row.Status == "update" {
			// INSERT
			_, err := stmt.Exec(row.Id, row.Id_pack_id)
			if err != nil {
				Printf("Error INSERT temp_update_ix_repo, error is %v\n", err)
				return err
			}
			count++
		}
	}
	stmt.Close()
	if dbOptions.echo && count > 0 {
		Printf("%s %s %5d rows updated\n", time.Now().Format("2006-01-02 15:04:05.000"),
			sql, count)
	}

	// one BIG UPDATE: UPDATE index_repo SET ... FROM temp_update_ix_repo
	sql = `
    UPDATE index_repo SET id_pack_id = temp_update_ix_repo.id_pack_id
    FROM temp_update_ix_repo WHERE index_repo.id = temp_update_ix_repo.id`
	r := tx.MustExec(sql)
	if dbOptions.echo {
		Printf("%s %s\n", time.Now().Format("2006-01-02 15:04:05.000"), sql)
	}

	count, _ = r.RowsAffected()
	if count > 0 {
		*changes_made = true
		Printf("UPDATE repo_index %10d rows updated.\n", count)
	}
	return nil
}

// remove rows in tables, which reference other tables
// i.e. foreign key reference check
func scrub_superfluous_rows(tx *sqlx.Tx, changes_made *bool) error {
	type ForeignKeys struct {
		check_table string
		column_name string
		ref_table   string // the implied column is always "Id" for the ref_table
	}
	var check_tables = []ForeignKeys{
		// fill sql statement place holders <ct>, <cn>, <rt>
		// DELETE FROM <ct> WHERE <cn> IN (SELECT DISTINCT <ct>.<cn> FROM <ct>
		// LEFT OUTER JOIN <rt> ON <rt>.<cn> = <ct>.id WHERE <ct>.id IS NULL)
		{"meta_dir", "Id_snap_id", "snapshots"},
		{"meta_dir", "Id_idd", "index_repo"},
		{"idd_file", "Id_blob", "index_repo"},
		{"contents", "Id_data_idd", "index_repo"},
		{"contents", "Id_blob", "index_repo"},
		{"idd_file", "Id_name", "names"},
		{"index_repo", "Id_pack_id", "packfiles"},
		{"dir_path_id", "Id", "index_repo"},
		{"dir_path_id", "Id_pathname", "fullname"},
	}

	for _, action := range check_tables {
		sql := fmt.Sprintf(`DELETE FROM %s WHERE %s IN (SELECT DISTINCT %s.%s FROM %s
  LEFT OUTER JOIN %s ON %s.%s = %s.id WHERE %s.id IS NULL)`,
			action.check_table, action.column_name,
			action.check_table, action.column_name,
			action.check_table, action.ref_table,
			action.check_table, action.column_name,
			action.ref_table, action.ref_table)

		if dbOptions.echo {
			Printf("%s %s\n", time.Now().Format("2006-01-02 15:04:05.000"), sql)
		}
		r := tx.MustExec(sql)
		count, _ := r.RowsAffected()
		if count > 0 {
			Printf("DELETE %-15s %6d rows deleted.\n", action.check_table, count)
			*changes_made = true
		}
	}
	return nil
}

// detect names in TABLE 'names' which are old and not longer in the repository
func CreateOldMemNames(db_aggregate *DBAggregate, repositoryData *RepositoryData) {

	// step1: extract names from idd_file
	seen := mapset.NewSet[string]()
	for _, file_list := range repositoryData.directory_map {
		for _, meta := range file_list {
			switch meta.Type {
			case "file", "dir":
				seen.Add(meta.name)
			}
		}
	}

	for name, row := range db_aggregate.Table_names {
		if seen.Contains(name) {
			continue
		}

		// mark for deletion
		row.Status = "delete"
		db_aggregate.Table_names[name] = row
	}
	seen = nil
}

// detect names in TABLE 'packfiles' which are old and not longer in the repository
func CreateOldPackfiles(db_aggregate *DBAggregate, repositoryData *RepositoryData) {
	// collect all packfiles from the index_handle
	pack_intIDs := mapset.NewSet[IntID]()
	for _, handle := range repositoryData.index_handle {
		pack_intIDs.Add(handle.pack_index)
	}

	// collect all packfiles from the database records (TABLE 'packfiles')
	for pack_intID, row := range db_aggregate.Table_packfiles {
		if pack_intIDs.Contains(pack_intID) {
			continue
		}

		// mark row for deletion which are still in the database but not in the
		// repository
		row.Status = "delete"
		db_aggregate.Table_packfiles[pack_intID] = row
	}
	pack_intIDs = nil
}
