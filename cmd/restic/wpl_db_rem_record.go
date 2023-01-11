package main

// compile with "go run build.go -tags debug""
// run with DEBUG_LOG=/home/wplapper/restic/debug.log fully-qualified-name/restic -r <repo> overview

// ref to onedrive: rclone:onedrive:restic_backups

import (
	"golang.org/x/sync/errgroup"
	"time"
	"fmt"

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
	Use:   "db_rem_record [flags]",
	Short: "remove rows from SQLite database",
	Long: `remove rows from SQLite database.

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
	DisableAutoGenTag: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runDBRem(globalOptions, args)
	},
}

func init() {
	cmdRoot.AddCommand(cmdDBRem)
	flags := cmdDBRem.Flags()
	flags.BoolVarP(&dbOptions.echo, "echo", "E", false, "echo database operations to stdout")
	flags.BoolVarP(&dbOptions.rollback, "rollback", "R", false, "ROOLABCK databae operations")
	flags.BoolVarP(&dbOptions.timing, "timing", "T", false, "produce timings")
	flags.StringVarP(&dbOptions.altDB, "DB", "", "", "aternative database name")
}

func runDBRem(gopts GlobalOptions, args []string) error {
	// step 0: setup global stuff
	var (
		repositoryData = init_repositoryData() // is a *RepositoryData
		newComers = InitNewcomers()
		db_name string
		ok bool
	)
	start := time.Now()
	db_aggregate.repositoryData = repositoryData
	// EMPTY_NODE_ID = ac08ce34ba4f8123618661bef2425f7028ffb9ac740578a3ee88684d2523fee8
	EMPTY_NODE_ID = restic.Hash([]byte("{\"nodes\":[]}\n"))
	// need access to verbose option
	gOptions = gopts

	// step 1: open repository
	repo, err := OpenRepository(gopts)
	if err != nil {
		return err
	}
	Printf("Repository is %s\n", gopts.Repo)
	if dbOptions.timing {
		timeMessage("%-30s %10.1f seconds\n", "open repository",
			time.Now().Sub(start).Seconds())
	}

	repositoryData.snaps, err = GatherAllSnapshots(gopts, repo)
	if err != nil {
		return err
	}
	repositoryData.snap_map = make(map[string]*restic.Snapshot)
	for _, sn := range repositoryData.snaps {
		repositoryData.snap_map[sn.ID().Str()] = sn
	}
	if dbOptions.timing {
		timeMessage("%-30s %10.1f seconds\n", "gather snapshots",
			time.Now().Sub(start).Seconds())
	}

	// step 2: manage Index Records
	if err = HandleIndexRecords(gopts, repo, repositoryData); err != nil {
		return err
	}
	if dbOptions.timing {
		timeMessage("%-30s %10.1f seconds\n", "read index records",
			time.Now().Sub(start).Seconds())
	}

	// step 3: collect all snapshot related information
	//start = time.Now()
	GatherAllRepoData(gopts, repo, repositoryData)
	if dbOptions.timing {
		timeMessage("%-30s %10.1f seconds\n", "GatherAllRepoData",
			time.Now().Sub(start).Seconds())
	}

	// step 4.1: get database name
	if dbOptions.altDB != "" {
		db_name = dbOptions.altDB
	} else {
		db_name, ok = DATABASE_NAMES[gopts.Repo]
		if !ok {
			Printf("database name for repo %s is missing!\n", gopts.Repo)
			return nil
		}
	}

	// step 4.2: open selected database
	db_conn, err := sqlite.OpenDatabase(db_name, true, 1, true)
	if err != nil {
		Printf("db_add_record: OpenDatabase failed, error is %v\n", err)
		return err
	}
	db_aggregate.db_conn = db_conn

	sql := "PRAGMA temp_store = memory"
	_, err = db_aggregate.db_conn.Exec(sql)
	if err != nil {
		Printf("Can't set PRAGMA temp_store, error is %v\n", err)
		return err
	}

	tx, err := db_aggregate.db_conn.Beginx()
	if err != nil {
		Printf("Cant start transaction. Error is %v\n", err)
		return err
	}
	db_aggregate.tx = tx
	Printf("BEGIN TRANSACTION\n")
	//PrintMemUsage()
	//ConfirmStdin()

	names_and_counts := make(map[string]int)

	// the first three tables can be read in paralel
	wg, _ := errgroup.WithContext(gopts.ctx)
	wg.Go(func() error { return sqlite.Get_all_high_ids() })
	wg.Go(func() error { return readAllTablesAndCounts(db_conn, names_and_counts) })

	wg.Go(func() error { return ReadSnapshotTable(tx, &db_aggregate) })
	wg.Go(func() error { return ReadNamesTable(tx, &db_aggregate) })
	wg.Go(func() error { return ReadPackfilesTable(tx, &db_aggregate) })
	res := wg.Wait()
	if res != nil {
		Printf("cmd_db_rem_record: error processing group 1\n")
		return res
	}
	db_aggregate.table_counts = names_and_counts

	// the last of the READs are sequential
	err = ReadIndexRepoTable(tx, &db_aggregate)
	if err != nil {
		Printf("ReadIndexRepoTable failed with error %v\n", err)
		return err
	}
	/*if dbOptions.timing {
		timeMessage("%-30s %10.1f seconds\n", "After reading 4 tables",
			time.Now().Sub(start).Seconds())
	}*/

	// TABLE snapshots triggers all the DELETE processing
	ProcessSnapshots(&db_aggregate, repositoryData, newComers)

	// TABLE packfiles
	newComers.Mem_packfiles = CreateMemPackfiles(&db_aggregate, repositoryData, newComers)
	newComers.old_packfiles = OldDBKeys(db_aggregate.Table_packfiles, newComers.Mem_packfiles)
	for packID := range newComers.old_packfiles.Iter() {
		row := db_aggregate.Table_packfiles[packID]
		row.Status = "delete"
		db_aggregate.Table_packfiles[packID] = row
	}
	newComers.Mem_packfiles = nil
	newComers.old_packfiles = nil

	// TABLE index_repo
	for ix, row := range db_aggregate.Table_index_repo {
		blob := repositoryData.index_to_blob[ix]
		if _, ok := repositoryData.index_handle[blob]; !ok {
			row.Status = "delete"
			db_aggregate.Table_index_repo[ix] = row
		}
	}
	if dbOptions.timing {
		timeMessage("%-30s %10.1f seconds\n", "find old rows - 4 tables",
			time.Now().Sub(start).Seconds())
	}

	CreateDeleteBlobSummary(&db_aggregate, repositoryData)

	// TABLE names
	newComers.Mem_names = CreateMemNames(&db_aggregate, repositoryData, newComers)
	newComers.old_names = OldDBKeys(db_aggregate.Table_names, newComers.Mem_names)
	for name := range newComers.old_names.Iter() {
		row := db_aggregate.Table_names[name]
		row.Status = "delete"
		db_aggregate.Table_names[name] = row
	}
	newComers.Mem_names = nil
	newComers.old_names = nil

	// process index_repo changes
	err = db_rem_index_repo(&db_aggregate, repositoryData, newComers)
	if err != nil {
		return err
	}

	// modify database tables
	if dbOptions.timing {
		timeMessage("%-30s %10.1f seconds\n", "before modify_database_tables",
			time.Now().Sub(start).Seconds())
	}
	modify_database_tables(&db_aggregate, repositoryData, newComers)
	if dbOptions.timing {
		timeMessage("%-30s %10.1f seconds\n", "after  modify_database_tables",
			time.Now().Sub(start).Seconds())
	}
	return nil
}

func db_rem_index_repo(db_aggregate *DBAggregate, repositoryData *RepositoryData,
	newComers *Newcomers) error {

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
		Printf("%s\n", temp_table)
	}
	_, err := db_aggregate.tx.Exec(temp_table)
	if err != nil {
		Printf("error creating TEMP TABLE %s: %v\n", temp_table, err)
		return err
	}

	count_updates := 0
	// update index_repo.id_pack_id, packfiles.id is master here!
	for id, ih := range repositoryData.index_handle {
		id_int := repositoryData.blob_to_index[id]
		db_index_repo, ok := (db_aggregate.Table_index_repo)[id_int]
		if !ok {
			packID := repositoryData.index_to_blob[id_int]
			Printf("missing entry for blob %s at index %6d\n", packID.String()[:12], id_int)
			panic("update_index_repo: index_repo row not found in database")
		}

		pack_index := ih.pack_index
		pack_row, ok := (db_aggregate.Table_packfiles)[pack_index]
		if !ok {
			packID := repositoryData.index_to_blob[pack_index]
			Printf("missing packfile %s for index %6d\n", packID.String()[:12], pack_index)
			Printf("Run db_add_record first!\n")
			panic("update_index_repo: packfiles row not found in database")
		}

		if db_index_repo.Id_pack_id != pack_row.Id {
			db_index_repo.Status = "update"
			db_index_repo.Id_pack_id = pack_row.Id
			(db_aggregate.Table_index_repo)[id_int] = db_index_repo
			count_updates++
		}
	}
	return nil
}

type process_remove func(*sqlx.Tx, *DBAggregate, *bool) error

// make changes to the database. Use TEMP TABLE 'remove_snaps' to fire up
// various DELETE requests from the dependent tables 'meta_dir', 'idd_file' and
// 'contents'.
// Remove rows from TABLEs 'names', 'packfiles' and 'index_repo' which where marked
// as DELETE.
// UPDATE rows from index_repo where the packfile has been changed during the
// pruning process.
// Last UPDATE the timestamp in the database.
func modify_database_tables(db_aggregate *DBAggregate, repositoryData *RepositoryData,
	newComers *Newcomers) error {

	var (
		changes_made = false
		err error
		do_the_jobs = []process_remove{
			removeSecondaryTables,
			removePrimaryTables,
			updateIndexRepoTable,
	})

	tx := db_aggregate.tx
	for _, do_one_job := range do_the_jobs {
		err := do_one_job(tx, db_aggregate, &changes_made)
		if err != nil {
			return err
		}
	}
	scrub_superfluous_rows(db_aggregate.tx)

	// update timestamp
	if err := db_update_timestamp(db_aggregate.Table_snapshots, tx); err != nil {
		Printf("update timestamp failed: error is %v\n", err)
	}

	// COMMIT or ROLLBACK transaction
	if !dbOptions.rollback && changes_made {
		Printf("COMMIT\n")
		err = tx.Commit()
		if err != nil {
			Printf("COMMIT error %v\n")
			return err
		}

		// now VACUUM
		Printf("VACUUM\n")
		_, err = db_aggregate.db_conn.Exec("VACUUM")
		if err != nil {
			Printf("VACUUM failed. Error is %v\n", err)
		}
		return err
	} else {
		Printf("ROLLBACK\n")
		tx.Rollback()
	}
	return nil
}

// compare snapshots on database with snapshots from repository
// if snapshot not in repository, mark it "DELETE" and create TEMP TABLE,
// fill TEMP TABLE
func ProcessSnapshots(db_aggregate *DBAggregate, repositoryData *RepositoryData,
	newComers *Newcomers) error {
	t_insert := make([]RemoveTable, 0)
	for snap_id, row := range db_aggregate.Table_snapshots {
		if _, ok := repositoryData.snap_map[snap_id]; !ok {
			t_insert = append(t_insert, RemoveTable{Id: row.Id})
		}
	}

	// create TEMP TABLE remove_snaps
	_, err := db_aggregate.tx.Exec("CREATE TEMP TABLE remove_snaps (id INTEGER)")
	if err != nil {
		Printf("error creating TEMP TABLE remove_snaps: %v\n", err)
		return err
	}

	// fill TEMP TABLE remove_snaps
	if len(t_insert) > 0 {
		sql := "INSERT INTO remove_snaps(id) VALUES(:id)"
		_, err := db_aggregate.tx.NamedExec(sql, t_insert)
		if err != nil {
			Printf("error INSERTing into TEMP TABLE remove_snaps: %v\n", err)
			return err
		}
		//Printf("INSERT remove_snaps    %5d rows inserted.\n", len(t_insert))
	}
	return nil
}

// print a brief summary of changes in meta and datablobs, countd ans sizes
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
	Printf("\n*** Summary of data to be deleted from database ***\n")
	Printf("type   count       size\n")
	Printf("%s %7d %10.3f MiB\n", "meta", count_meta_blobs, float64(sum_meta_blobs)/ONE_MEG)
	Printf("%s %7d %10.3f MiB\n", "data", count_data_blobs, float64(sum_data_blobs)/ONE_MEG)
	Printf("%s %7d %10.3f MiB\n", "sum ", count_data_blobs + count_meta_blobs,
		float64(sum_data_blobs + sum_meta_blobs) / ONE_MEG)
	Printf("\n")
}

// remove rows from TABLEs 'names', 'packfiles' and 'index_repo'
func removePrimaryTables(tx *sqlx.Tx, db_aggregate *DBAggregate, changes_made *bool) error {
	var sqlc = []RemoveSqLTable{
		RemoveSqLTable{"names", "CREATE TEMP TABLE delete_names (id INTEGER PRIMARY KEY)"},
		RemoveSqLTable{"packfiles", "CREATE TEMP TABLE delete_packfiles (id INTEGER PRIMARY KEY)"},
		RemoveSqLTable{"index_repo", "CREATE TEMP TABLE delete_index_repo (id INTEGER PRIMARY KEY)"},
	}
	for _, sql := range sqlc {
		if dbOptions.echo {
			Printf("%s\n", sql.sql)
		}

		_, err := tx.Exec(sql.sql)
		if err != nil {
			Printf("error CREATE %s error is: %v\n", sql.table_name, err)
			return err
		}
	}

	// DELETE FROM names
	sql := "INSERT INTO delete_names(id) VALUES(:id)"
	name_stmt, err := tx.Prepare(sql)
	if err != nil {
		Printf("Error Prepare %s error is %v\n", sql, err)
		return err
	}

	for _, row := range db_aggregate.Table_names {
		if row.Status == "delete" {
			// INSERT INTO delete_names
			_, err := name_stmt.Exec(row.Id)
			if err != nil {
				Printf("Error INSERT INFO delete_names, error is %v\n", err)
				return err
			}
		}
	}
	name_stmt.Close()

	// DELETE FROM packfiles
	sql = "INSERT INTO delete_packfiles(id) VALUES(:id)"
	pack_stmt, err := tx.Prepare(sql)
	if err != nil {
		Printf("Error Prepare %s, error is %v\n", sql, err)
		return err
	}

	for _, row := range db_aggregate.Table_packfiles {
		if row.Status == "delete" {
			// INSERT
			_, err := pack_stmt.Exec(row.Id)
			if err != nil {
				Printf("Error INSERT INFO delete_packfiles, error is %v\n", err)
				return err
			}
		}
	}
	pack_stmt.Close()

	// DELETE FROM index_repo - INSERT INTO temp TABLE
	sql = "INSERT INTO delete_index_repo(id) VALUES(:id)"
	repo_stmt, err := tx.Prepare(sql)
	if err != nil {
		Printf("Error Prepare %s error is %v\n", sql, err)
		return err
	}

	for _, row := range db_aggregate.Table_index_repo {
		if row.Status == "delete" {
			// INSERT
			_, err := repo_stmt.Exec(row.Id)
			if err != nil {
				Printf("Error INSERT INFO delete_index_repo, error is %v\n", err)
				return err
			}
		}
	}
	repo_stmt.Close()

	var del_stmts = []RemoveSqLTable{
		RemoveSqLTable{"names", "DELETE FROM names WHERE id IN (SELECT id FROM delete_names)"},
		RemoveSqLTable{"packfiles", "DELETE FROM packfiles WHERE id IN (SELECT id FROM delete_packfiles)"},
		RemoveSqLTable{"index_repo", "DELETE FROM index_repo WHERE id IN (SELECT id FROM delete_index_repo)"},
	}
	for _, del_stmt := range del_stmts {
		r, err := tx.Exec(del_stmt.sql)
		if err != nil {
			Printf("error DELETE from %s, error id %v\n", del_stmt.table_name, err)
			return err
		}
		count, _ := r.RowsAffected()
		Printf("DELETE %-15s %5d rows deleted.\n", del_stmt.table_name, count)
		*changes_made = true
	}
	return nil
}

// remove rows from TABLEs which are defined by contents of TEMP TABLEs
func removeSecondaryTables(tx *sqlx.Tx, db_aggregate *DBAggregate, changes_made *bool) error {
	var sqld = []RemoveSqLTable{
		RemoveSqLTable{"snapshots",
			`DELETE FROM snapshots WHERE snapshots.id IN (SELECT id FROM remove_snaps)`},
		RemoveSqLTable{"meta_dir",
			`DELETE FROM meta_dir WHERE meta_dir.id_snap_id IN (SELECT id FROM remove_snaps)`},
		RemoveSqLTable{"contents",
			`DELETE FROM contents WHERE contents.id_blob IN (SELECT id FROM meta_blobs)`},
		RemoveSqLTable{"idd_file",
			`DELETE FROM idd_file WHERE idd_file.id_blob IN (SELECT id FROM meta_blobs)`},
		RemoveSqLTable{"dir_children",
			`DELETE FROM dir_children WHERE dir_children.id_parent IN (SELECT id FROM meta_blobs)`},
		RemoveSqLTable{"dir_children",
			`DELETE FROM dir_children WHERE dir_children.id_child IN (SELECT id FROM meta_blobs)`},
		RemoveSqLTable{"dir_path_id",
			`DELETE FROM dir_path_id WHERE dir_path_id.id IN (SELECT id FROM meta_blobs)`},
		RemoveSqLTable{"dir_name_id",
			`DELETE FROM dir_name_id WHERE dir_name_id.id IN (SELECT id FROM meta_blobs)`},
	}

	for _, sql := range sqld {
		if dbOptions.echo {
			Printf("%s\n", sql.sql)
		}

		r, err := tx.Exec(sql.sql)
		if err != nil {
			Printf("DELETE %s error is: %v\n", sql.table_name, err)
			return err
		}

		count, _ := r.RowsAffected()
		Printf("DELETE %-15s %5d rows deleted.\n", sql.table_name, count)
		if count > 0 {
			*changes_made = true
		}
	}
	return nil
}

// UPDATE rows in TABLE index_repo which have been moved to new packfiles
func updateIndexRepoTable(tx *sqlx.Tx, db_aggregate *DBAggregate, changes_made *bool) error {
	// we need a TEMP TABLE which contains the move blobs -> packfiles relationship
	sql := "CREATE TEMP TABLE temp_update_ix_repo (id INTEGER PRIMARY KEY, id_pack_id INTEGER)"
	_, err := tx.Exec(sql)
	if err != nil {
		Printf("Error CREATE TEMP TABLE, err is %v\n", err)
		return err
	}

	// prepare INSERT into TEMP TABLE
	sql = "INSERT INTO temp_update_ix_repo(id, id_pack_id) VALUES(:id, :id_pack_id)"
	stmt, err := tx.Prepare(sql)
	if err != nil {
		Printf("error Prepare INSERT temp_update_ix_repo: %v\n", err)
		return err
	}

	for _, row := range db_aggregate.Table_index_repo {
		if row.Status == "update" {
			// INSERT
			_, err := stmt.Exec(row.Id, row.Id_pack_id)
			if err != nil {
				Printf("Error INSERT temp_update_ix_repo, error is %v\n", err)
				return err
			}
		}
	}
	stmt.Close()

	// one BIG UPDATE UPDATE index_repo SET ... FROM temp_update_ix_repo
	//         WHERE index_repo.id = temp_update_ix_repo.id
	sql = `
    UPDATE index_repo SET id_pack_id = temp_update_ix_repo.id_pack_id
    FROM temp_update_ix_repo WHERE index_repo.id = temp_update_ix_repo.id`
	r, err := tx.Exec(sql)
	if err != nil {
		Printf("error UPDATE index_repo: %v\n", err)
		return err
	}
	count, _ := r.RowsAffected()
	Printf("UPDATE repo_index %10d rows updated.\n", count)
	if count > 0 {
		*changes_made = true
	}
	return nil
}

// remove rows in tables, wic reference oter tables
// copied and modified from
func scrub_superfluous_rows(tx *sqlx.Tx) error {
	type ForeignKeys struct {
		check_table  string
		column_name  string
		ref_table    string // the implied column is always "Id" for the ref_table
	}
	var check_tables = []ForeignKeys{
		{"meta_dir",     "Id_snap_id",  "snapshots"},
		{"meta_dir",     "Id_idd",      "index_repo"},
		{"idd_file",     "Id_blob",     "index_repo"},
		{"contents",     "Id_data_idd", "index_repo"},
		{"contents",     "Id_blob",     "index_repo"},
		{"idd_file",     "Id_name",     "names"},
		{"index_repo",   "Id_pack_id",  "packfiles"},
		{"dir_name_id",  "Id",          "index_repo"},
		{"dir_name_id",  "Id_name",     "names"},
		{"dir_path_id",  "Id",          "index_repo"},
		{"dir_path_id",  "Id_pathname", "fullname"},
		{"dir_children", "Id_parent",   "index_repo"},
		{"dir_children", "Id_child",    "index_repo"},
	}

	//Printf("\n*** scrub superfluous rows ***\n")
	for _, action := range check_tables {
		sql := fmt.Sprintf(`DELETE FROM %s WHERE %s IN (SELECT DISTINCT %s.%s FROM %s
  LEFT OUTER JOIN %s ON %s.%s = %s.id WHERE %s.id IS NULL)`,
      action.check_table, action.column_name,
			action.check_table, action.column_name,
			action.check_table, action.ref_table,
			action.check_table, action.column_name,
			action.ref_table, action.ref_table)

		if dbOptions.echo {
			Printf("%s\n", sql)
		}
		r, err := tx.Exec(sql)
		if err != nil {
			Printf("Error in sql %s, error is %v\n", sql, err)
			return err
		}
		count, _ := r.RowsAffected()
		if count > 0 {
			Printf("DELETE %-15s %6d rows deleted.\n", action.check_table, count)
		}
	}
	return nil
}

func CreateMemNames(db_aggregate *DBAggregate,
	repositoryData *RepositoryData, newComers *Newcomers) map[string]*NamesRecordMem {

	Mem_names_map := make(map[string]*NamesRecordMem, len(repositoryData.directory_map))
	for _, file_list := range repositoryData.directory_map {
		for _, meta := range file_list {
			switch meta.Type {
			case "file", "dir":
				data, ok := db_aggregate.Table_names[meta.name]
				if !ok {
					row := NamesRecordMem{Name: meta.name, Status: "memory"}
					Mem_names_map[meta.name] = &row
				} else {
					data.Status = "db"
					Mem_names_map[meta.name] = data
				}
			}
		}
	}
	return Mem_names_map
}

func CreateMemPackfiles(db_aggregate *DBAggregate, repositoryData *RepositoryData,
newComers *Newcomers) map[restic.IntID]*PackfilesRecordMem {

	// collect all packfiles from the index_handle
	pack_intIDs := mapset.NewSet[restic.IntID]()
	for _, handle := range repositoryData.index_handle {
		pack_intIDs.Add(handle.pack_index)
	}

	// convert the set to a map of Mem_packfiles_map
	Mem_packfiles_map := make(map[restic.IntID]*PackfilesRecordMem, pack_intIDs.Cardinality())
	for pack_intID := range pack_intIDs.Iter() {
		data, ok := db_aggregate.Table_packfiles[pack_intID]
		if !ok {
			packID := repositoryData.index_to_blob[pack_intID]
			row := PackfilesRecordMem{Packfile_id: packID.String(), Status: "memory"}
			Mem_packfiles_map[pack_intID] = &row
		} else {
			data.Status = "db"
			Mem_packfiles_map[pack_intID] = data
		}
	}
	pack_intIDs = nil
	return Mem_packfiles_map
}
