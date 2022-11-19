package main

// compile with "go run build.go -tags debug""
// run with DEBUG_LOG=/home/wplapper/restic/debug.log fully-qualified-name/restic -r <repo> overview

// ref to onedrive: rclone:onedrive:restic_backupss

import (
	"golang.org/x/sync/errgroup"
	"time"

	//argparse
	"github.com/spf13/cobra"

	// sqlx for sqlite3
	"github.com/jmoiron/sqlx"
	//"database/sql"

	// restic library
	"github.com/wplapper/restic/library/restic"
	"github.com/wplapper/restic/library/sqlite"
)

var cmdDBRem = &cobra.Command{
	Use:   "db_rem_record [flags]",
	Short: "remove records from SQLite database",
	Long: `add records to SQLite database.

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
	flags.StringVarP(&dbOptions.altDB, "DB", "", "", "aternative database name")
}

func runDBRem(gopts GlobalOptions, args []string) error {
	// step 0: setup global stuff
	repositoryData := init_repositoryData() // is a *RepositoryData
	newComers := InitNewcomers()
	db_aggregate.repositoryData = repositoryData
	EMPTY_NODE_ID = restic.Hash([]byte("{\"nodes\":[]}\n"))
	// need access to verbose option
	gOptions = gopts
	var db_name string
	var ok bool

	// step 1: open repository
	repo, err := OpenRepository(gopts)
	if err != nil {
		return err
	}
	Printf("Repository is %s\n", gopts.Repo)

	repositoryData.snaps, err = GatherAllSnapshots(gopts, repo)
	if err != nil {
		return err
	}

	// step 2: manage Index Records
	start := time.Now()
	_ = start
	if err = HandleIndexRecords(gopts, repo, repositoryData); err != nil {
		return err
	}

	// step 3: collect all snapshot related information
	start = time.Now()
	GatherAllRepoData(gopts, repo, repositoryData)

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
	names_and_counts := make(map[string]int)

	// the first three tables can be read in paralel
	wg, _ := errgroup.WithContext(gopts.ctx)
	wg.Go(func() error { return sqlite.Get_all_high_ids() })
	wg.Go(func() error { return readAllTablesAndCounts(db_conn, names_and_counts) })
	wg.Go(func() error { return ReadSnapshotTable(db_conn, &db_aggregate) })
	wg.Go(func() error { return ReadNamesTable(db_conn, &db_aggregate) })
	res := wg.Wait()
	if res != nil {
		Printf("error processing group 1\n")
		return res
	}
	db_aggregate.table_counts = names_and_counts

	// te rest is sequential
	err = ReadIndexRepoTable(db_conn, &db_aggregate)
	if err != nil {
		return err
	}
	err = ReadIddFileTable(db_conn, &db_aggregate)
	if err != nil {
		return err
	}
	err = ReadPackfilesTable(db_conn, &db_aggregate)
	if err != nil {
		Printf("Error processing ReadPackfilesTable. Error is %v\n", err)
		return err
	}

	// compare snapshots in memory with snapshots in the database
	newComers.mem_snapshots = CreateMemSnapshots(&db_aggregate, repositoryData, newComers)
	newComers.old_snapshots = OldDBKeys(db_aggregate.Table_snapshots, newComers.mem_snapshots)
	Printf("%7d old records in old_snapshots\n", newComers.old_snapshots.Cardinality())

	// create TEMP TABLE remove_snaps
	_, err = db_aggregate.db_conn.Exec("CREATE TEMP TABLE remove_snaps (id INTEGER)")
	if err != nil {
		Printf("error creating TEMP TABLE remove_snaps: %v\n", err)
		return err
	}

	// fill TABLE remove_snaps
	if newComers.old_snapshots.Cardinality() > 0 {
		t_insert := make([]RemoveTable, 0, newComers.old_snapshots.Cardinality())
		for snap_id := range newComers.old_snapshots.Iter() {
			row := (db_aggregate.Table_snapshots)[snap_id]
			row.Status = "delete"
			(db_aggregate.Table_snapshots)[snap_id] = row
			t_insert = append(t_insert, RemoveTable{Id: row.Id})
		}

		sql := "INSERT INTO remove_snaps(id) VALUES(:id)"
		_, err := db_aggregate.db_conn.NamedExec(sql, t_insert)
		if err != nil {
			Printf("error INSERTing into TEMP TABLE remove_snaps: %v\n", err)
			return err
		}
		Printf("INSERTED %d rows into TEMP TABLE remove_snaps\n", len(t_insert))
	}

	// compare packfiles with its DB counterpart
	newComers.mem_packfiles = CreateMemPackfiles(&db_aggregate, repositoryData, newComers)
	newComers.old_packfiles = OldDBKeys(db_aggregate.Table_packfiles, newComers.mem_packfiles)
	Printf("%7d old records in old_packfiles\n", newComers.old_packfiles.Cardinality())
	for packID := range newComers.old_packfiles.Iter() {
		row := (db_aggregate.Table_packfiles)[packID]
		row.Status = "delete"
		(db_aggregate.Table_packfiles)[packID] = row
	}
	ComparePackfiles(&db_aggregate, repositoryData, newComers)

	// compare index_repo with its DB counterpart
	newComers.mem_index_repo = CreateMemIndexRepo(&db_aggregate, repositoryData, newComers)
	newComers.old_index_repo = OldDBKeys(db_aggregate.Table_index_repo, newComers.mem_index_repo)
	Printf("%7d old records in old_index_repo\n", newComers.old_index_repo.Cardinality())
	for name := range newComers.old_index_repo.Iter() {
		row := (db_aggregate.Table_index_repo)[name]
		row.Status = "delete"
		(db_aggregate.Table_index_repo)[name] = row
	}

	// compare names with its DB counterpart
	newComers.mem_names = CreateMemNames(&db_aggregate, repositoryData, newComers)
	newComers.old_names = OldDBKeys(db_aggregate.Table_names, newComers.mem_names)
	Printf("%7d old records in old_names\n", newComers.old_names.Cardinality())
	for name := range newComers.old_names.Iter() {
		row := db_aggregate.Table_names[name]
		row.Status = "delete"
		db_aggregate.Table_names[name] = row
	}

	// compare idd_file with its DB counterpart
	newComers.mem_idd_file = CreateMemIddFile(&db_aggregate, repositoryData, newComers)
	newComers.old_idd_file = OldDBKeys(db_aggregate.Table_idd_file, newComers.mem_idd_file)
	Printf("%7d old records in old_idd_file\n", newComers.old_idd_file.Cardinality())
	for comp_ix := range newComers.old_idd_file.Iter() {
		row := (db_aggregate.Table_idd_file)[comp_ix]
		row.Status = "delete"
		(db_aggregate.Table_idd_file)[comp_ix] = row
	}

	// process index_repo changes
	err = db_rem_index_repo(&db_aggregate, repositoryData, newComers)
	if err != nil {
		return err
	}

	// modify database tables
	modify_database_tables(&db_aggregate, repositoryData, newComers)
	return nil
}

func db_rem_index_repo(db_aggregate *DBAggregate, repositoryData *RepositoryData,
	newComers *Newcomers) error {

	var temp_tables = []string{
		// temp TABLE meta_blobs
		`CREATE TEMP TABLE meta_blobs AS
    SELECT index_repo.id FROM remove_snaps
      JOIN meta_dir   ON meta_dir.id_snap_id = remove_snaps.id
      JOIN index_repo ON meta_dir.id_idd     = index_repo.id
    EXCEPT
    SELECT index_repo.id FROM snapshots
      JOIN meta_dir   ON meta_dir.id_snap_id = snapshots.id
      JOIN index_repo ON meta_dir.id_idd     = index_repo.id
      WHERE snapshots.id NOT IN (SELECT id FROM remove_snaps)`,
		// temp TABLE data_blobs
		`CREATE TEMP TABLE data_blobs AS
    SELECT contents.id_data_idd FROM contents
      JOIN idd_file
      ON  contents.id_blob  = idd_file.id_blob
      AND contents.position = idd_file.position
      JOIN meta_blobs
      ON meta_blobs.id = idd_file.id_blob
    EXCEPT
    SELECT contents.id_data_idd FROM contents
      JOIN idd_file
      ON  contents.id_blob  = idd_file.id_blob
      AND contents.position = idd_file.position
      JOIN index_repo ON index_repo.id
        NOT IN (SELECT meta_blobs.id FROM meta_blobs)
      WHERE index_repo.index_type = 'tree' AND idd_file.type = 'f'
        AND idd_file.id_blob = index_repo.id`,
		// TEMP TABLE touched_blobs, a UNION TABLE of 'meta_blobs'|'data_blobs'
		// wih additional size and pack_id information
		`CREATE TEMP TABLE touched_blobs AS
     SELECT index_repo.id, index_repo.id_pack_id,index_repo.idd_size
      FROM index_repo
      JOIN meta_blobs
      ON index_repo.id = meta_blobs.id
     UNION
     SELECT index_repo.id, index_repo.id_pack_id,index_repo.idd_size
      FROM index_repo
      JOIN data_blobs
      ON index_repo.id = data_blobs.id_data_idd`}

	for _, sql := range temp_tables {
		if dbOptions.echo {
			Printf("%s\n", sql)
		}
		_, err := db_aggregate.db_conn.Exec(sql)
		if err != nil {
			Printf("error creating TEMP TABLE %s: %v\n", sql, err)
			return err
		}
	}

	if gOptions.verbosity > 1 {
		var count int
		for _, table_name := range []string{"meta_blobs", "data_blobs", "touched_blobs"} {
			sql := "SELECT count(*) FROM " + table_name
			err := db_aggregate.db_conn.Get(&count, sql)
			if err != nil {
				Printf("Query error for Get %s: %v\n", sql, err)
				return err
			}
			Printf("TEMP TABLE %-15s has %5d entries.\n", table_name, count)
		}
	}

	count_updates := 0
	// update index_repo.id_pack_id, packfiles.id is master here!s
	for id, ih := range repositoryData.index_handle {
		pack_index := ih.pack_index
		db_index_repo, ok := (db_aggregate.Table_index_repo)[id]
		if !ok {
			panic("update_index_repo: index_repo row not found in database")
		}

		// translate memory 'pack_index' into database packfiles index
		pack_key := &(repositoryData.index_to_blob[pack_index])
		pack_row, ok := (db_aggregate.Table_packfiles)[pack_key]
		if !ok {
			panic("update_index_repo: packfiles row not found in database")
		}

		if db_index_repo.Id_pack_id != pack_row.Id {
			db_index_repo.Status = "update"
			db_index_repo.Id_pack_id = pack_row.Id
			(db_aggregate.Table_index_repo)[id] = db_index_repo
			count_updates++
		}
	}
	Printf("number of updates to index_repo %5d\n", count_updates)
	return nil
}

func modify_database_tables(db_aggregate *DBAggregate, repositoryData *RepositoryData,
	newComers *Newcomers) error {
	// et column names
	column_names, err := GetColumnNames(db_aggregate.db_conn)
	if err != nil {
		return err
	}

	// start transaction
	var changes_made = false
	tx, err := (db_aggregate).db_conn.Beginx()
	_ = tx
	if err != nil {
		Printf("Cant start transaction. Error is %v\n", err)
		return err
	}
	Printf("BEGIN TRANSACTION\n")

	var sqls = []RemoveSqLTable{
		RemoveSqLTable{table_name: "snapshots",
			sql: `DELETE FROM snapshots WHERE snapshots.id IN (SELECT id FROM remove_snaps)`},
		RemoveSqLTable{table_name: "meta_dir",
			sql: `DELETE FROM meta_dir WHERE meta_dir.id_snap_id IN (SELECT id FROM remove_snaps)`},
		RemoveSqLTable{table_name: "contents",
			sql: `DELETE FROM contents WHERE contents.id_blob IN (SELECT id FROM meta_blobs)`},
		RemoveSqLTable{table_name: "idd_file",
			sql: `DELETE FROM idd_file WHERE idd_file.id_blob IN (SELECT id FROM meta_blobs)`},
		RemoveSqLTable{table_name: "index_repo",
			sql: `DELETE FROM index_repo WHERE index_repo.id IN (SELECT id FROM meta_blobs)`}}

	for _, sql := range sqls {
		if dbOptions.echo {
			Printf("%s\n", sql.sql)
		}

		r, err := tx.Exec(sql.sql)
		if err != nil {
			Printf("error %s error is: %v\n", sql.sql, err)
			return err
		}
		count, _ := r.RowsAffected()
		Printf("%-15s %5d rows deleted.\n", sql.table_name, count)
		if count > 0 {
			changes_made = true
		}
	}

	// DELETE FROM names
	t_delete := make([]RemoveTable, 0, newComers.old_names.Cardinality())
	for name := range newComers.old_names.Iter() {
		row := db_aggregate.Table_names[name]
		if row.Status == "delete" {
			t_delete = append(t_delete, RemoveTable{Id: row.Id})
		}
	}
	delete_selected_rows(&t_delete, tx, "names", &changes_made)

	// DELETE FROM packfiles
	t_delete = make([]RemoveTable, 0, newComers.old_packfiles.Cardinality())
	for key := range newComers.old_packfiles.Iter() {
		row := (db_aggregate.Table_packfiles)[key]
		if row.Status == "delete" {
			t_delete = append(t_delete, RemoveTable{Id: row.Id})
		}
	}
	delete_selected_rows(&t_delete, tx, "packfiles", &changes_made)

	// add to packfiles
	err = InsertATable("packfiles", newComers.mem_packfiles, tx, column_names, &changes_made)
	if err != nil {
		return err
	}

	// DELETE FROM index_repo
	t_delete = make([]RemoveTable, 0, newComers.old_index_repo.Cardinality())
	for key := range newComers.old_index_repo.Iter() {
		row := (db_aggregate.Table_index_repo)[key]
		if row.Status == "delete" {
			t_delete = append(t_delete, RemoveTable{Id: row.Id})
		}
	}
	delete_selected_rows(&t_delete, tx, "index_repo", &changes_made)

	// UPDATE index_repo
	update_table := make([]UpdateTable_index_repo, 0)
	for _, row := range db_aggregate.Table_index_repo {
		if row.Status == "update" {
			update_table = append(update_table, UpdateTable_index_repo{
				Id_pack_id: row.Id_pack_id, Id: row.Id})
		}
	}
	if len(update_table) > 0 {
		sql := "UPDATE index_repo SET id_pack_id = :id_pack_id WHERE id = :id"
		if dbOptions.echo {
			Printf("%s\n", sql)
		}
		stmt, err := tx.Prepare(sql)
		if err != nil {
			Printf("error Prepare UPDATE index_repo: %v\n", err)
			return err
		}
		// do one UPDATE at a time, loop over all changes
		for _, slice := range update_table {
			r, err := stmt.Exec(slice.Id_pack_id, slice.Id)
			if err != nil {
				Printf("error UPDATE index_repo: %v\n", err)
				return err
			}
			count, _ := r.RowsAffected()
			if count == 1 {
				continue
			} else {
				Printf("??? count=%d\n", count)
			}
		}
		Printf("%5d rows of repo_index updated\n", len(update_table))
		changes_made = true
	}

	// update timestamp
	err = db_update_timestamp(db_aggregate.Table_snapshots, tx)
	if err != nil {
		Printf("update timestamp failed: error is %v\n", err)
	}

	// COMMIT or ROLLBACK transaction
	if !dbOptions.rollback && changes_made {
		err = tx.Commit()
		if err != nil {
			Printf("COMMIT error %v\n")
			return err
		}
		Printf("COMMIT\n")
	} else {
		tx.Rollback()
		Printf("ROLLBACK\n")
	}
	return nil
}

// this is an ugly fix, because sql / sqlx cannot do bulk DELETEs
// delete_selected_rows generates a bulk DELETE via subquery
// DELETE FROM <table_name> WHERE id IN (SELECT id FROM delete_table)
// but it can DELETE <table_name> WHERE id IN (SELECT id FROM <t_del>)
func delete_selected_rows(t_del *[]RemoveTable, db *sqlx.Tx, table_name string,
	changes_made *bool) error {

	if len(*t_del) == 0 {
		return nil
	}

	// CREATE TEMP TABLE
	_, err := db.Exec("CREATE TEMP TABLE IF NOT EXISTS delete_table (id INTEGER)")
	if err != nil {
		Printf("error creating TEMP TABLE delete_table: %v\n", err)
		return err
	}

	// fill - bulk INSERT works
	sql := "INSERT INTO delete_table(id) VALUES(:id)"
	_, err = db.NamedExec(sql, *t_del)
	if err != nil {
		Printf("error INSERTing into TEMP TABLE delete_table: %v\n", err)
		return err
	}

	// really DELETE via subquery in one go
	sql = "DELETE FROM " + table_name + " WHERE id IN (SELECT id FROM delete_table)"
	r, err := db.Exec(sql)
	if err != nil {
		Printf("error in %s: error is: %v\n", sql, err)
		return err
	}
	count, _ := r.RowsAffected()
	Printf("DELETE %-15s %5d rows deleted\n", table_name, count)
	if count > 0 {
		*changes_made = true
	}

	// empty TEMP TABLE
	sql = "DELETE FROM delete_table"
	r, err = db.Exec(sql)
	if err != nil {
		Printf("error in %s: error is: %v\n", sql, err)
		return err
	}
	return nil
}
