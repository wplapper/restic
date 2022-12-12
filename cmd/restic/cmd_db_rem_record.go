package main

// compile with "go run build.go -tags debug""
// run with DEBUG_LOG=/home/wplapper/restic/debug.log fully-qualified-name/restic -r <repo> overview

// ref to onedrive: rclone:onedrive:restic_backups

import (
	"golang.org/x/sync/errgroup"
	"time"

	//argparse
	"github.com/spf13/cobra"

	// sqlx for sqlite3
	"github.com/jmoiron/sqlx"
	//"database/sql"

	// sets
	//"github.com/wplapper/restic/library/mapset"

	// restic library
	"github.com/wplapper/restic/library/restic"
	"github.com/wplapper/restic/library/sqlite"
)

type RemoveSqLTable struct {
	table_name string
	sql        string
}

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
	repositoryData.snap_map = make(map[string]*restic.Snapshot)
	for _, sn := range repositoryData.snaps {
		repositoryData.snap_map[sn.ID().Str()] = sn
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

	tx, err := (db_aggregate).db_conn.Beginx()
	if err != nil {
		Printf("Cant start transaction. Error is %v\n", err)
		return err
	}
	db_aggregate.tx = tx
	Printf("BEGIN TRANSACTION\n")

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

	// TABLE snapshots
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
	modify_database_tables(&db_aggregate, repositoryData, newComers)
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
	// update index_repo.id_pack_id, packfiles.id is master here!s
	for id, ih := range repositoryData.index_handle {
		id_int := repositoryData.blob_to_index[id]
		pack_index := ih.pack_index
		db_index_repo, ok := (db_aggregate.Table_index_repo)[id_int]
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
			(db_aggregate.Table_index_repo)[id_int] = db_index_repo
			count_updates++
		}
	}
	//Printf("number of updates to index_repo %5d\n", count_updates)
	return nil
}

func modify_database_tables(db_aggregate *DBAggregate, repositoryData *RepositoryData,
	newComers *Newcomers) error {

	// start transaction
	var changes_made = false
	var err error
	tx := db_aggregate.tx

	var sqls = []RemoveSqLTable{
		RemoveSqLTable{"snapshots",
			`DELETE FROM snapshots WHERE snapshots.id IN (SELECT id FROM remove_snaps)`},
		RemoveSqLTable{"meta_dir",
			`DELETE FROM meta_dir WHERE meta_dir.id_snap_id IN (SELECT id FROM remove_snaps)`},
		RemoveSqLTable{"contents",
			`DELETE FROM contents WHERE contents.id_blob IN (SELECT id FROM meta_blobs)`},
		RemoveSqLTable{"idd_file",
			`DELETE FROM idd_file WHERE idd_file.id_blob IN (SELECT id FROM meta_blobs)`},
		//RemoveSqLTable{"index_repo",
		//	`DELETE FROM index_repo WHERE index_repo.id IN (SELECT id FROM meta_blobs)`},
	}

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
		Printf("DELETE %-15s %5d rows deleted.\n", sql.table_name, count)
		if count > 0 {
			changes_made = true
		}
	}

	// DELETE FROM names
	t_delete := make([]RemoveTable, 0)
	for _, row := range db_aggregate.Table_names {
		if row.Status == "delete" {
			t_delete = append(t_delete, RemoveTable{Id: row.Id})
		}
	}
	delete_selected_rows(&t_delete, tx, "names", &changes_made)

	// DELETE FROM packfiles
	t_delete = make([]RemoveTable, 0)
	for _, row := range db_aggregate.Table_packfiles {
		if row.Status == "delete" {
			t_delete = append(t_delete, RemoveTable{Id: row.Id})
		}
	}
	delete_selected_rows(&t_delete, tx, "packfiles", &changes_made)

	// DELETE FROM index_repo
	t_delete = make([]RemoveTable, 0)
	for _, row := range db_aggregate.Table_index_repo {
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
		Printf("UPDATE repo_index %10d rows updated.\n", len(update_table))
		changes_made = true
	}

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

// this is an ugly fix, because sql / sqlx cannot do bulk DELETEs.
// delete_selected_rows generates a bulk DELETE via subquery
// DELETE FROM <table_name> WHERE id IN (SELECT id FROM delete_table)
// but it can DELETE <table_name> WHERE id IN (SELECT id FROM <t_del>)
func delete_selected_rows(t_del *[]RemoveTable, tx *sqlx.Tx, table_name string,
	changes_made *bool) error {

	if len(*t_del) == 0 {
		return nil
	}

	// CREATE TEMP TABLE
	_, err := tx.Exec("CREATE TEMP TABLE IF NOT EXISTS delete_table (id INTEGER)")
	if err != nil {
		Printf("Error creating TEMP TABLE delete_table: %v\n", err)
		return err
	}

	// fill - bulk INSERT works
	sql := "INSERT INTO delete_table(id) VALUES(:id)"
	if _, err = tx.NamedExec(sql, *t_del); err != nil {
		Printf("error INSERTing into TEMP TABLE delete_table: %v\n", err)
		return err
	}

	// really DELETE via subquery in one go
	sql = "DELETE FROM " + table_name + " WHERE id IN (SELECT id FROM delete_table)"
	r, err := tx.Exec(sql)
	if err != nil {
		Printf("error in %s: error is: %v\n", sql, err)
		return err
	}
	count, _ := r.RowsAffected()
	Printf("DELETE %-15s %5d rows deleted.\n", table_name, count)
	if count > 0 {
		*changes_made = true
	}

	// empty TEMP TABLE
	sql = "DELETE FROM delete_table"
	r, err = tx.Exec(sql)
	if err != nil {
		Printf("error in %s: error is: %v\n", sql, err)
		return err
	}
	return nil
}

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

	// fill TABLE remove_snaps
	if len(t_insert) > 0 {
		sql := "INSERT INTO remove_snaps(id) VALUES(:id)"
		_, err := db_aggregate.tx.NamedExec(sql, t_insert)
		if err != nil {
			Printf("error INSERTing into TEMP TABLE remove_snaps: %v\n", err)
			return err
		}
		Printf("INSERT remove_snaps    %5d rows inserted.\n", len(t_insert))
	}
	newComers.old_snapshots = nil
	return nil
}
