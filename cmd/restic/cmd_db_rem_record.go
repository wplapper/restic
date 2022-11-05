package main

// compile with "go run build.go -tags debug""
// run with DEBUG_LOG=/home/wplapper/restic/debug.log fully-qualified-name/restic -r <repo> overview

// ref to onedrive:

import (
	// system
	//"fmt"
	//"strings"
	"time"
	//"reflect"

	//argparse
	"github.com/spf13/cobra"

	// sets
	//"github.com/wplapper/restic/library/mapset"

	// sqlx for sqlite3
	"github.com/jmoiron/sqlx"

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
}

func runDBRem(gopts GlobalOptions, args []string) error {
	// step 0: setup global stuff
	repositoryData := init_repositoryData() // is a *RepositoryData
	newComers := InitNewcomers()
	db_aggregate.repositoryData = repositoryData
	EMPTY_NODE_ID = restic.Hash([]byte("{\"nodes\":[]}\n"))
	// need access to verbose option
	gOptions = gopts

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
	db_name, ok := DATABASE_NAMES[gopts.Repo]
	if !ok {
		Printf("database name for repo %s is missing!\n", gopts.Repo)
		return nil
	}

	// step 4.2: open selected database
	db_conn, err := sqlite.OpenDatabase(db_name, true, 1, true)
	if err != nil {
		Printf("db_add_record: OpenDatabase failed, error is %v\n", err)
		return err
	}
	db_aggregate.db_conn = db_conn
	names_and_counts := make(map[string]int)
	err = readAllTablesAndCounts(db_conn, names_and_counts)
	if err != nil {
		Printf("readAllTablesAndCounts error is %v\n", err)
		return err
		return err
	}
	db_aggregate.table_counts = names_and_counts

	// action loop for reading all database tables
	// this is semi generic
	type action_function func(*sqlx.DB, *DBAggregate) error
	type ActionStruct struct {
		table_name string
		routine    action_function
	}
	// all these Read<tbl_name>Table functions store their work in a struct
	// element of 'db_aggregate'. All TABLE results are maps.
	var actions = []ActionStruct{
		{table_name: "snapshots", 	routine: ReadSnapshotTable},
		{table_name: "index_repo", 	routine: ReadIndexRepoTable},
		{table_name: "meta_dir", 		routine: ReadMetaDirTable},
		{table_name: "names", 			routine: ReadNamesTable},
		{table_name: "idd_file", 		routine: ReadIddFileTable},
		{table_name: "packfiles", 	routine: ReadPackfilesTable},
		{table_name: "contents", 		routine: ReadContentsTable}}

	for _, action := range actions {
		Printf("reading table %s\n", action.table_name)
		err := action.routine(db_conn, &db_aggregate)
		if err != nil {
			Printf("error reading table %s %v\n", action.table_name, err)
			return err
		}
	}

	// compare snapshots in memory with snapshots in the database
	newComers.mem_snapshots = CreateMemSnapshots(&db_aggregate, repositoryData, newComers)
	newComers.old_snapshots = OldDBKeys(*db_aggregate.table_snapshots, newComers.mem_snapshots)
	Printf("%7d old records in old_snapshots\n", newComers.old_snapshots.Cardinality())

	// create TEMP TABLE remove_snaps
	_, err = db_aggregate.db_conn.Exec("CREATE TEMP TABLE remove_snaps (id INTEGER)")
	if err != nil {
		Printf("error creating TEMP TABLE remove_snaps: %v\n", err)
		return err
	}

	// fill TABLE remove_snaps
	if newComers.old_snapshots.Cardinality() > 0 {
		type RemoveSnaps struct {
			Id int
		}
		t_insert := make([]RemoveSnaps, 0, newComers.old_snapshots.Cardinality())
		for snap_id := range newComers.old_snapshots.Iter() {
			row := (*db_aggregate.table_snapshots)[snap_id]
			//Printf("DELETE %#v\n", row)
			t_insert = append(t_insert, RemoveSnaps{Id: row.Id})
		}

		sql := "INSERT INTO remove_snaps(id) VALUES(:id)"
		_, err := db_aggregate.db_conn.NamedExec(sql, t_insert)
		if err != nil {
			Printf("error INSERTing into TEMP TABLE remove_snaps: %v\n", err)
			return err
		}
		Printf("INSERTED %d rows into TEMP TABLE remove_snaps\n", len(t_insert))
	}

	// compare index_repo with its DB counterpart
	newComers.mem_index_repo = CreateMemIndexRepo(&db_aggregate, repositoryData, newComers)
	newComers.old_index_repo = OldDBKeys(*db_aggregate.table_index_repo, newComers.mem_index_repo)
	Printf("%7d old records in old_index_repo\n", newComers.old_index_repo.Cardinality())

	// compare names with its DB counterpart
	newComers.mem_names = CreateMemNames(&db_aggregate, repositoryData, newComers)
	newComers.new_names = OldDBKeys(*db_aggregate.table_names, newComers.mem_names)
	Printf("%7d old records in old_names\n", newComers.new_names.Cardinality())


	// compare packfiles with its DB counterpart
	newComers.mem_packfiles = CreateMemPackfiles(&db_aggregate, repositoryData, newComers)
	newComers.new_packfiles = OldDBKeys(*db_aggregate.table_packfiles, newComers.mem_packfiles)
	Printf("%7d old records in old_packfiles\n", newComers.new_packfiles.Cardinality())


	// compare idd_file with its DB counterpart
	newComers.mem_idd_file = CreateMemIddFile(&db_aggregate, repositoryData, newComers)
	newComers.new_idd_file = OldDBKeys(*db_aggregate.table_idd_file, newComers.mem_idd_file)
	Printf("%7d old records in old_idd_file\n", newComers.new_idd_file.Cardinality())

	/*
	// compare meta_dir with its DB counterpart
	newComers.mem_meta_dir = CreateMemMetaDir(&db_aggregate, repositoryData, newComers)
	newComers.new_meta_dir = OldDBKeys(*db_aggregate.table_meta_dir, newComers.mem_meta_dir)
	Printf("%7d new records in new_meta_dir\n", newComers.new_meta_dir.Cardinality())
	*/
	/*
	// compare contents with its DB counterpart
	newComers.mem_contents = CreateMemContents(&db_aggregate, repositoryData, newComers)
	newComers.new_contents = OldDBKeys(*db_aggregate.table_contents, newComers.mem_contents)
	Printf("%5d new records in new_contents\n", newComers.new_contents.Cardinality())
	*/
	err = db_rem_index_repo(&db_aggregate, repositoryData, newComers)
	if err != nil {
		return err
	}
	return nil
}

func db_rem_index_repo(db_aggregate *DBAggregate, repositoryData *RepositoryData,
newComers *Newcomers)  error {

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
      ON index_repo.id = data_blobs.id_data_idd`,}

	for _, sql := range temp_tables {
		if dbOptions.echo {
			Printf("%s\n", sql)
		}
		_, err := db_aggregate.db_conn.Exec(sql)
		if err != nil {
			Printf("error creating TEMP TABLE %s %v\n", sql, err)
			return err
		}
	}

	if gOptions.verbosity > 1 {
		var count int
		for _,table_name := range []string{"meta_blobs", "data_blobs", "touched_blobs"} {
			sql := "SELECT count(*) FROM " + table_name
			err := db_aggregate.db_conn.Get(&count, sql)
			if err != nil {
				Printf("Query error for Get %s is %v\n", sql, err)
				return err
			}
			Printf("TEMP TABLE %-14s has %4d entries.\n", table_name, count)
		}
	}

	// find rows which
	// 1. have been deleted or
	// 2. live in a different packfile
	for key := range *db_aggregate.table_index_repo {
		_, ok := newComers.mem_index_repo[key]
		if !ok {
			// mark delete
			temp := (*db_aggregate.table_index_repo)[key]
			temp.Status = "delete"
			(*db_aggregate.table_index_repo)[key] = temp
			Printf("DELETE %s\n", key.String()[:12])
		} else {
			// check if mapping is different
			// packfile from memory
			ID_from_memory := newComers.mem_index_repo[key].packfile
			// packfile from database
			ID_from_DB := (*db_aggregate.table_index_repo)[key].packfile
			if ID_from_DB == nil || ID_from_memory == nil {
				Printf("ID_from_DB %p ID_from_memory %p\n", ID_from_DB, ID_from_memory)
				panic("Nul pointer references")
			}

			if *ID_from_DB != *ID_from_memory {
				// index_repo.Id_pack_id needs updating
				temp2 := (*db_aggregate.table_index_repo)[key]
				temp2.Status = "upd"
				(*db_aggregate.table_index_repo)[key] = temp2
				// get new packID pointer from memory
				new_pack_index := repositoryData.index_handle[key].pack_index
				new_pack_ID := repositoryData.index_to_blob[new_pack_index]
				// packfile record
				ptr_new_pack_ID := &repositoryData.index_to_blob[repositoryData.blob_to_index[new_pack_ID]]
				new_packfile_row, ok := (*db_aggregate.table_packfiles)[ptr_new_pack_ID]
				_ = new_packfile_row
				if !ok {
					Printf("Can't proceed\n")
					Printf("blob %s ID-mem %s ID-DB %s\n", key.String()[:12], ID_from_memory.String()[:12],
						ID_from_DB.String()[:12])
					panic("db_rem_index_repo: packfiles missing")
				}
				temp3 := (*db_aggregate.table_index_repo)[key]
				temp3.Id_pack_id = new_packfile_row.Id
				(*db_aggregate.table_index_repo)[key] = temp3
				Printf("UPDATE %s\n to %5d\n", key.String()[:12], new_packfile_row.Id)
			}
		}
	}
	return nil
}

