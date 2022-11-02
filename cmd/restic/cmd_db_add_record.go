package main
// compile with "go run build.go -tags debug""
// run with DEBUG_LOG=/home/wplapper/restic/debug.log fully-qualified-name/restic -r <repo> overview

// ref to onedrive: rclone:onedrive:restic_backups

import (
	// system
	"time"
	//"sort"
	//"math"
	"fmt"
	"strings"

	//argparse
	"github.com/spf13/cobra"

  // sets
  "github.com/deckarep/golang-set"

	// sqlx for sqlite3
  "github.com/jmoiron/sqlx"

	// restic library
	"github.com/wplapper/restic/library/restic"
	"github.com/wplapper/restic/library/sqlite"
)

type Newcomers struct {
	// setup variables and maps to catch the newcomers
	mem_snapshots  	map[string]SnapshotRecordMem
	mem_index_repo 	map[*restic.ID]IndexRepoRecordMem
	mem_names      	map[string]NamesRecordMem
	mem_idd_file	 	map[CompIddFile]IddFileRecordMem
	mem_meta_dir		map[CompMetaDir]MetaDirRecordMem
	mem_contents		map[CompContents]ContentsRecordMem
	mem_packfiles		map[*restic.ID]PackfilesRecordMem

	new_snapshots		mapset.Set
	new_index_repo	mapset.Set
	new_names				mapset.Set
	new_idd_file		mapset.Set
	new_meta_dir		mapset.Set
	new_contents		mapset.Set
	new_packfiles		mapset.Set
}

var cmdDBAdd = &cobra.Command{
	Use:   "db_add_record [flags]",
	Short: "add records to SQLite database",
	Long: `add records to SQLite database.

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
	DisableAutoGenTag: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runDBAdd(globalOptions, args)
	},
}

func InitNewcomers() *Newcomers {
	var new_comers Newcomers
	new_comers.mem_snapshots = 	make(map[string]SnapshotRecordMem)
	new_comers.mem_index_repo = make(map[*restic.ID]IndexRepoRecordMem)
	new_comers.mem_names			= make(map[string]NamesRecordMem)
	new_comers.mem_idd_file		= make(map[CompIddFile]IddFileRecordMem)
	new_comers.mem_meta_dir		= make(map[CompMetaDir]MetaDirRecordMem)
	new_comers.mem_contents		= make(map[CompContents]ContentsRecordMem)
	new_comers.mem_packfiles	= make(map[*restic.ID]PackfilesRecordMem)

	new_comers.new_snapshots 	= mapset.NewSet()
	new_comers.new_index_repo = mapset.NewSet()
	new_comers.new_names 			= mapset.NewSet()
	new_comers.new_idd_file 	= mapset.NewSet()
	new_comers.new_meta_dir 	= mapset.NewSet()
	new_comers.new_contents 	= mapset.NewSet()
	new_comers.new_packfiles 	= mapset.NewSet()
	return &new_comers
}

func init() {
	cmdRoot.AddCommand(cmdDBAdd)
	flags := cmdDBAdd.Flags()
	flags.BoolVarP(&dbOptions.echo, "echo", "E", false, "echo database operations to stdout")
}

func runDBAdd(gopts GlobalOptions, args []string) error {
	// step 0: setup global stuff
	start := time.Now()
	_ = start
	repositoryData := init_repositoryData() // is a *RepositoryData
	newComers := InitNewcomers()
	db_aggregate.repositoryData = repositoryData
	EMPTY_NODE_ID = restic.Hash([]byte("{\"nodes\":[]}\n"))
	gOptions = gopts

	// step 1: open repository
	repo, err := OpenRepository(gopts)
	if err != nil {
		return err
	}
	Printf("Repository is %s\n", gopts.Repo)
	//timeMessage("%-30s %10.1f seconds\n", "open repository", time.Now().Sub(start).Seconds())

	repositoryData.snaps, err = GatherAllSnapshots(gopts, repo)
	if err != nil {
			return err
	}

	// step 2: manage Index Records
	start = time.Now()
	if err = HandleIndexRecords(gopts, repo, repositoryData); err != nil {
		return err
	}
	//timeMessage("%-30s %10.1f seconds\n", "read index records", time.Now().Sub(start).Seconds())

	// step 3: collect all snapshot related information
	start = time.Now()
	GatherAllRepoData(gopts, repo, repositoryData)
	//timeMessage("%-30s %10.1f seconds\n", "GatherAllRepoData (sum)", time.Now().Sub(start).Seconds())

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
	err = sqlite.Get_all_high_ids()
	if err != nil {
		Printf("db_add_record: Could not get Get_all_high_ids, error is %v\n", err)
		return err
	}
	// step 5 with multiple substeps: read the various tables:
	// step 5.1: get tabe names and their counts
	// gather counts for all tables in database
	names_and_counts := make(map[string]int)
	err = readAllTablesAndCounts(db_conn, &names_and_counts)
	if err != nil {
		Printf("readAllTablesAndCounts error is %v\n", err)
		return err
	}
	db_aggregate.table_counts = &names_and_counts

	//GetColumnNames(db_aggregate.db_conn)

	// step 5.2: read the snapshots table:
	type action_function func(*sqlx.DB, *DBAggregate) error
	type ActionStruct struct {
		table_name string
		routine action_function
	}
	var actions = []ActionStruct {
		{table_name: "snapshots", 	routine:	ReadSnapshotTable},
		{table_name: "index_repo", 	routine:  ReadIndexRepoTable},
		{table_name: "meta_dir", 		routine: 	ReadMetaDirTable},
		{table_name: "names", 			routine:	ReadNamesTable},
		{table_name: "idd_file", 		routine:  ReadIddFileTable},
		{table_name: "packfiles", 	routine:  ReadPackfilesTable},
		{table_name: "contents",  	routine:  ReadContentsTable},}

	for _, action := range actions {
		Printf("reading table %s\n", action.table_name)
		err := action.routine(db_conn, &db_aggregate)
		if err != nil {
			Printf("error reading table %s %v\n", action.table_name, err)
			return err
		}
	}

	// compare snapshots in memory with snapshots in the database
	newComers.mem_snapshots = CreateMemSnapshots(&db_aggregate, repositoryData)
	newComers.new_snapshots = NewMemoryKeys(*db_aggregate.table_snapshots, newComers.mem_snapshots)
	high_snap := sqlite.Get_high_id("snapshots")
	for snap := range newComers.new_snapshots.Iter() {
		snap_id 					:= snap.(string)
		row 							:= newComers.mem_snapshots[snap_id]
		row.status 				= "new"
		row.Id 						= high_snap
		row.Snap_id 			= snap_id
		row.Id_snap_root 	= row.ID_mem.String()
		newComers.mem_snapshots[snap_id] = row
		//Printf("new snapshot %#v\n", row)
		high_snap++
	}

	//CalcuateNewEntries(*db_aggregate.table_snapshots, mem_snapshots,
	//	repositoryData, &db_aggregate, CreateMemSnapshots, &new_snapshots)
	Printf("%5d new records in new_snapshots\n", newComers.new_snapshots.Cardinality())

	// compare names with its DB counterpart
	newComers.mem_names = CreateMemNames(&db_aggregate, repositoryData)
	newComers.new_names = NewMemoryKeys(*db_aggregate.table_names, newComers.mem_names)
	Printf("%5d new records in new_names\n", newComers.new_names.Cardinality())

	high_names := sqlite.Get_high_id("names")
	for raw := range newComers.new_names.Iter() {
		name 					:= raw.(string)
		row 					:= newComers.mem_names[name]
		row.status 		= "new"
		row.Id				= high_names
		row.Name    	=	name
		row.Name_type = "b" // there is no other name type (left)
		newComers.mem_names[name] = row
		high_names++
		Printf("new name %#v\n", row)
	}

	// compare packfiles with its DB counterpart
	newComers.mem_packfiles = CreateMemPackfiles(&db_aggregate, repositoryData)
	newComers.new_packfiles = NewMemoryKeys(*db_aggregate.table_packfiles, newComers.mem_packfiles)
	Printf("%5d new records in new_packfiles\n", newComers.new_packfiles.Cardinality())

	high_pack := sqlite.Get_high_id("packfiles")
	for raw := range newComers.new_packfiles.Iter() {
		pack_ID_ptr := raw.(*restic.ID)
		row := newComers.mem_packfiles[pack_ID_ptr]
		row.status 			= "new"
		row.Id 					= high_pack
		row.Packfile_id = (*pack_ID_ptr).String()

		// check if we need to update the back pointer
		//(*db_aggregate.pk_packfiles)[high_pack] = pack_ID_ptr
		newComers.mem_packfiles[pack_ID_ptr] = row
		//Printf("pack %s %#v\n", pack_ID_ptr.String()[:12], row)
		high_pack++
	}

	// compare index_repo with its DB counterpart
	newComers.mem_index_repo = CreateMemIndexRepo(&db_aggregate, repositoryData)
	newComers.new_index_repo = NewMemoryKeys(*db_aggregate.table_index_repo, newComers.mem_index_repo)
	Printf("%5d new records in new_index_repo\n", newComers.new_index_repo.Cardinality())

	high_repo := sqlite.Get_high_id("index_repo")
	for unchecked := range newComers.new_index_repo.Iter() {
		blob 			 := unchecked.(*restic.ID)
		ih 				 := repositoryData.index_handle[*blob]
		pack_Int 	 := ih.pack_index
		row 			 := newComers.mem_index_repo[blob]
		row.status = "new"
		row.Id     = high_repo
		row.Idd    = (*blob).String()
		ptr 			 := &(repositoryData.index_to_blob[pack_Int])
		row.Id_pack_id = newComers.mem_packfiles[ptr].Id
		row.packfile = ptr

		// write back newy constructed row
		newComers.mem_index_repo[blob] = row
		high_repo++
		//Printf("mem row %#v\n", row)
	}

	// compare idd_file with its DB counterpart
	newComers.mem_idd_file = CreateMemIddFile(&db_aggregate, repositoryData)
	newComers.new_idd_file = NewMemoryKeys(*db_aggregate.table_idd_file, newComers.mem_idd_file)
	Printf("%5d new records in new_idd_file\n", newComers.new_idd_file.Cardinality())

	high_idd := sqlite.Get_high_id("idd_file")
	for raw := range newComers.new_idd_file.Iter() {
		comp_ix := raw.(CompIddFile)
		meta_blob 		:= comp_ix.meta_blob
		position  		:= comp_ix.position
		row 					:= newComers.mem_idd_file[comp_ix]
		meta 					:= repositoryData.directory_map[repositoryData.blob_to_index[*meta_blob]][position]
		name          := meta.name
		row.status 		= "new"
		row.Id     		= high_idd
		ptr 					:= &(repositoryData.index_to_blob[repositoryData.blob_to_index[*meta_blob]])
		row.Id_blob		= newComers.mem_index_repo[ptr].Id
		row.Position  = position
		row.Id_name   = newComers.mem_names[name].Id
		newComers.mem_idd_file[comp_ix] = row
		high_idd++
		//Printf("new idd %#v\n", row)
	}

	// compare meta_dir with its DB counterpart
	newComers.mem_meta_dir = CreateMemMetaDir(&db_aggregate, repositoryData)
	newComers.new_meta_dir = NewMemoryKeys(*db_aggregate.table_meta_dir, newComers.mem_meta_dir)
	Printf("%5d new records in new_meta_dir\n", newComers.new_meta_dir.Cardinality())

	high_mdir := sqlite.Get_high_id("meta_dir")
	for raw := range newComers.new_meta_dir.Iter() {
		comp_ix 				:= raw.(CompMetaDir)
		snap_id 				:= comp_ix.snap_id
		p_meta_blob 		:= comp_ix.meta_blob
		row							:= newComers.mem_meta_dir[comp_ix]
		row.status			= "new"
		row.Id					= high_mdir
		row.Id_snap_id 	= newComers.mem_snapshots[snap_id].Id
		row.Id_idd			= newComers.mem_index_repo[p_meta_blob].Id
		newComers.mem_meta_dir[comp_ix] = row
		high_mdir++
		//Printf("new mdir %#v\n", row)
	}

	// compare contents with its DB counterpart
	newComers.mem_contents = CreateMemContents(&db_aggregate, repositoryData)
	newComers.new_contents = NewMemoryKeys(*db_aggregate.table_contents, newComers.mem_contents)
	Printf("%5d new records in new_contents\n", newComers.new_contents.Cardinality())

	high_cont := sqlite.Get_high_id("contents")
	for raw := range newComers.new_contents.Iter() {
		comp_ix 				:= raw.(CompContents)
		p_meta_blob			:= comp_ix.meta_blob
		row							:= newComers.mem_contents[comp_ix]
		row.status			= "new"
		row.Id					= high_cont
		row.Id_blob			= newComers.mem_index_repo[p_meta_blob].Id
		row.Id_data_idd = newComers.mem_index_repo[row.id_data_idd].Id
		newComers.mem_contents[comp_ix] = row
		high_cont++
		//Printf("new conts %#v\n", row)
	}
	err = CommitNewRecords(&db_aggregate, repositoryData, newComers)
	if err != nil {
		return err
	}
	return nil
}

// attempt to build a generic pattern to generate the memory maps and the
// Sets
type MemBuildFunc func(*DBAggregate, *RepositoryData) any
func CalcuateNewEntries[K1 comparable, V1 any, K2 comparable, V2 any] (
db_map map[K1]V1, mem_map map[K2]V2, repositoryData *RepositoryData,
db_aggregate *DBAggregate, mem_build MemBuildFunc, new_set *mapset.Set) {
	// generic function create memory represenation and then compare
	// with its database partner
	a_mem_table := mem_build(db_aggregate, repositoryData)
	mem_map = a_mem_table.(map[K2]V2)
	*new_set = NewMemoryKeys(db_map, mem_map)
	return
}

func CommitNewRecords(db_aggregate *DBAggregate, repositoryData *RepositoryData,
newComers *Newcomers) error {
	column_names, err := GetColumnNames(db_aggregate.db_conn)
	if err != nil {
		return err
	}
	Printf("db_conn %#v\n", (*db_aggregate).db_conn)
	tx, err := (*db_aggregate).db_conn.Beginx()
	_ = tx
	if err != nil {
		Printf("Cant start transaction. Error is %v\n", err)
		return err
	}
	Printf("BEGIN TRANSACTION\n")
	// snapshots
	for key, data := range newComers.mem_snapshots {
		if data.status != "new" {
			delete (newComers.mem_snapshots, key)
		}
	}
	err = InsertTable("snapshots",  newComers.mem_snapshots, tx, column_names)
	if err != nil {
		return err
	}

	// packfiles
	for key, data := range newComers.mem_packfiles {
		if data.status != "new" {
			delete (newComers.mem_packfiles, key)
		}
	}
	err = InsertTable("packfiles",  newComers.mem_packfiles, tx, column_names)
	if err != nil {
		return err
	}

	// index_repo
	for key, data := range newComers.mem_index_repo {
		if data.status != "new" {
			delete (newComers.mem_index_repo, key)
		}
	}
	err = InsertTable("index_repo",  newComers.mem_index_repo, tx, column_names)
	if err != nil {
		return err
	}

	// names
	for key, data := range newComers.mem_names {
		if data.status != "new" {
			delete (newComers.mem_names, key)
		}
	}
	err = InsertTable("names",  newComers.mem_names, tx, column_names)
	if err != nil {
		return err
	}

	// meta_dir
	for key, data := range newComers.mem_meta_dir {
		if data.status != "new" {
			delete (newComers.mem_meta_dir, key)
		}
	}
	err = InsertTable("meta_dir",  newComers.mem_meta_dir, tx, column_names)
	if err != nil {
		return err
	}

	// idd_file
	for key, data := range newComers.mem_idd_file {
		if data.status != "new" {
			delete (newComers.mem_idd_file, key)
		}
	}
	err = InsertTable("idd_file",  newComers.mem_idd_file, tx, column_names)
	if err != nil {
		return err
	}

	// contents
	for key, data := range newComers.mem_contents {
		if data.status != "new" {
			delete (newComers.mem_contents, key)
		}
	}
	err = InsertTable("contents",  newComers.mem_contents, tx, column_names)
	if err != nil {
		return err
	}

	tx.Commit()
	Printf("COMMIT\n")
	return nil
}

func InsertTable[K comparable, V any](tbl_name string, mem_table map[K]V,
tx *sqlx.Tx, column_names *map[string][]string) error {
	if len(mem_table) == 0 {
		return nil
	}

	// build INSERT statement
	column_list := (*column_names)[tbl_name]
	value_list  := make([]string,  len(column_list))
	for ix ,name := range column_list {
		value_list[ix] = ":" + name
	}

	// extract status=new records from newComers.mem_snapshots and convert them
	// into a slice
	t_insert := make([]V, 0, len(mem_table))
	for _, data := range mem_table {
		t_insert = append(t_insert, data)
		Printf("data  %#v\n", data)
	}

	sql := fmt.Sprintf("INSERT INTO %s(%s) VALUES(%s)", tbl_name,
		strings.Join(column_list, ","), strings.Join(value_list, ","))
	Printf("%s\n", sql)

	_, err := tx.NamedExec(sql, t_insert)

	if err != nil {
		Printf("error INSERT error is %v\n", err)
		return err
	}
	return nil
}

