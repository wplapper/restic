package main

// compile with "go run build.go -tags debug""
// run with DEBUG_LOG=/home/wplapper/restic/debug.log fully-qualified-name/restic -r <repo> overview

// ref to onedrive: rclone:onedrive:restic_backups

import (
	// system
	"fmt"
	"strings"
	"time"
	"reflect"

	//argparse
	"github.com/spf13/cobra"

	// sets
	"github.com/deckarep/golang-set"
	//"../../libary/mapset"
	"github.com/wplapper/restic/library/mapset"

	// sqlx for sqlite3
	"github.com/jmoiron/sqlx"

	// restic library
	"github.com/wplapper/restic/library/restic"
	"github.com/wplapper/restic/library/sqlite"
)

type Newcomers struct {
	// setup variables and maps to catch the newcomers
	mem_snapshots  map[string]SnapshotRecordMem
	mem_index_repo map[*restic.ID]IndexRepoRecordMem
	mem_names      map[string]NamesRecordMem
	mem_idd_file   map[CompIddFile]IddFileRecordMem
	mem_meta_dir   map[CompMetaDir]MetaDirRecordMem
	mem_contents   map[CompContents]ContentsRecordMem
	mem_packfiles  map[*restic.ID]PackfilesRecordMem

	new_snapshots  mapset.Set
	new_index_repo mapset.Set
	new_names      mapset.Set
	new_idd_file   mapset.Set
	new_meta_dir   mapset.Set
	new_contents   mapset.Set
	new_packfiles  mapset.Set
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
	new_comers.mem_snapshots = make(map[string]SnapshotRecordMem)
	new_comers.mem_index_repo = make(map[*restic.ID]IndexRepoRecordMem)
	new_comers.mem_names = make(map[string]NamesRecordMem)
	new_comers.mem_idd_file = make(map[CompIddFile]IddFileRecordMem)
	new_comers.mem_meta_dir = make(map[CompMetaDir]MetaDirRecordMem)
	new_comers.mem_contents = make(map[CompContents]ContentsRecordMem)
	new_comers.mem_packfiles = make(map[*restic.ID]PackfilesRecordMem)

	new_comers.new_snapshots = mapset.NewSet()
	new_comers.new_index_repo = mapset.NewSet()
	new_comers.new_names = mapset.NewSet()
	new_comers.new_idd_file = mapset.NewSet()
	new_comers.new_meta_dir = mapset.NewSet()
	new_comers.new_contents = mapset.NewSet()
	new_comers.new_packfiles = mapset.NewSet()
	return &new_comers
}

func init() {
	cmdRoot.AddCommand(cmdDBAdd)
	flags := cmdDBAdd.Flags()
	flags.BoolVarP(&dbOptions.echo, "echo", "E", false, "echo database operations to stdout")
	flags.BoolVarP(&dbOptions.rollback, "rollback", "R", false, "ROOLABCK databae operations")
}

func runDBAdd(gopts GlobalOptions, args []string) error {
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
	// get the the highest id for each TABLE
	err = sqlite.Get_all_high_ids()
	if err != nil {
		Printf("db_add_record: Could not get Get_all_high_ids, error is %v\n", err)
		return err
	}

	// step 5 with multiple substeps: read the various tables:
	// step 5.1: get table names and their counts
	// gather counts for all tables in database
	names_and_counts := make(map[string]int)
	err = readAllTablesAndCounts(db_conn, &names_and_counts)
	if err != nil {
		Printf("readAllTablesAndCounts error is %v\n", err)
		return err
	}
	db_aggregate.table_counts = &names_and_counts

	// action loop for reading all database tables
	type action_function func(*sqlx.DB, *DBAggregate) error
	type ActionStruct struct {
		table_name string
		routine    action_function
	}
	var actions = []ActionStruct{
		{table_name: "snapshots", routine: ReadSnapshotTable},
		{table_name: "index_repo", routine: ReadIndexRepoTable},
		{table_name: "meta_dir", routine: ReadMetaDirTable},
		{table_name: "names", routine: ReadNamesTable},
		{table_name: "idd_file", routine: ReadIddFileTable},
		{table_name: "packfiles", routine: ReadPackfilesTable},
		{table_name: "contents", routine: ReadContentsTable}}

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
		snap_id := snap.(string)
		row := newComers.mem_snapshots[snap_id]
		row.status = "new"
		row.Id = high_snap
		row.Snap_id = snap_id
		row.Id_snap_root = row.ID_mem.String()
		newComers.mem_snapshots[snap_id] = row
		high_snap++
	}
	Printf("%5d new records in new_snapshots\n", newComers.new_snapshots.Cardinality())

	// compare names with its DB counterpart
	newComers.mem_names = CreateMemNames(&db_aggregate, repositoryData)
	newComers.new_names = NewMemoryKeys(*db_aggregate.table_names, newComers.mem_names)
	Printf("%5d new records in new_names\n", newComers.new_names.Cardinality())

	high_names := sqlite.Get_high_id("names")
	for raw := range newComers.new_names.Iter() {
		name := raw.(string)
		row := newComers.mem_names[name]
		row.status = "new"
		row.Id = high_names
		row.Name = name
		row.Name_type = "b" // there is no other name type (left)
		newComers.mem_names[name] = row
		high_names++
	}

	// compare packfiles with its DB counterpart
	newComers.mem_packfiles = CreateMemPackfiles(&db_aggregate, repositoryData)
	newComers.new_packfiles = NewMemoryKeys(*db_aggregate.table_packfiles, newComers.mem_packfiles)
	Printf("%5d new records in new_packfiles\n", newComers.new_packfiles.Cardinality())

	high_pack := sqlite.Get_high_id("packfiles")
	for raw := range newComers.new_packfiles.Iter() {
		pack_ID_ptr := raw.(*restic.ID)
		row := newComers.mem_packfiles[pack_ID_ptr]
		row.status = "new"
		row.Id = high_pack
		row.Packfile_id = (*pack_ID_ptr).String()

		// check if we need to update the back pointer
		//(*db_aggregate.pk_packfiles)[high_pack] = pack_ID_ptr
		newComers.mem_packfiles[pack_ID_ptr] = row
		high_pack++
	}

	// compare index_repo with its DB counterpart
	newComers.mem_index_repo = CreateMemIndexRepo(&db_aggregate, repositoryData)
	newComers.new_index_repo = NewMemoryKeys(*db_aggregate.table_index_repo, newComers.mem_index_repo)
	Printf("%5d new records in new_index_repo\n", newComers.new_index_repo.Cardinality())

	high_repo := sqlite.Get_high_id("index_repo")
	for unchecked := range newComers.new_index_repo.Iter() {
		blob := unchecked.(*restic.ID)
		ih := repositoryData.index_handle[*blob]
		pack_Int := ih.pack_index
		row := newComers.mem_index_repo[blob]
		row.status = "new"
		row.Id = high_repo
		row.Idd = (*blob).String()
		ptr := &(repositoryData.index_to_blob[pack_Int])
		row.Id_pack_id = newComers.mem_packfiles[ptr].Id
		row.packfile = ptr
		newComers.mem_index_repo[blob] = row
		high_repo++
	}

	// compare idd_file with its DB counterpart
	newComers.mem_idd_file = CreateMemIddFile(&db_aggregate, repositoryData)
	newComers.new_idd_file = NewMemoryKeys(*db_aggregate.table_idd_file, newComers.mem_idd_file)
	Printf("%5d new records in new_idd_file\n", newComers.new_idd_file.Cardinality())

	high_idd := sqlite.Get_high_id("idd_file")
	for raw := range newComers.new_idd_file.Iter() {
		comp_ix := raw.(CompIddFile)
		meta_blob := comp_ix.meta_blob
		position := comp_ix.position
		row := newComers.mem_idd_file[comp_ix]
		meta := repositoryData.directory_map[repositoryData.blob_to_index[*meta_blob]][position]
		name := meta.name
		row.status = "new"
		row.Id = high_idd
		ptr := &(repositoryData.index_to_blob[repositoryData.blob_to_index[*meta_blob]])
		row.Id_blob = newComers.mem_index_repo[ptr].Id
		row.Position = position
		row.Id_name = newComers.mem_names[name].Id
		newComers.mem_idd_file[comp_ix] = row
		high_idd++
	}

	// compare meta_dir with its DB counterpart
	newComers.mem_meta_dir = CreateMemMetaDir(&db_aggregate, repositoryData)
	newComers.new_meta_dir = NewMemoryKeys(*db_aggregate.table_meta_dir, newComers.mem_meta_dir)
	Printf("%5d new records in new_meta_dir\n", newComers.new_meta_dir.Cardinality())

	high_mdir := sqlite.Get_high_id("meta_dir")
	for raw := range newComers.new_meta_dir.Iter() {
		comp_ix := raw.(CompMetaDir)
		snap_id := comp_ix.snap_id
		p_meta_blob := comp_ix.meta_blob
		row := newComers.mem_meta_dir[comp_ix]
		row.status = "new"
		row.Id = high_mdir
		row.Id_snap_id = newComers.mem_snapshots[snap_id].Id
		row.Id_idd = newComers.mem_index_repo[p_meta_blob].Id
		newComers.mem_meta_dir[comp_ix] = row
		high_mdir++
	}

	// compare contents with its DB counterpart
	newComers.mem_contents = CreateMemContents(&db_aggregate, repositoryData)
	newComers.new_contents = NewMemoryKeys(*db_aggregate.table_contents, newComers.mem_contents)
	Printf("%5d new records in new_contents\n", newComers.new_contents.Cardinality())

	high_cont := sqlite.Get_high_id("contents")
	for raw := range newComers.new_contents.Iter() {
		comp_ix := raw.(CompContents)
		p_meta_blob := comp_ix.meta_blob
		row := newComers.mem_contents[comp_ix]
		row.status = "new"
		row.Id = high_cont
		row.Id_blob = newComers.mem_index_repo[p_meta_blob].Id
		row.Id_data_idd = newComers.mem_index_repo[row.id_data_idd].Id
		newComers.mem_contents[comp_ix] = row
		high_cont++
	}

	// create blob summary
	for key, data := range newComers.mem_index_repo {
		if data.status != "new" {
			delete(newComers.mem_index_repo, key)
		}
	}
	sum_data_blobs := uint64(0)
	sum_meta_blobs := uint64(0)
	count_meta_blobs := 0
	count_data_blobs := 0
	for blob := range newComers.mem_index_repo {
		ih := repositoryData.index_handle[*blob]
		typ := ih.Type.String()[0:1]
		if typ == "t" {
			sum_meta_blobs += uint64(ih.size)
			count_meta_blobs++
		} else if typ == "d" {
			sum_data_blobs += uint64(ih.size)
			count_data_blobs++
		}
	}
	Printf("\n*** Summary of new data in database ***\n")
	Printf("%s %7d %10.3f MiB\n", "meta", count_meta_blobs, float64(sum_meta_blobs)/ONE_MEG)
	Printf("%s %7d %10.3f MiB\n", "data", count_data_blobs, float64(sum_data_blobs)/ONE_MEG)
	Printf("%s %7d %10.3f MiB\n", "sum ", count_data_blobs+count_meta_blobs,
		float64(sum_data_blobs+sum_meta_blobs)/ONE_MEG)
	Printf("\n")

	ShowBlobsPerSnap(repositoryData, newComers)
	CalcuateNewEntries(*db_aggregate.table_names, newComers.mem_names,
		repositoryData, &db_aggregate, CreateMemNamesV2, &newComers.new_names, newComers)

	// finale: INSERT new records
	err = CommitNewRecords(&db_aggregate, repositoryData, newComers)
	if err != nil {
		return err
	}
	return nil
}

// CommitNewRecords goes over all the Sets created before and INSERTs the
// newly found dta into the database
// because of my lack of generic programming capabilities in Go I have
// to do every TABLE by hand
func CommitNewRecords(db_aggregate *DBAggregate, repositoryData *RepositoryData,
	newComers *Newcomers) error {
	column_names, err := GetColumnNames(db_aggregate.db_conn)
	if err != nil {
		return err
	}

	// BEGIN TRANSACTION
	tx, err := (*db_aggregate).db_conn.Beginx()
	_ = tx
	if err != nil {
		Printf("Cant start transaction. Error is %v\n", err)
		return err
	}
	Printf("BEGIN TRANSACTION\n")


	//type K comparable
	//type V any
	type type_InsertATable func(string, interface{}, *sqlx.Tx, map[string][]string)
	type InsertRecord struct {
		table_name string
		mem_tab interface{}
	}
	var insert_struct = []InsertRecord {InsertRecord{table_name: "snapshots", mem_tab: newComers.mem_snapshots}}
	for _, entry := range insert_struct {
		err = InsertATable(entry.table_name, entry.mem_tab.(map), tx, column_names)
		if err != nil {
			return err
		}
	}

	err = InsertATable("snapshots", newComers.mem_snapshots, tx, column_names)
	if err != nil {
		return err
	}

	err = InsertATable("packfiles", newComers.mem_packfiles, tx, column_names)
	if err != nil {
		return err
	}

	err = InsertATable("index_repo", newComers.mem_index_repo, tx, column_names)
	if err != nil {
		return err
	}

	err = InsertATable("names", newComers.mem_names, tx, column_names)
	if err != nil {
		return err
	}

	err = InsertATable("meta_dir", newComers.mem_meta_dir, tx, column_names)
	if err != nil {
		return err
	}

	err = InsertATable("idd_file", newComers.mem_idd_file, tx, column_names)
	if err != nil {
		return err
	}

	err = InsertATable("contents", newComers.mem_contents, tx, column_names)
	if err != nil {
		return err
	}

	// COMMIT or ROLLBACK transaction
	if !dbOptions.rollback {
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

func InsertTable[K comparable, V any](tbl_name string, mem_table map[K]V,
	tx *sqlx.Tx, column_names *map[string][]string) error {
	if len(mem_table) == 0 {
		return nil
	}

	// build INSERT statement
	column_list := (*column_names)[tbl_name]
	value_list := make([]string, len(column_list))
	for ix, name := range column_list {
		value_list[ix] = ":" + name
	}

	t_insert := make([]V, 0, len(mem_table))
	for _, data := range mem_table {
		t_insert = append(t_insert, data)
	}

	sql := fmt.Sprintf("INSERT INTO %s(%s) VALUES(%s)", tbl_name,
		strings.Join(column_list, ","), strings.Join(value_list, ","))
	if dbOptions.echo {
		Printf("%s\n", sql)
	}

	// do the INSERT
	_, err := tx.NamedExec(sql, t_insert)
	if err != nil {
		Printf("error INSERT error is %v\n", err)
		return err
	}
	Printf("%7d rows inserted into table %s\n", len(t_insert), tbl_name)
	return nil
}

// ShowBlobsPerSnap maps the new meta and data blobs to one of the new
// snapshots
func ShowBlobsPerSnap(repositoryData *RepositoryData, newComers *Newcomers) {
	// create a set of the new blobs
	// new_blob_set is the Set of al new blobs
	new_blob_set := mapset.NewSet()
	for key := range newComers.mem_index_repo {
		new_blob_set.Add(key)
	}

	// assign all the new blobs to one or more snapshots
	allocate_map := make(map[*restic.ID]mapset.Set)
	for raw := range newComers.new_snapshots.Iter() {
		snap_id := raw.(string)
		row := newComers.mem_snapshots[snap_id]
		// we need access to the full *restic.ID of the snap
		// reference and dereference the pointer properly
		ptr_snap_id := &(repositoryData.index_to_blob[repositoryData.blob_to_index[*row.ID_mem]])

		// all meta blobs of this snap
		for int_blob := range repositoryData.meta_dir_map[ptr_snap_id] { // IntSet()
			ptr_meta_blob := &(repositoryData.index_to_blob[int_blob])

			// access the data blobs
			for _, meta := range repositoryData.directory_map[int_blob] {
				for _, cont_int := range meta.content {
					cont := &(repositoryData.index_to_blob[cont_int])
					if new_blob_set.Contains(cont) {
						_, ok := allocate_map[cont]
						if !ok {
							allocate_map[cont] = mapset.NewSet()
						}
						allocate_map[cont].Add(snap_id)
					}
				}
			}

			// insert meta blob if found
			if new_blob_set.Contains(ptr_meta_blob) {
				_, ok := allocate_map[ptr_meta_blob]
				if !ok {
					allocate_map[ptr_meta_blob] = mapset.NewSet()
				}
				allocate_map[ptr_meta_blob].Add(snap_id)
			}
		}
	}

	//
	map_snap_to_blobs := make(map[string]mapset.Set)
	for ptr_meta_blob, snap_set := range allocate_map {
		// we need to go over all members of 'snap_set'
		for raw := range snap_set.Iter() {
			snap_id := raw.(string)
			_, ok := map_snap_to_blobs[snap_id]
			if !ok {
				map_snap_to_blobs[snap_id] = mapset.NewSet()
			}
			// finally assign blob to a snap
			map_snap_to_blobs[snap_id].Add(ptr_meta_blob)
			// stop here after first element
			break
		}
	}

	for snap_id, blob_set := range map_snap_to_blobs {
		sum_data_blobs := uint64(0)
		sum_meta_blobs := uint64(0)
		count_meta_blobs := 0
		count_data_blobs := 0
		for raw := range blob_set.Iter() {
			blob := raw.(*restic.ID)
			ih := repositoryData.index_handle[*blob]
			typ := ih.Type.String()[0:1]
			if typ == "t" {
				sum_meta_blobs += uint64(ih.size)
				count_meta_blobs++
			} else if typ == "d" {
				sum_data_blobs += uint64(ih.size)
				count_data_blobs++
			}
		}
		row := newComers.mem_snapshots[snap_id]
		Printf("\n*** snap %s %s %s:%s ***\n", snap_id, row.Snap_time, row.Snap_host, row.Snap_fsys)
		Printf("meta %5d %10.3f MiB\n", count_meta_blobs, float64(sum_meta_blobs)/ONE_MEG)
		Printf("data %5d %10.3f MiB\n", count_data_blobs, float64(sum_data_blobs)/ONE_MEG)
		Printf("sum  %5d %10.3f MiB\n", count_data_blobs+count_meta_blobs,
			float64(sum_data_blobs+sum_meta_blobs)/ONE_MEG)
	}
	Printf("\n")
}

/* Try to emulate:
	// compare contents with its DB counterpart
	CreateMemContentsV2(&db_aggregate, repositoryData)
	newComers.new_contents = NewMemoryKeys(*db_aggregate.table_contents, newComers.mem_contents)
*/
// FAILED attempt to build a generic pattern to generate the memory maps and the
// Sets
type MemBuildFunc func(*DBAggregate, *RepositoryData, *Newcomers)

func CalcuateNewEntries[K1 comparable, V1 any, K2 comparable, V2 any](
	db_map map[K1]V1, mem_map map[K2]V2, repositoryData *RepositoryData,
	db_aggregate *DBAggregate, mem_build MemBuildFunc, new_set *mapset.Set,
	new_comers *Newcomers) {

	// build memory map
	mem_build(db_aggregate, repositoryData, new_comers)
	//compare the two maps
	*new_set = NewMemoryKeys(db_map, mem_map)
	return
}

// generic function using 'reflect' to access the status field
func filter_new[K comparable, V any](mem_map map[K]V) {
	for key, data := range mem_map {
		v := reflect.ValueOf(data)
		if v.FieldByName("status").String() != "new" {
			delete(mem_map, key)
		}
	}
}

func InsertATable[K comparable, V any](table_name string, mem_map map[K]V,
tx *sqlx.Tx,  column_names *map[string][]string) error {
	filter_new(mem_map)
	err := InsertTable(table_name, mem_map, tx, column_names)
	if err != nil {
		return err
	}
	return nil
}
