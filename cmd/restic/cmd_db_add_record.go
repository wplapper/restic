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
	"golang.org/x/sync/errgroup"

	//argparse
	"github.com/spf13/cobra"

	// sets
	"github.com/wplapper/restic/library/mapset"

	// sqlx for sqlite3
	"github.com/jmoiron/sqlx"

	// restic library
	"github.com/wplapper/restic/library/restic"
	"github.com/wplapper/restic/library/sqlite"
)

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
	new_comers.mem_snapshots =  make(map[string]SnapshotRecordMem)
	new_comers.mem_index_repo = make(map[restic.ID]IndexRepoRecordMem)
	new_comers.mem_names =      make(map[string]NamesRecordMem)
	new_comers.mem_idd_file =   make(map[CompIddFile]IddFileRecordMem)
	new_comers.mem_meta_dir =   make(map[CompMetaDir]MetaDirRecordMem)
	new_comers.mem_contents =   make(map[CompContents]ContentsRecordMem)
	new_comers.mem_packfiles =  make(map[*restic.ID]PackfilesRecordMem)

	new_comers.new_snapshots =  mapset.NewSet[string]()
	new_comers.new_index_repo = mapset.NewSet[restic.ID]()
	new_comers.new_names =      mapset.NewSet[string]()
	new_comers.new_idd_file =   mapset.NewSet[CompIddFile]()
	new_comers.new_meta_dir =   mapset.NewSet[CompMetaDir]()
	new_comers.new_contents =   mapset.NewSet[CompContents]()
	new_comers.new_packfiles =  mapset.NewSet[*restic.ID]()
	return &new_comers
}

func init() {
	cmdRoot.AddCommand(cmdDBAdd)
	flags := cmdDBAdd.Flags()
	flags.BoolVarP(&dbOptions.echo, "echo", "E", false, "echo database operations to stdout")
	flags.BoolVarP(&dbOptions.rollback, "rollback", "R", false, "ROOLABCK databae operations")
	flags.StringVarP(&dbOptions.altDB, "DB", "", "", "aternative database name")
}

func runDBAdd(gopts GlobalOptions, args []string) error {
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
	/*
	// get the the highest id for each TABLE
	err = sqlite.Get_all_high_ids()
	if err != nil {
		Printf("db_add_record: Could not get Get_all_high_ids, error is %v\n", err)
		return err
	}*/

	// step 5 with multiple substeps: read the various tables:
	// step 5.1: get table names and their counts
	// gather counts for all tables in database
	names_and_counts := make(map[string]int)
	/*
	err = readAllTablesAndCounts(db_conn, names_and_counts)
	if err != nil {
		Printf("readAllTablesAndCounts error is %v\n", err)
		return err
	}
	*/

	// the first three tables can go parallel
	wg, _ := errgroup.WithContext(gopts.ctx)
	wg.Go (func() error {return sqlite.Get_all_high_ids()})
	wg.Go (func() error {return readAllTablesAndCounts(db_conn, names_and_counts)})
	wg.Go (func() error {return ReadSnapshotTable (db_conn, &db_aggregate)})
	wg.Go (func() error {return ReadNamesTable    (db_conn, &db_aggregate)})
	//wg.Go (func() error {return ReadPackfilesTable(db_conn, &db_aggregate)})
	res := wg.Wait()
	if res != nil {
		Printf("Esrror processing group 1. Error is %v\n", res)
		return res
	}
	db_aggregate.table_counts = names_and_counts

	err = ReadIndexRepoTable(db_conn, &db_aggregate)
	if err != nil {
		Printf("Error processing ReadIndexRepoTable. Error is %v\n", err)
		return err
	}
	err = ReadPackfilesTable(db_conn, &db_aggregate)
	if err != nil {
		Printf("Error processing ReadPackfilesTable. Error is %v\n", err)
		return err
	}

	// the first tree tables can be read in parallel, since they dont depend on one another
	wg1, _ := errgroup.WithContext(gopts.ctx)
	wg1.Go (func() error {return ReadMetaDirTable (db_conn, &db_aggregate)})
	wg1.Go (func() error {return ReadIddFileTable (db_conn, &db_aggregate)})
	wg1.Go (func() error {return ReadContentsTable(db_conn, &db_aggregate)})
	res = wg1.Wait()
	if res != nil {
		Printf("Error processing group 2. Error is %v\n", res)
		return res
	}

	// the last tree tables can be read in parallel, since they dont depend on one another
	wg, _ = errgroup.WithContext(gopts.ctx)
	wg.Go (func() error {return CompareSnapshots(&db_aggregate, repositoryData, newComers)})
	wg.Go (func() error {return CompareNames(&db_aggregate, repositoryData, newComers)})
	wg.Go (func() error {return ComparePackfiles(&db_aggregate, repositoryData, newComers)})
	res = wg.Wait()
	if res != nil {
		Printf("Error processing Compare group 1. Error is %v\n", res)
		return res
	}

	// sync
	CompareIndexRepo(&db_aggregate, repositoryData, newComers)

	wg, _ = errgroup.WithContext(gopts.ctx)
	wg.Go (func() error {return CompareIddFile(&db_aggregate, repositoryData, newComers)})
	wg.Go (func() error {return CompareMetaDir(&db_aggregate, repositoryData, newComers)})
	wg.Go (func() error {return CompareContents(&db_aggregate, repositoryData, newComers)})
	res = wg.Wait()
	if res != nil {
		Printf("Error processing Compare group 2. Error is %v\n", res)
		return res
	}

	CreateBlobSummary(&db_aggregate, repositoryData, newComers)

	// finale: INSERT new records
	err = CommitNewRecords(&db_aggregate, repositoryData, newComers, gopts)
	if err != nil {
		return err
	}
	return nil
}

// CommitNewRecords goes over all the Sets created before and INSERTs the
// newly found data into the database
// because of my lack of generic programming capabilities in Go I have
// to do every TABLE by hand
func CommitNewRecords(db_aggregate *DBAggregate, repositoryData *RepositoryData,
	newComers *Newcomers, gopts GlobalOptions,) error {
	column_names, err := GetColumnNames(db_aggregate.db_conn)
	if err != nil {
		return err
	}

	// BEGIN TRANSACTION
	tx, err := (db_aggregate).db_conn.Beginx()
	_ = tx
	if err != nil {
		Printf("Cant start transaction. Error is %v\n", err)
		return err
	}
	Printf("BEGIN TRANSACTION\n")
	changes_made := false

	wg, _ := errgroup.WithContext(gopts.ctx)
	wg.Go (func() error {return InsertATable("snapshots",  newComers.mem_snapshots,  tx, column_names, &changes_made)})
	wg.Go (func() error {return InsertATable("packfiles",  newComers.mem_packfiles,  tx, column_names, &changes_made)})
	wg.Go (func() error {return InsertATable("index_repo", newComers.mem_index_repo, tx, column_names, &changes_made)})
	wg.Go (func() error {return InsertATable("names",      newComers.mem_names,      tx, column_names, &changes_made)})
	wg.Go (func() error {return InsertATable("meta_dir",   newComers.mem_meta_dir,   tx, column_names, &changes_made)})
	wg.Go (func() error {return InsertATable("idd_file",   newComers.mem_idd_file,   tx, column_names, &changes_made)})
	wg.Go (func() error {return InsertATable("contents",   newComers.mem_contents,   tx, column_names, &changes_made)})
	res := wg.Wait()
	if res != nil {
		Printf("Error processing INSERT INTO tables Error is %v\n", res)
		return res
	}

	// update timestamp
	err = db_update_timestamp(db_aggregate.table_snapshots, tx)
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

func InsertTable[K comparable, V any](tbl_name string, mem_table map[K]V,
	tx *sqlx.Tx, column_names map[string][]string, changes_made *bool) error {
	if len(mem_table) == 0 {
		return nil
	}

	// build INSERT statement
	column_list := column_names[tbl_name]
	value_list := make([]string, len(column_list))
	for ix, name := range column_list {
		value_list[ix] = ":" + name
	}

	t_insert := make([]V, 0, len(mem_table))
	for _, data := range mem_table {
		t_insert = append(t_insert, data)
	}

	sql := fmt.Sprintf("INSERT INTO %s(%s) VALUES(%s)", tbl_name,
		strings.Join(column_list, ", "), strings.Join(value_list, ", "))
	if dbOptions.echo {
		Printf("%s\n", sql)
	}

	// do the INSERT
	// te splittin into sements could a bit more dynamic
	// its is imited by te product of column * rows_inserted
	for offset := 0; offset < len(t_insert); offset += 1000 {
		max := len(t_insert)
		if max > 1000 {
			max = 1000
		}
		if offset + max > len(t_insert) {
			max = len(t_insert) - offset
		}
		//Printf("INSERT from %4d to %4d\n", offset, offset + max)
		r, err := tx.NamedExec(sql, t_insert[offset:offset + max])
		if err != nil {
			Printf("error INSERT %s error is: %v\n", tbl_name, err)
			return err
		}
		count,_ := r.RowsAffected()
		if count > 0 {
			*changes_made = true
		}
	}
	Printf("%7d rows inserted into table %s\n", len(t_insert), tbl_name)
	return nil
}

// ShowBlobsPerSnap maps the new meta and data blobs to one of the new
// snapshots
func ShowBlobsPerSnap(repositoryData *RepositoryData, newComers *Newcomers) {
	// create a set of the new blobs
	// new_blob_set is the Set of al new blobs
	new_blob_set := mapset.NewSet[restic.ID]()
	for key := range newComers.mem_index_repo {
		new_blob_set.Add(key)
	}

	// assign all the new blobs to one or more snapshots
	allocate_map := make(map[restic.ID]mapset.Set[string])
	for raw := range newComers.new_snapshots.Iter() {
		snap_id := raw
		row := newComers.mem_snapshots[snap_id]
		// we need access to the full *restic.ID of the snap
		// reference and dereference the pointer properly
		ptr_snap_id := &(repositoryData.index_to_blob[repositoryData.blob_to_index[*row.ID_mem]])

		// all meta blobs of this snap
		for int_blob := range repositoryData.meta_dir_map[ptr_snap_id] { // IntSet()
			meta_blob := repositoryData.index_to_blob[int_blob]

			// access the data blobs
			for _, meta := range repositoryData.directory_map[int_blob] {
				for _, cont_int := range meta.content {
					cont := repositoryData.index_to_blob[cont_int]
					if new_blob_set.Contains(cont) {
						_, ok := allocate_map[cont]
						if !ok {
							allocate_map[cont] = mapset.NewSet[string]()
						}
						allocate_map[cont].Add(snap_id)
					}
				}
			}

			// insert meta blob if found
			if new_blob_set.Contains(meta_blob) {
				_, ok := allocate_map[meta_blob]
				if !ok {
					allocate_map[meta_blob] = mapset.NewSet[string]()
				}
				allocate_map[meta_blob].Add(snap_id)
			}
		}
	}

	//
	map_snap_to_blobs := make(map[string]mapset.Set[restic.ID])
	for meta_blob, snap_set := range allocate_map {
		// we need to go over all members of 'snap_set'
		for raw := range snap_set.Iter() {
			snap_id := raw
			_, ok := map_snap_to_blobs[snap_id]
			if !ok {
				map_snap_to_blobs[snap_id] = mapset.NewSet[restic.ID]()
			}
			// finally assign blob to a snap
			map_snap_to_blobs[snap_id].Add(meta_blob)
			// stop here after first element
			break
		}
	}

	for snap_id, blob_set := range map_snap_to_blobs {
		sum_data_blobs := uint64(0)
		sum_meta_blobs := uint64(0)
		count_meta_blobs := 0
		count_data_blobs := 0
		for blob := range blob_set.Iter() {
			ih := repositoryData.index_handle[blob]
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
		Printf("sum  %5d %10.3f MiB\n", count_data_blobs + count_meta_blobs,
			float64(sum_data_blobs + sum_meta_blobs)/ONE_MEG)
	}
	Printf("\n")
}

// this is generic comparision function
type MemBuildFunc func(*DBAggregate, *RepositoryData, *Newcomers)
func CalcuateNewEntries[K comparable, V1 any, V2 any](
	db_map map[K]V1, mem_map map[K]V2, repositoryData *RepositoryData,
	db_aggregate *DBAggregate, mem_build MemBuildFunc, new_set *mapset.Set[K],
	new_comers *Newcomers) {

	// build memory map
	mem_build(db_aggregate, repositoryData, new_comers)
	//compare the two maps
	*new_set = NewMemoryKeys(db_map, mem_map)
	return
}

// generic function using 'reflect' to access the status field
// for removal of all rows which do not have a status of "new"
func filter_new[K comparable, V any](mem_map map[K]V, status string) {
	print_count := 0
	for key, data := range mem_map {
		v := reflect.ValueOf(data)
		if print_count < 3 {
			for i := 0; i < v.NumField(); i++ {
				the_field := v.Type().Field(i)
				fieldName := the_field.Name
				varType   := the_field.Type
				Printf("%-20s %s\n", fieldName, varType)
			}
		}
		print_count++
		if v.FieldByName("Status").String() != status {
			delete(mem_map, key)
		}
	}
}

// generic funtion to INSERT new data into the database
func InsertATable[K comparable, V any](table_name string, mem_map map[K]V,
tx *sqlx.Tx,  column_names map[string][]string, changes_made *bool) error {
	filter_new(mem_map, "new")
	err := InsertTable(table_name, mem_map, tx, column_names, changes_made)
	if err != nil {
		return err
	}
	return nil
}

func CompareSnapshots(db_aggregate *DBAggregate, repositoryData *RepositoryData,
newComers *Newcomers) error {
	// compare snapshots in memory with snapshots in the database
	newComers.mem_snapshots = CreateMemSnapshots(db_aggregate, repositoryData, newComers)
	newComers.new_snapshots = NewMemoryKeys(db_aggregate.table_snapshots, newComers.mem_snapshots)
	high_snap := sqlite.Get_high_id("snapshots")
	for snap := range newComers.new_snapshots.Iter() {
		snap_id := snap
		row := newComers.mem_snapshots[snap_id]
		row.Status = "new"
		row.Id = high_snap
		row.Snap_id = snap_id
		row.Id_snap_root = row.ID_mem.String()
		newComers.mem_snapshots[snap_id] = row
		high_snap++
	}
	Printf("%7d new records in new_snapshots\n", newComers.new_snapshots.Cardinality())
	return nil
}

func CompareNames(db_aggregate *DBAggregate, repositoryData *RepositoryData,
newComers *Newcomers) error {
	// compare names with its DB counterpart
	newComers.mem_names = CreateMemNames(db_aggregate, repositoryData, newComers)
	newComers.new_names = NewMemoryKeys(db_aggregate.table_names, newComers.mem_names)
	Printf("%7d new records in new_names\n", newComers.new_names.Cardinality())

	high_names := sqlite.Get_high_id("names")
	for raw := range newComers.new_names.Iter() {
		name := raw
		row := newComers.mem_names[name]
		row.Status = "new"
		row.Id = high_names
		row.Name = name
		row.Name_type = "b" // there is no other name type (left)
		newComers.mem_names[name] = row
		high_names++
	}
	return nil
}

func ComparePackfiles(db_aggregate *DBAggregate, repositoryData *RepositoryData,
newComers *Newcomers) error {
	// compare packfiles with its DB counterpart
	newComers.mem_packfiles = CreateMemPackfiles(db_aggregate, repositoryData, newComers)
	newComers.new_packfiles = NewMemoryKeys(db_aggregate.table_packfiles, newComers.mem_packfiles)
	Printf("%7d new records in new_packfiles\n", newComers.new_packfiles.Cardinality())

	high_pack := sqlite.Get_high_id("packfiles")
	for raw := range newComers.new_packfiles.Iter() {
		pack_ID_ptr := raw
		row := newComers.mem_packfiles[pack_ID_ptr]
		row.Status = "new"
		row.Id = high_pack
		row.Packfile_id = (*pack_ID_ptr).String()
		newComers.mem_packfiles[pack_ID_ptr] = row
		high_pack++
	}
	return nil
}

func CompareIndexRepo(db_aggregate *DBAggregate, repositoryData *RepositoryData,
newComers *Newcomers) error {
	// compare index_repo with its DB counterpart
	newComers.mem_index_repo = CreateMemIndexRepo(db_aggregate, repositoryData, newComers)
	newComers.new_index_repo = NewMemoryKeys(db_aggregate.table_index_repo, newComers.mem_index_repo)
	Printf("%7d new records in new_index_repo\n", newComers.new_index_repo.Cardinality())

	high_repo := sqlite.Get_high_id("index_repo")
	for blob := range newComers.new_index_repo.Iter() {
		if blob == EMPTY_NODE_ID {
			continue
		}
		ih 				:= repositoryData.index_handle[blob]
		pack_Int 	:= ih.pack_index
		row 			:= newComers.mem_index_repo[blob]
		row.Status = "new"
		row.Id 		= high_repo
		row.Idd 	= blob.String()
		ptr 			:= &(repositoryData.index_to_blob[pack_Int])
		row.Id_pack_id = newComers.mem_packfiles[ptr].Id
		row.packfile = ptr
		newComers.mem_index_repo[blob] = row

		// consistency check
		if row.Id_pack_id == 0 {
			Printf("Logic error for pack pointer %p in index_repo %s IndexHandle %#v\n",
				ptr, blob.String()[:12], ih)
			panic("new index_repo")
		}
		high_repo++
	}
	return nil
}

func CompareIddFile(db_aggregate *DBAggregate, repositoryData *RepositoryData,
newComers *Newcomers) error {
	// compare idd_file with its DB counterpart
	newComers.mem_idd_file = CreateMemIddFile(db_aggregate, repositoryData, newComers)
	newComers.new_idd_file = NewMemoryKeys(db_aggregate.table_idd_file, newComers.mem_idd_file)
	Printf("%7d new records in new_idd_file\n", newComers.new_idd_file.Cardinality())

	high_idd := sqlite.Get_high_id("idd_file")
	for raw := range newComers.new_idd_file.Iter() {
		comp_ix := raw
		meta_blob := comp_ix.meta_blob
		position := comp_ix.position
		row := newComers.mem_idd_file[comp_ix]
		meta := repositoryData.directory_map[repositoryData.blob_to_index[meta_blob]][position]
		name := meta.name
		row.Status = "new"
		row.Id = high_idd
		row.Id_blob = newComers.mem_index_repo[meta_blob].Id
		row.Position = position
		row.Id_name = newComers.mem_names[name].Id
		newComers.mem_idd_file[comp_ix] = row
		high_idd++

		// consistency check
		if row.Id_blob == 0 {
			Printf("Logic error for blob pointer %s in idd_file\n",
				meta_blob.String()[:12])
			panic("new idd_file 1")
		}
		if row.Id_blob == 0 {
			Printf("Logic error for name pointer %s in idd_file\n", name)
			panic("new idd_file 2")
		}
	}
	return nil
}

func CompareMetaDir(db_aggregate *DBAggregate, repositoryData *RepositoryData,
newComers *Newcomers) error {
	// compare meta_dir with its DB counterpart
	newComers.mem_meta_dir = CreateMemMetaDir(db_aggregate, repositoryData, newComers)
	newComers.new_meta_dir = NewMemoryKeys(db_aggregate.table_meta_dir, newComers.mem_meta_dir)
	Printf("%7d new records in new_meta_dir\n", newComers.new_meta_dir.Cardinality())

	high_mdir := sqlite.Get_high_id("meta_dir")
	for comp_ix := range newComers.new_meta_dir.Iter() {
		snap_id 				:= comp_ix.snap_id
		meta_blob       := comp_ix.meta_blob
		row 						:= newComers.mem_meta_dir[comp_ix]
		row.Status 			= "new"
		row.Id 					= high_mdir
		newComers.mem_meta_dir[comp_ix] = row
		high_mdir++

		//  consistency check
		if row.Id_snap_id == 0 {
			Printf("Logic error for meta_dir.snap_idd %s %#v\n",
				snap_id, newComers.mem_snapshots[snap_id])
			panic("meta_dir 1")
		}
		if row.Id_idd == 0 {
			Printf("Logic error for meta_dir.blob %s %#v\n",
				snap_id, newComers.mem_index_repo[meta_blob])
			panic("meta_dir 2")
		}
	}
	return nil
}

func CompareContents(db_aggregate *DBAggregate, repositoryData *RepositoryData,
newComers *Newcomers) error {
	// compare contents with its DB counterpart
	newComers.mem_contents = CreateMemContents(db_aggregate, repositoryData, newComers)
	newComers.new_contents = NewMemoryKeys(db_aggregate.table_contents, newComers.mem_contents)
	Printf("%7d new records in new_contents\n", newComers.new_contents.Cardinality())

	high_cont := sqlite.Get_high_id("contents")
	for raw := range newComers.new_contents.Iter() {
		comp_ix := raw
		p_meta_blob := comp_ix.meta_blob
		row := newComers.mem_contents[comp_ix]
		row.Status = "new"
		row.Id = high_cont
		row.Id_blob = newComers.mem_index_repo[p_meta_blob].Id
		row.Id_data_idd = newComers.mem_index_repo[*(row.id_data_idd)].Id
		newComers.mem_contents[comp_ix] = row
		high_cont++

		// check consistency
		if row.Id_blob == 0 {
			Printf("Logic error for meta_dir.blob %s %#v\n",
				p_meta_blob.String()[:12], newComers.mem_index_repo[p_meta_blob])
			panic("contents 1")
		}
		if row.Id_data_idd == 0 {
			Printf("Logic error for meta_dir.blob %s %#v\n",
				(*(row.id_data_idd)).String()[:12], newComers.mem_index_repo[*(row.id_data_idd)])
			panic("contents 2")
		}
	}
	return nil
}

func CreateBlobSummary(db_aggregate *DBAggregate, repositoryData *RepositoryData, newComers *Newcomers) {
	// create blob summary
	for key, data := range newComers.mem_index_repo {
		if data.Status != "new" {
			delete(newComers.mem_index_repo, key)
		}
	}
	sum_data_blobs := uint64(0)
	sum_meta_blobs := uint64(0)
	count_meta_blobs := 0
	count_data_blobs := 0
	for blob := range newComers.mem_index_repo {
		ih := repositoryData.index_handle[blob]
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
	CalcuateNewEntries(db_aggregate.table_names, newComers.mem_names,
		repositoryData, db_aggregate, CreateMemNamesV2, &newComers.new_names, newComers)
}

// manae timestamp table (with one row)
func db_update_timestamp(table_snapshots map[string]SnapshotRecordMem, tx *sqlx.Tx) error {
	max_time := ""
	for _, data := range table_snapshots {
		if data.Snap_time > max_time {
			max_time = data.Snap_time
		}
	}
	//  convert youngest time from snapshots
	max_snap_time, err := time.Parse("2006-01-02 15:04:05", max_time)
	if err != nil {
		Printf("GetMaxSnaptime: error parsing time %v\n", err)
		panic("GetMaxSnaptime: error parsing time")
	}

	// get restic_updated time from timestamp
	var restic_updated time.Time
	sql := "SELECT restic_updated FROM timestamp WHERE id = 1"
	err = tx.Get(&restic_updated, sql)
	if err != nil {
		Printf("UPDATE timestamp failed 1: %v\n", err)
		return err
	}

	if restic_updated.Before(max_snap_time) {
		sql = `UPDATE timestamp SET database_updated =
              (SELECT datetime('now', 'localtime')),
              restic_updated = :restic_updated WHERE id = 1`
    _, err := tx.Exec(sql, max_snap_time)
    if err != nil {
			Printf("UPDATE timestamp failed 1: %v\n", err)
			return err
		}
   } else {
		 sql := `UPDATE timestamp SET database_updated =
               (SELECT datetime('now', 'localtime')) WHERE id = 1`
    _, err := tx.Exec(sql)
    if err != nil {
			Printf("UPDATE timestamp failed 2: %v\n", err)
			return err
		}
	}
	return nil
}
