package main

// compile with "go run build.go -tags debug""
// run with DEBUG_LOG=/home/wplapper/restic/debug.log fully-qualified-name/restic -r <repo> overview

// ref to onedrive: rclone:onedrive:restic_backups

import (
	// system
	"fmt"
	"golang.org/x/sync/errgroup"
	"reflect"
	"strings"
	"runtime"
	"time"

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

var repositoryData *RepositoryData
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
	new_comers.mem_snapshots = make(map[string]*SnapshotRecordMem)
	new_comers.mem_index_repo = make(map[restic.ID]*IndexRepoRecordMem)
	new_comers.mem_names = make(map[string]*NamesRecordMem)
	new_comers.mem_idd_file = make(map[CompIddFile]*IddFileRecordMem)
	new_comers.mem_meta_dir = make(map[CompMetaDir]*MetaDirRecordMem)
	new_comers.mem_contents = make(map[CompContents]*ContentsRecordMem)
	new_comers.mem_packfiles = make(map[*restic.ID]*PackfilesRecordMem)

	new_comers.new_snapshots = mapset.NewSet[string]()
	new_comers.new_index_repo = mapset.NewSet[restic.ID]()
	new_comers.new_names = mapset.NewSet[string]()
	new_comers.new_idd_file = mapset.NewSet[CompIddFile]()
	new_comers.new_meta_dir = mapset.NewSet[CompMetaDir]()
	new_comers.new_contents = mapset.NewSet[CompContents]()
	new_comers.new_packfiles = mapset.NewSet[*restic.ID]()
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
	Printf("Usage START\n")
	PrintMemUsage()
	repositoryData = init_repositoryData() // is a *RepositoryData
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
	PrintMemUsage()

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
	Printf("After HandleIndexRecords\n")
	PrintMemUsage()

	// step 3: collect all snapshot related information
	start = time.Now()
	GatherAllRepoData(gopts, repo, repositoryData)
	Printf("Usage after all data gathered\n")
	PrintMemUsage()
	//ConfirmStdin()

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
	Printf("Usage after database Open\n")
	PrintMemUsage()

	// get the the highest id for each TABLE
	err = sqlite.Get_all_high_ids()
	if err != nil {
		Printf("db_add_record: Could not get Get_all_high_ids, error is %v\n", err)
		return err
	}
	//ConfirmStdin()

	// step 5 with multiple substeps: read the various tables:
	// the first three tables can go parallel
	wg, _ := errgroup.WithContext(gopts.ctx)
	wg.Go(func() error { return ReadSnapshotTable(db_conn, &db_aggregate) })
	wg.Go(func() error { return ReadNamesTable(db_conn, &db_aggregate) })
	wg.Go(func() error { return ReadPackfilesTable(db_conn, &db_aggregate) })
	res := wg.Wait()
	if res != nil {
		Printf("Error processing group 1. Error is %v\n", res)
		return res
	}
	// no parallelism ...
	err = ReadIndexRepoTable(db_conn, &db_aggregate)
	if err != nil {
		Printf("Error processing ReadIndexRepoTable. Error is %v\n", err)
		return err
	}

	// the next three tables can be read in parallel, since they dont depend on one another
	wg1, _ := errgroup.WithContext(gopts.ctx)
	wg1.Go(func() error { return ReadMetaDirTable(db_conn, &db_aggregate) })
	wg1.Go(func() error { return ReadIddFileTable(db_conn, &db_aggregate) })
	wg1.Go(func() error { return ReadContentsTable(db_conn, &db_aggregate) })
	res = wg1.Wait()
	if res != nil {
		Printf("Error processing group 2. Error is %v\n", res)
		return res
	}
	Printf("Usage after reading all database tables\n")
	PrintMemUsage()
  //ConfirmStdin()

	// the first three tables can be compared in parallel, since they dont depend on one another
	//Printf("Start first wg\n")
	wg, _ = errgroup.WithContext(gopts.ctx)
	wg.Go(func() error { return CompareSnapshots(&db_aggregate, repositoryData, newComers) })
	wg.Go(func() error { return CompareNames(&db_aggregate, repositoryData, newComers) })
	wg.Go(func() error { return ComparePackfiles(&db_aggregate, repositoryData, newComers) })
	res = wg.Wait()
	if res != nil {
		Printf("Error processing Compare group 1. Error is %v\n", res)
		return res
	}
	// sync
	CompareIndexRepo(&db_aggregate, repositoryData, newComers)
	Printf("after CompareIndexRepo\n")

	// compare the rest in paralel
	wg1, _ = errgroup.WithContext(gopts.ctx)
	wg1.Go(func() error { return CompareIddFile(&db_aggregate, repositoryData, newComers) })
	wg1.Go(func() error { return CompareMetaDir(&db_aggregate, repositoryData, newComers) })
	wg1.Go(func() error { return CompareContents(&db_aggregate, repositoryData, newComers) })
	res = wg1.Wait()
	if res != nil {
		Printf("Error processing Compare group 2. Error is %v\n", res)
		return res
	}
	Printf("Usage after comparing all database tables\n")
	PrintMemUsage()
	//ConfirmStdin()
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
	newComers *Newcomers, gopts GlobalOptions) error {
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
	//ConfirmStdin()
	changes_made := false

	// all INSERTs can be done in parallel
	Printf("Usage before INSERT\n")
	PrintMemUsage()

	wg, _ := errgroup.WithContext(gopts.ctx)
	wg.Go(func() error { return InsTab("snapshots", newComers.mem_snapshots, tx, column_names, &changes_made) })
	wg.Go(func() error { return InsTab("packfiles", newComers.mem_packfiles, tx, column_names, &changes_made) })
	wg.Go(func() error { return InsTab("index_repo", newComers.mem_index_repo, tx, column_names, &changes_made) })
	wg.Go(func() error { return InsTab("names", newComers.mem_names, tx, column_names, &changes_made) })
	wg.Go(func() error { return InsTab("meta_dir", newComers.mem_meta_dir, tx, column_names, &changes_made) })
	wg.Go(func() error { return InsTab("idd_file", newComers.mem_idd_file, tx, column_names, &changes_made) })
	wg.Go(func() error { return InsTab("contents", newComers.mem_contents, tx, column_names, &changes_made) })
	res := wg.Wait()
	if res != nil {
		Printf("Error processing INSERT INTO tables Error is %v\n", res)
		return res
	}
	Printf("Usage after INSERT\n")
	PrintMemUsage()
	//ConfirmStdin()

	// update timestamp
	if len(db_aggregate.Table_snapshots) > 0 {
		err = db_update_timestamp(db_aggregate.Table_snapshots, tx)
	} else {
		err = db_update_timestamp(newComers.mem_snapshots, tx)
	}
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
	Printf("Usage FINALE....\n")
	PrintMemUsage()
	return nil
}

func InsertTable[K comparable, V any](tbl_name string, mem_table map[K]V,
	tx *sqlx.Tx, column_names map[string][]string, changes_made *bool) error {
	if len(mem_table) == 0 {
		return nil
	}

	var m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m2)
	// build INSERT statement - bulk INSERT is the only SQL statement which
	// does not need a loop over all new rows
	column_list := column_names[tbl_name]
	value_list := make([]string, len(column_list))
	for ix, name := range column_list {
		value_list[ix] = ":" + name
	}

	t_insert := make([]V, len(mem_table))
	ix := 0
	for key, data := range mem_table {
		t_insert[ix] = data
		ix++
		if tbl_name != "snapshots" {
		  delete(mem_table, key)
		}
	}

	sql := fmt.Sprintf("INSERT INTO %s(%s) VALUES(%s)", tbl_name,
		strings.Join(column_list, ", "), strings.Join(value_list, ", "))
	if dbOptions.echo {
		Printf("%s\n", sql)
	}

	// do the INSERT
	// the splitting into segments could a bit more dynamic
	// its is limited by the product of number-of-columns * rows_inserted
	const OFFSET = 4000 // max 8 columns * 4000 = 32000 < 32k
	//Printf("INSERT %-15s #rows %6d #columns %2d\n", tbl_name, len(t_insert), len(value_list))
	for offset := 0; offset < len(t_insert); offset += OFFSET {
		max := len(t_insert)
		if max > OFFSET {
			max = OFFSET
		}
		if offset+max > len(t_insert) {
			max = len(t_insert) - offset
		}
		r, err := tx.NamedExec(sql, t_insert[offset:offset+max])
		if err != nil {
			Printf("error INSERT %s error is: %v\n", tbl_name, err)
			return err
		}
		count, _ := r.RowsAffected()
		if count > 0 {
			*changes_made = true
		}
	}
	Printf("%7d rows inserted into table %s\n", len(t_insert), tbl_name)
	var m3 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m3)
	// use reflect to calculate m3 - m2
	var m4 runtime.MemStats
	ref_m2 := reflect.ValueOf(&m2).Elem()
	ref_m3 := reflect.ValueOf(&m3).Elem()
	ref_m4 := reflect.ValueOf(&m4).Elem()
	for i := 0; i < ref_m3.NumField(); i++ {
		if ref_m3.Field(i).Kind() == reflect.Uint64 {
			diff := ref_m3.Field(i).Uint() - ref_m2.Field(i).Uint()
			if diff < 0 {
				diff = -diff
			}
			ref_m4.Field(i).SetUint(uint64(diff))
		}
	}

	Printf("Inserting table %-14s ", tbl_name)
	Printf("Alloc = %4d MiB", bToMb(m4 .Alloc))
	Printf("\tTotalAlloc = %4d MiB", bToMb(m4 .TotalAlloc))
	Printf("\tSys = %4d MiB", bToMb(m4 .Sys))
	Printf("\tHeap = %4d MiB\n", bToMb(m4 .HeapInuse))
	return nil
}
// this is generic comparision function
type MemBuildFunc func(*DBAggregate, *RepositoryData, *Newcomers)

// generic function using 'reflect' to access the status field
// for removal of all rows which do not have a status of xxx
func filter_new[K comparable, V any](mem_map map[K]V, status string) {
	for key, data := range mem_map {
		v := reflect.ValueOf(data).Elem()
		if v.FieldByName("Status").String() != status {
			delete(mem_map, key)
		}
	}
}

// generic funtion to INSERT new data into the database
func InsTab[K comparable, V any](table_name string, mem_map map[K]V,
	tx *sqlx.Tx, column_names map[string][]string, changes_made *bool) error {
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
	if newComers.mem_snapshots == nil {
		Printf("Alarm CreateMemSnapshots\n")
		return nil
	}
	newComers.new_snapshots = NewMemoryKeys(db_aggregate.Table_snapshots, newComers.mem_snapshots)
	high_snap := sqlite.Get_high_id("snapshots")
	for snap_id := range newComers.new_snapshots.Iter() {

		row := newComers.mem_snapshots[snap_id]
		row.Status = "new"
		row.Id = high_snap
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
	newComers.new_names = NewMemoryKeys(db_aggregate.Table_names, newComers.mem_names)
	Printf("%7d new records in new_names\n", newComers.new_names.Cardinality())

	high_names := sqlite.Get_high_id("names")
	for name := range newComers.new_names.Iter() {

		row := newComers.mem_names[name]
		row.Status = "new"
		row.Id = high_names
		newComers.mem_names[name] = row

		high_names++
	}
	return nil
}

func ComparePackfiles(db_aggregate *DBAggregate, repositoryData *RepositoryData,
	newComers *Newcomers) error {
	// compare packfiles with its DB counterpart
	newComers.mem_packfiles = CreateMemPackfiles(db_aggregate, repositoryData, newComers)
	newComers.new_packfiles = NewMemoryKeys(db_aggregate.Table_packfiles, newComers.mem_packfiles)
	Printf("%7d new records in new_packfiles\n", newComers.new_packfiles.Cardinality())

	high_pack := sqlite.Get_high_id("packfiles")
	for pack_ID_ptr := range newComers.new_packfiles.Iter() {

		row := newComers.mem_packfiles[pack_ID_ptr]
		row.Status = "new"
		row.Id = high_pack
		newComers.mem_packfiles[pack_ID_ptr] = row

		high_pack++
	}
	return nil
}

func CompareIndexRepo(db_aggregate *DBAggregate, repositoryData *RepositoryData,
	newComers *Newcomers) error {
	// compare index_repo with its DB counterpart
	newComers.mem_index_repo = CreateMemIndexRepo(db_aggregate, repositoryData, newComers)
	if newComers.mem_index_repo == nil {
		Printf("Alarm CompareIndexRepo\n")
		return nil
	}
	newComers.new_index_repo = NewMemoryKeys(db_aggregate.Table_index_repo, newComers.mem_index_repo)
	Printf("%7d new records in new_index_repo\n", newComers.new_index_repo.Cardinality())

	high_repo := sqlite.Get_high_id("index_repo")
	for blob := range newComers.new_index_repo.Iter() {

		row := newComers.mem_index_repo[blob]
		row.Status = "new"
		row.Id = high_repo
		newComers.mem_index_repo[blob] = row

		high_repo++
	}
	return nil
}

func CompareIddFile(db_aggregate *DBAggregate, repositoryData *RepositoryData,
	newComers *Newcomers) error {
	// compare idd_file with its DB counterpart
	newComers.mem_idd_file = CreateMemIddFile(db_aggregate, repositoryData, newComers)
	newComers.new_idd_file = NewMemoryKeys(db_aggregate.Table_idd_file, newComers.mem_idd_file)
	Printf("%7d new records in new_idd_file\n", newComers.new_idd_file.Cardinality())

	high_idd := sqlite.Get_high_id("idd_file")
	for comp_ix := range newComers.new_idd_file.Iter() {
		meta_blob := comp_ix.meta_blob

		row := newComers.mem_idd_file[comp_ix]
		row.Status = "new"
		row.Id = high_idd
		row.Id_blob = newComers.mem_index_repo[repositoryData.index_to_blob[meta_blob]].Id // extra
		newComers.mem_idd_file[comp_ix] = row

		high_idd++

		// consistency check
		if row.Id_blob == 0 {
			Printf("Logic error for blob pointer %6d in idd_file\n",
				meta_blob)
			panic("CompareIddFile:Logic error -- new idd_file 1")
		}
	}
	return nil
}

func CompareMetaDir(db_aggregate *DBAggregate, repositoryData *RepositoryData,
	newComers *Newcomers) error {
	// compare meta_dir with its DB counterpart
	newComers.mem_meta_dir = CreateMemMetaDir(db_aggregate, repositoryData, newComers)
	newComers.new_meta_dir = NewMemoryKeys(db_aggregate.Table_meta_dir, newComers.mem_meta_dir)
	Printf("%7d new records in new_meta_dir\n", newComers.new_meta_dir.Cardinality())

	high_mdir := sqlite.Get_high_id("meta_dir")
	for comp_ix := range newComers.new_meta_dir.Iter() {
		snap_id := comp_ix.snap_id
		meta_blob := comp_ix.meta_blob

		row := newComers.mem_meta_dir[comp_ix]
		row.Status = "new"
		row.Id = high_mdir
		newComers.mem_meta_dir[comp_ix] = row

		high_mdir++

		//  consistency check
		if row.Id_snap_id == 0 {
			Printf("Logic error for meta_dir.snap_idd %s %#v\n",
				snap_id, newComers.mem_snapshots[snap_id])
			panic("CompareMetaDir:Logic error -- meta_dir 1")
		}
		if row.Id_idd == 0 {
			Printf("Logic error for meta_dir.blob %s %#v\n",
				snap_id, newComers.mem_index_repo[repositoryData.index_to_blob[meta_blob]])
			panic("CompareMetaDir:Logic error -- meta_dir 2")
		}
	}
	return nil
}

func CompareContents(db_aggregate *DBAggregate, repositoryData *RepositoryData,
	newComers *Newcomers) error {
	// compare contents with its DB counterpart
	newComers.mem_contents = CreateMemContents(db_aggregate, repositoryData, newComers)
	newComers.new_contents = NewMemoryKeys(db_aggregate.Table_contents, newComers.mem_contents)
	Printf("%7d new records in new_contents\n", newComers.new_contents.Cardinality())

	high_cont := sqlite.Get_high_id("contents")
	for comp_ix := range newComers.new_contents.Iter() {
		p_meta_blob := comp_ix.meta_blob

		row := newComers.mem_contents[comp_ix]
		row.Status = "new"
		row.Id = high_cont
		newComers.mem_contents[comp_ix] = row

		high_cont++

		// check consistency
		if row.Id_blob == 0 {
			Printf("Logic error for meta_dir.blob %6d %#v\n",
				p_meta_blob, newComers.mem_index_repo[repositoryData.index_to_blob[p_meta_blob]])
			panic("CompareContents:Logic error -- contents 1")
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
}

// manage timestamp table (with one row)
func db_update_timestamp(Table_snapshots map[string]*SnapshotRecordMem, tx *sqlx.Tx) error {
	max_time := ""
	for _, data := range Table_snapshots {
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
		// there is no row in timestamp, create one
		now := time.Now()
		sql2 := `INSERT INTO timestamp(id,restic_updated,database_updated,ts_created)
							VALUES(:id,:restic_updated,:database_updated,:ts_created)`
		_, err := tx.Exec(sql2, 1, now, now, now)
		if err != nil {
			fmt.Printf("Can't INSERT INTO timestamp. Error is %v\n", err)
		}
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
