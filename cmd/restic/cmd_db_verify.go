package main

// compile with "go run build.go -tags debug""
// run with DEBUG_LOG=/home/wplapper/restic/debug.log fully-qualified-name/restic -r <repo> overview

// ref to onedrive: rclone:onedrive:restic_backups

import (
	// system
	"sort"
	"time"
	"golang.org/x/sync/errgroup"
	"reflect"
	//"regexp"

	//argparse
	"github.com/spf13/cobra"

	// sets
	//"github.com/deckarep/golang-set"
	"github.com/wplapper/restic/library/mapset"

	// sqlx for sqlite3
	//"github.com/jmoiron/sqlx"

	// restic library
	"github.com/wplapper/restic/library/restic"
	"github.com/wplapper/restic/library/sqlite"
)

// system wirde variables
var db_aggregate 	 DBAggregate
var dbOptions 		 DBOptions
var newComers 		 *Newcomers
var repositoryData *RepositoryData

// map repo to database - really a const, but not according to the o gospel
var DATABASE_NAMES = map[string]string{
	// master
	"/media/mount-points/Backup-ext4-Mate/restic_master":  "/media/mount-points/home/wplapper/restic/db/restic-master_nfs.db",
	"/media/mount-points/Backup-ext4-Mate/restic_master/": "/media/mount-points/home/wplapper/restic/db/restic-master_nfs.db",

	// onedrive
	"rclone:onedrive:restic_backups":
	"/media/mount-points/home/wplapper/restic/db/restic-onedrive.db",

	// most
	"/media/mount-points/Backup-ext4-Mate/restic_most":  "/media/mount-points/home/wplapper/restic/db/restic-most_nfs.db",
	"/media/mount-points/Backup-ext4-Mate/restic_most/": "/media/mount-points/home/wplapper/restic/db/restic-most_nfs.db",

	// data
	"/media/wplapper/internal-fast/restic_Data":  "/home/wplapper/restic/db/XPS-restic-data_nfs.db",
	"/media/wplapper/internal-fast/restic_Data/": "/home/wplapper/restic/db/XPS-restic-data_nfs.db",

	// test
	"/media/wplapper/internal-fast/restic_test":  "/home/wplapper/restic/db/XPS-restic-test.db",
	"/media/wplapper/internal-fast/restic_test/": "/home/wplapper/restic/db/XPS-restic-test.db"}

var cmdDBVerify = &cobra.Command{
	Use:   "db_verify [flags]",
	Short: "verify SQLite database with repository",
	Long:  `verify SQLite database with repository.

EXIT STATUS
===========

Exit Status is 0 if the command was successful, and non-zero if there was any error.
`,
	DisableAutoGenTag: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runDBVerify(globalOptions, args)
	},
}

func init() {
	cmdRoot.AddCommand(cmdDBVerify)
	flags := cmdDBVerify.Flags()
	flags.BoolVarP(&dbOptions.echo, "echo", "E", false, "echo database operations to stdout")
	flags.StringVarP(&dbOptions.altDB, "DB", "", "", "aternative database name")
}

func runDBVerify(gopts GlobalOptions, args []string) error {

	// step 0: setup global stuff
	start := time.Now()
	_ = start
	repositoryData = init_repositoryData() // is a *RepositoryData
	db_aggregate.repositoryData = repositoryData
	EMPTY_NODE_ID = restic.Hash([]byte("{\"nodes\":[]}\n"))
	newComers = InitNewcomers()
	gOptions = gopts
	var db_name string
	var ok bool

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
	if dbOptions.altDB != "" {
		db_name = dbOptions.altDB
	} else {
		db_name, ok = DATABASE_NAMES[gopts.Repo]
		if !ok {
			Printf("database name for repo %s is missing!\n", gopts.Repo)
			return nil
		}
	}

	// step 4.2: open selected database -- XXX more generic verbose & echo options
	db_conn, err := sqlite.OpenDatabase(db_name, true, 1, true)
	if err != nil {
		Printf("db_verify: OpenDatabase failed, error is %v\n", err)
		return err
	}
	db_aggregate.db_conn = db_conn

	// gather counts for all tables in database
	names_and_counts := make(map[string]int)
	err = readAllTablesAndCounts(db_conn, names_and_counts)
	if err != nil {
		Printf("readAllTablesAndCounts error is %v\n", err)
		return err
	}
	db_aggregate.table_counts = names_and_counts

	// sort names
	tbl_names_sorted := make([]string, len(names_and_counts))
	ix := 0
	for tbl_name := range names_and_counts {
		tbl_names_sorted[ix] = tbl_name
		ix++
	}
	sort.Strings(tbl_names_sorted)
	// print table counts

	for _, tbl_name := range tbl_names_sorted {
		count := names_and_counts[tbl_name]
		Printf("%-25s %8d\n", tbl_name, count)
	}

	// the first three tables can go parallel
	wg, _ := errgroup.WithContext(gopts.ctx)
	wg.Go (func() error {return ReadSnapshotTable (db_conn, &db_aggregate)})
	wg.Go (func() error {return ReadNamesTable    (db_conn, &db_aggregate)})
	wg.Go (func() error {return ReadPackfilesTable(db_conn, &db_aggregate)})
	res := wg.Wait()
	if res != nil {
		Printf("Esrror processing group 1. Error is %v\n", res)
		return res
	}

	err = ReadIndexRepoTable(db_conn, &db_aggregate)
	if err != nil {
		Printf("Error processing ReadIndexRepoTable. Error is %v\n", err)
		return err
	}

	// the last three tables can be read in parallel, since they dont depend on one another
	wg1, _ := errgroup.WithContext(gopts.ctx)
	wg1.Go (func() error {return ReadMetaDirTable (db_conn, &db_aggregate)})
	wg1.Go (func() error {return ReadIddFileTable (db_conn, &db_aggregate)})
	wg1.Go (func() error {return ReadContentsTable(db_conn, &db_aggregate)})
	res = wg1.Wait()
	if res != nil {
		Printf("Error processing group 2. Error is %v\n", res)
		return res
	}

	wg, _ = errgroup.WithContext(gopts.ctx)
	//_ = wg
	var r1 bool
	var r2 bool
	var r3 bool
	var r4 bool
	var r5 bool
	var r6 bool
	var r7 bool
	wg, _ = errgroup.WithContext(gopts.ctx)
	wg.Go (func() error {return GenericCompare("snapshots", &db_aggregate, check_db_snapshots, repositoryData, newComers, &r1)})
	wg.Go (func() error {return GenericCompare("index_repo", &db_aggregate, check_db_index_repo, repositoryData, newComers, &r2)})
	wg.Go (func() error {return GenericCompare("packfiles", &db_aggregate, check_db_packfiles, repositoryData, newComers, &r3)})
	wg.Go (func() error {return GenericCompare("contents", &db_aggregate, check_db_contents, repositoryData, newComers, &r4)})
	wg.Go (func() error {return GenericCompare("meta_dir", &db_aggregate, check_db_meta_dir, repositoryData, newComers, &r5)})
	wg.Go (func() error {return GenericCompare("names", &db_aggregate, check_db_names, repositoryData, newComers, &r6)})
	wg.Go (func() error {return GenericCompare("idd_file", &db_aggregate, check_db_idd_file, repositoryData, newComers, &r7)})
	res = wg.Wait()
	if res != nil {
		Printf("Error processing group 1. Error is %v\n", res)
		return res
	}

	// collect summary
	var flags = []bool{r1, r2, r3, r4, r5, r6, r7}
	total := true
	for ix, which := range flags {
		if !which {
			Printf("check %d failed!\n", ix)
			total = false
		}
	}
	if total {
		Printf("*** all tables compare equal ***\n")
	} else {
		Printf("*** some tables fail to compare! ***\n")
	}
	// check foreign key relationship
  CheckForeignKeys(&db_aggregate, repositoryData)
	return nil
}

func check_db_snapshots(db_aggregate *DBAggregate, repositoryData *RepositoryData, newComers *Newcomers) bool {
	// compare snapshots from repo with snapshots stored in the database
	// step 1: build memory table to allow the comparison
	mem_snapshots := CreateMemSnapshots(db_aggregate, repositoryData, newComers)

	// compare snapshot keys
	equal := CompareKeys("snapshots", db_aggregate.Table_snapshots, mem_snapshots)
	if !equal {
		set_db_keys  := mapset.NewSet[string]()
		set_mem_keys := mapset.NewSet[string]()
		for key := range db_aggregate.Table_snapshots {
			set_db_keys.Add(key)
		}
		for key := range mem_snapshots {
			set_mem_keys.Add(key)
		}

		len_db  := set_db_keys.Cardinality()
		len_mem := set_mem_keys.Cardinality()

		var diff mapset.Set[string]
		var which string
		if len_db > len_mem {
			diff = set_db_keys.Difference(set_mem_keys)
			which = "mem"
		} else {
			diff = set_mem_keys.Difference(set_db_keys)
			which = "db "
		}

		count := 0
		for comp_ix := range diff.Iter() {
			Printf("check_db_snapshots key %s %s\n", which, comp_ix)
			count++
			if count > 20 {
				break
			}
		}
		return equal
	}

	// compare snapshot values
	compare_equals := true
	for db_key, db_value := range db_aggregate.Table_snapshots {
		mem_value := mem_snapshots[db_key]
		if db_value.Snap_host != mem_value.Snap_host || db_value.Snap_time != mem_value.Snap_time {
			Printf("db  %s %s %s %s\n", db_value.Snap_time, db_value.Snap_host, db_value.Snap_fsys, db_value.root)
			Printf("mem %s %s %s %s\n", mem_value.Snap_time, mem_value.Snap_host, mem_value.Snap_fsys, mem_value.root)
			compare_equals = false
		}
	}
	return compare_equals
}

func check_db_index_repo(db_aggregate *DBAggregate, repositoryData *RepositoryData,
newComers *Newcomers) bool {

	mem_repo_index_map := CreateMemIndexRepo(db_aggregate, repositoryData, newComers)
	if mem_repo_index_map == nil {
		Printf("Cannot create CreateMemIndexRepo\n")
		return false
	}
	equal := CompareKeys("index_repo", db_aggregate.Table_index_repo, mem_repo_index_map)
	if !equal {
		return equal
	}

	// compare table values
	compare_equals := true
	for db_key, db_value := range db_aggregate.Table_index_repo {
		mem_value := mem_repo_index_map[db_key]
		if db_value.Idd_size != mem_value.Idd_size || db_value.Index_type != mem_value.Index_type {
			Printf("v db  %7d\n", db_value.Idd_size)
			Printf("v mem %7d\n", mem_value.Idd_size)
			compare_equals = false
		}
	}
	return compare_equals
}

func check_db_names(db_aggregate *DBAggregate, repositoryData *RepositoryData,
newComers *Newcomers) bool {
	// build a memory map of the names, whic come from three(3) different sources
	table_name := "names"
	mem_names_map := CreateMemNames(db_aggregate, repositoryData, newComers)
	equal := CompareKeys(table_name, db_aggregate.Table_names, mem_names_map)
	if equal {
		return equal
	}

	set_db_keys  := mapset.NewSet[string]()
	set_mem_keys := mapset.NewSet[string]()
	for key := range db_aggregate.Table_names {
		set_db_keys.Add(key)
	}
	for key := range mem_names_map {
		set_mem_keys.Add(key)
	}

	len_db  := set_db_keys.Cardinality()
	len_mem := set_mem_keys.Cardinality()

	var diff mapset.Set[string]
	var which string
	if len_db > len_mem {
		diff = set_db_keys.Difference(set_mem_keys)
		which = "mem"
	} else {
		diff = set_mem_keys.Difference(set_db_keys)
		which = "db "
	}

	count := 0
	for comp_ix := range diff.Iter() {
		Printf("%s %s\n", which, comp_ix)
		count++
		if count > 20 {
			break
		}
	}
	return equal
}

func check_db_packfiles(db_aggregate *DBAggregate, repositoryData *RepositoryData,
newComers *Newcomers) bool {
	//table_name := "packfiles"
	// build a memory map of the packfiles
	mem_packfiles_map := CreateMemPackfiles(db_aggregate, repositoryData, newComers)
	if mem_packfiles_map == nil {
		return false
	}
	// compare keys
	return CompareKeys("packfiles", db_aggregate.Table_packfiles, mem_packfiles_map)
}

// check_db_contents
func check_db_contents(db_aggregate *DBAggregate, repositoryData *RepositoryData,
newComers *Newcomers) bool {
	//table_name := "contents"
	// build a memory map of the contents
	mem_contents_map := CreateMemContents(db_aggregate, repositoryData, newComers)
	if mem_contents_map == nil {
		return false
	}

	// compare keys
	equal := CompareKeys("contents", db_aggregate.Table_contents, mem_contents_map)
	if !equal {
		Printf("**** Key mismatch for contents ***\n")
		set_db_keys  := mapset.NewSet[CompContents]()
		set_mem_keys := mapset.NewSet[CompContents]()
		for key := range db_aggregate.Table_contents {
			set_db_keys.Add(key)
		}
		for key := range db_aggregate.Table_contents {
			set_mem_keys.Add(key)
		}
		diff := set_db_keys.Difference(set_mem_keys)
		Printf("missing from memory %d keys\n", diff.Cardinality())
		count := 0
		for comp_ix := range diff.Iter() {
			meta_blob := comp_ix.meta_blob
			Printf("missing %s %3d %3d\n", meta_blob.String()[:12], comp_ix.position,
				comp_ix.offset)
			count++
			if count > 20 {
				break
			}
		}
		return equal
	}

	// loop over data_blobs
	equal = true
	count := 0
	for db_key, db_value := range db_aggregate.Table_contents {
		mem_value := mem_contents_map[db_key]
		if db_value.id_data_idd != mem_value.id_data_idd {
			equal = false
			count++
		}
	}
	if count == 0 {
		return equal
	}

	count = 0
	for db_key, db_value := range db_aggregate.Table_contents {
		mem_value := mem_contents_map[db_key]
		if db_value.id_data_idd != mem_value.id_data_idd {
			Printf("key db  %s %3d %3d\n", db_key.meta_blob.String()[:12], db_key.position,
				db_key.offset)
			Printf("  db  %v\n", db_value)
			Printf("  mem %v\n", mem_value)
			count++
			if count > 20 {
				break
			}
		}
	}
	return equal
}

func check_db_meta_dir(db_aggregate *DBAggregate, repositoryData *RepositoryData,
newComers *Newcomers) bool {
	mem_meta_dir_map := CreateMemMetaDir(db_aggregate, repositoryData, newComers)
	if mem_meta_dir_map == nil {
		return false
	}

	// compare keys
	equal := CompareKeys("meta_dir", db_aggregate.Table_meta_dir, mem_meta_dir_map)
	if !equal {
		set_db_keys  := mapset.NewSet[CompMetaDir]()
		set_mem_keys := mapset.NewSet[CompMetaDir]()
		for key := range db_aggregate.Table_meta_dir {
			set_db_keys.Add(key)
		}
		for key := range mem_meta_dir_map {
			set_mem_keys.Add(key)
		}

		len_db  := set_db_keys.Cardinality()
		len_mem := set_mem_keys.Cardinality()

		var diff mapset.Set[CompMetaDir]
		var which string
		if len_db > len_mem {
			diff = set_db_keys.Difference(set_mem_keys)
			which = "mem"
		} else {
			diff = set_mem_keys.Difference(set_db_keys)
			which = "db "
		}

		count := 0
		count_empty_node := 0
		for comp_ix := range diff.Iter() {
			if comp_ix.meta_blob == EMPTY_NODE_ID {
				count_empty_node++
				continue
			}
			Printf("%s %s %s\n", which, comp_ix.snap_id, comp_ix.meta_blob)
			count++
			if count > 10 {
				break
			}
		}
		if count_empty_node == diff.Cardinality() {
			equal = true
		}
	}

	if !equal {
		Printf("mismatch keys for table %s\n", "meta_dir")
	}
	return equal
}

func check_db_idd_file(db_aggregate *DBAggregate, repositoryData *RepositoryData,
newComers *Newcomers) bool {
	table_name := "idd_file"
	// build a memory map of the packfiles
	mem_idd_file_map := CreateMemIddFile(db_aggregate, repositoryData, newComers)
	if mem_idd_file_map == nil {
		return false
	}
	// compare keys
	equal := CompareKeys(table_name, db_aggregate.Table_idd_file, mem_idd_file_map)
	if !equal {
		return equal
	}

	// check contents of idd_file
	count_print := 0
	for db_key, db_value := range db_aggregate.Table_idd_file {
		mem_value := mem_idd_file_map[db_key]
		if mem_value.Inode != db_value.Inode || mem_value.Size != db_value.Size ||
			mem_value.Mtime  != db_value.Mtime || mem_value.Type != db_value.Type {
			equal = false
			Printf("key %s.%3d\n", db_key.meta_blob.String()[:12], db_key.position)
			Printf("  db   %8d %7d %s %-4s\n", db_value.Inode, db_value.Size,
				db_value.Mtime, db_value.Type)
			Printf("  mem  %8d %7d %s %-4s %s\n", mem_value.Inode, mem_value.Size,
				mem_value.Mtime, mem_value.Type, mem_value.name)
			count_print++
			if count_print > 20 {
				break
			}
		}
	}
	return equal
}

func CheckForeignKeys(db_aggregate *DBAggregate, repositoryData *RepositoryData) bool {
	type ForeignKeys struct {
		check_table  	string
		column_name 	string
		group_number 	int
		ref_table     string
		db_table      interface{}
		db_ref_table  interface{}
	}
	var check_tables = []ForeignKeys{
		{"meta_dir",   "Id_snap_id", 2, "snapshots",  MetaDirRecordMem{},   SnapshotRecordMem{}},
		{"meta_dir",   "Id_idd",     3, "index_repo", MetaDirRecordMem{},   IndexRepoRecordMem{}},
		{"idd_file",   "Id_blob",    3, "index_repo", IddFileRecordMem{},   IndexRepoRecordMem{}},
		{"contents",   "Id_data_idd",3, "index_repo", ContentsRecordMem{},  IndexRepoRecordMem{}},
		{"contents",   "Id_blob",    3, "index_repo", ContentsRecordMem{},  IndexRepoRecordMem{}},
		{"idd_file",   "Id_name",    4, "names",      IddFileRecordMem{},   NamesRecordMem{}},
		{"index_repo", "Id_pack_id", 5, "packfiles",  IndexRepoRecordMem{}, PackfilesRecordMem{}}}

	Printf("\n*** Check Foreign Key relationship ***\n")
	dyna_table := reflect.ValueOf(db_aggregate).Elem()
	previous_group := 0
	ref_table_keys1 := mapset.NewSet[int64]()
	all_good := true

	// loop over all ForeignKeys relationships
	for _, entry := range check_tables {
		table_good := true
		Printf("checking %s.%s vs %s.id\n", entry.check_table, entry.column_name, entry.ref_table)
		current_group := entry.group_number
		if current_group != previous_group {
			ref_table_keys1 = BuildReferenceSet(db_aggregate, entry.ref_table, entry.db_ref_table)
			previous_group = current_group
		}

		// build reflect iterator for the check_table
		iter := dyna_table.FieldByName("Table_" + entry.check_table).MapRange()

		// regex to extract key and value Type
		//re := regexp.MustCompile(`^map\[([A-Za-z0-9_.]+)\]([A-Za-z0-9_.]+)$`)
		//res := re.FindStringSubmatch(type_string)
		//Printf("result of regex key='%s' values='%s'\n", res[1], res[2])

		print_count := 0
		var data interface{}
		for iter.Next() {
			raw_data := iter.Value()
			print_count++

			switch typ := entry.db_table.(type) {
			case IndexRepoRecordMem:
				data = raw_data.Interface().(IndexRepoRecordMem)
			case MetaDirRecordMem:
				data = raw_data.Interface().(MetaDirRecordMem)
			case IddFileRecordMem:
				data = raw_data.Interface().(IddFileRecordMem)
			case ContentsRecordMem:
				data = raw_data.Interface().(ContentsRecordMem)
			default:
				Printf("Wrong Type %v\n", typ)
				panic("Wrong Type -- loop 2")
			}

			// extract id-data from <check_table>.<column_name>, convert to int64
			to_be_checked := reflect.ValueOf(data).FieldByName(entry.column_name).Int()
			if ! ref_table_keys1.Contains(to_be_checked) {
				table_good = false
				all_good   = false
				if print_count < 10 {
					Printf("%s.%s[%v] not in %s.id\n", entry.check_table, entry.column_name,
						to_be_checked, entry.ref_table)
				}
			}
		}
		Printf("foreign key constraints for %-15s %v\n", entry.check_table, table_good)
	}
	ref_table_keys1.Clear()
	return all_good
}

// utility function - compare_keys
// compare the keys of the equvalent memory and database table,
// test of equalness and return result
func CompareKeys[K comparable, V1 any, V2 any](table_name string, db map[K]V1, mem map[K]V2) bool {
	// define sets for the keys
	set_db_keys  := mapset.NewSet[K]()
	set_mem_keys := mapset.NewSet[K]()
	for key := range db {
		set_db_keys.Add(key)
	}
	for key := range mem {
		set_mem_keys.Add(key)
	}
	return set_mem_keys.Equal(set_db_keys)
}

// compare the keys of the equvalent memory and database table,
// return the difference between memory and database
func NewMemoryKeys[K comparable, V1 any, V2 any](db map[K]V1, mem map[K]V2) mapset.Set[K] {
	// define sets for the keys
	set_db_keys  := mapset.NewSet[K]()
	set_mem_keys := mapset.NewSet[K]()
	for key := range db {
		set_db_keys.Add(key)
	}
	for key := range mem {
		set_mem_keys.Add(key)
	}
	return set_mem_keys.Difference(set_db_keys)
}

// compare the keys of the equvalent memory and database table,
// return the difference between database and memory
func OldDBKeys[K comparable, V1 any, V2 any](db map[K]V1, mem map[K]V2) mapset.Set[K] {
	// define sets for the keys
	set_db_keys  := mapset.NewSet[K]()
	set_mem_keys := mapset.NewSet[K]()
	for key := range db {
		set_db_keys.Add(key)
	}
	for key := range mem {
		set_mem_keys.Add(key)
	}
	return set_db_keys.Difference(set_mem_keys)
}

// call generic comparator
type ComparatorFunc func(*DBAggregate, *RepositoryData, *Newcomers) bool
func GenericCompare(table_name string, db_aggregate *DBAggregate, comparator ComparatorFunc,
repositoryData *RepositoryData, newComers *Newcomers, return_value *bool) error {
	Printf("checking table %s\n", table_name)
	*return_value = comparator(db_aggregate, repositoryData, newComers)
	return nil
}

// build mapset.Set based on table_name, use Id as field reference
func BuildReferenceSet(db_aggregate *DBAggregate, table_name string,
db_ref_table  interface{}) mapset.Set[int64] {
	// reflect on db_aggregate.Table_<table_name>
	dyna_table := reflect.ValueOf(db_aggregate).Elem()
	ref_table  := dyna_table.FieldByName("Table_" + table_name)
	iter       := ref_table.MapRange()
	reference_set := mapset.NewSet[int64]()
	for iter.Next() {
		raw_data := iter.Value()
		switch typ := db_ref_table.(type) {
		case IndexRepoRecordMem:
			data := raw_data.Interface().(IndexRepoRecordMem)
			reference_set.Add(int64(data.Id))
		case MetaDirRecordMem:
			data := raw_data.Interface().(MetaDirRecordMem)
			reference_set.Add(int64(data.Id))
		case IddFileRecordMem:
			data := raw_data.Interface().(IddFileRecordMem)
			reference_set.Add(int64(data.Id))
		case ContentsRecordMem:
			data := raw_data.Interface().(ContentsRecordMem)
			reference_set.Add(int64(data.Id))
		case SnapshotRecordMem:
			data := raw_data.Interface().(SnapshotRecordMem)
			reference_set.Add(int64(data.Id))
		case NamesRecordMem:
			data := raw_data.Interface().(NamesRecordMem)
			reference_set.Add(int64(data.Id))
		case PackfilesRecordMem:
			data := raw_data.Interface().(PackfilesRecordMem)
			reference_set.Add(int64(data.Id))
		default:
			Printf("Wrong Type %+v\n", typ)
			panic("Wrong Type -- loop 1")
		}
	}
	return reference_set
}
