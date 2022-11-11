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

	//argparse
	"github.com/spf13/cobra"

	// sets
	//"github.com/deckarep/golang-set"
	"github.com/wplapper/restic/library/mapset"

	// sqlx for sqlite3
	"github.com/jmoiron/sqlx"

	// restic library
	"github.com/wplapper/restic/library/restic"
	"github.com/wplapper/restic/library/sqlite"
)

// system wirde variables
var db_aggregate 	 DBAggregate
var dbOptions 		 DBOptions
var newComers 		 *Newcomers
var repositoryData *RepositoryData

// map repo to database - really a const, but not accordin to the o gospel
var DATABASE_NAMES = map[string]string{
	// master
	"/media/mount-points/Backup-ext4-Mate/restic_master":  "/media/mount-points/home/wplapper/restic/db/restic-master_nfs.db",
	"/media/mount-points/Backup-ext4-Mate/restic_master/": "/media/mount-points/home/wplapper/restic/db/restic-master_nfs.db",

	// onedrive
	"rclone:onedrive:restic_backups":
	"/media/mount-points/home/wplapper/restic/db/restic-onedrive.db",
	////"/home/wplapper/restic/db/restic-onedrive.db",

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
	Long: `gverify SQLite database with repository.

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

	type action_function func(*sqlx.DB, *DBAggregate) error
	type ActionStruct struct {
		table_name string
		routine    action_function
	}
	//var table_name string
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
	wg, _ := errgroup.WithContext(gopts.ctx)
	_ = wg
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
	res := wg.Wait()
	if res != nil {
		Printf("Error processing group 1. Error is %v\n", res)
		return res
	}
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
	equal := CompareKeys("snapshots", db_aggregate.table_snapshots, mem_snapshots)
	if !equal {
		set_db_keys  := mapset.NewSet[string]()
		set_mem_keys := mapset.NewSet[string]()
		for key := range db_aggregate.table_snapshots {
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
			Printf("key %s %s\n", which, comp_ix)
			count++
			if count > 20 {
				break
			}
		}
		return equal
	}

	// compare snapshot values
	compare_equals := true
	for db_key, db_value := range db_aggregate.table_snapshots {
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
	equal := CompareKeys("index_repo", db_aggregate.table_index_repo, mem_repo_index_map)
	if !equal {
		return equal
	}

	// compare table values
	compare_equals := true
	for db_key, db_value := range db_aggregate.table_index_repo {
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
	equal := CompareKeys(table_name, db_aggregate.table_names, mem_names_map)
	if equal {
		return equal
	}

	set_db_keys  := mapset.NewSet[string]()
	set_mem_keys := mapset.NewSet[string]()
	for key := range db_aggregate.table_names {
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
		if count > 100 {
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

	// compare keys
	return CompareKeys("packfiles", db_aggregate.table_packfiles, mem_packfiles_map)
}

// check_db_contents
func check_db_contents(db_aggregate *DBAggregate, repositoryData *RepositoryData,
newComers *Newcomers) bool {
	//table_name := "contents"
	// build a memory map of the contents
	mem_contents_map := CreateMemContents(db_aggregate, repositoryData, newComers)

	// compare keys
	equal := CompareKeys("contents", db_aggregate.table_contents, mem_contents_map)
	if !equal {
		Printf("**** Key mismatch for contents ***\n")
		set_db_keys  := mapset.NewSet[CompContents]()
		set_mem_keys := mapset.NewSet[CompContents]()
		for key := range db_aggregate.table_contents {
			set_db_keys.Add(key)
		}
		for key := range db_aggregate.table_contents {
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
			if count > 100 {
				break
			}
		}
		return equal
	}

	// loop over data_blobs
	equal = true
	count := 0
	for db_key, db_value := range db_aggregate.table_contents {
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
	for db_key, db_value := range db_aggregate.table_contents {
		mem_value := mem_contents_map[db_key]
		if db_value.id_data_idd != mem_value.id_data_idd {
			Printf("key db  %s %3d %3d\n", db_key.meta_blob.String()[:12], db_key.position,
				db_key.offset)
			Printf("  db  %v\n", db_value)
			Printf("  mem %v\n", mem_value)
			count++
			if count > 100 {
				break
			}
		}
	}
	return equal
}

func check_db_meta_dir(db_aggregate *DBAggregate, repositoryData *RepositoryData,
newComers *Newcomers) bool {
	mem_meta_dir_map := CreateMemMetaDir(db_aggregate, repositoryData, newComers)

	// compare keys
	equal := CompareKeys("meta_dir", db_aggregate.table_meta_dir, mem_meta_dir_map)
	if !equal {
		set_db_keys  := mapset.NewSet[CompMetaDir]()
		set_mem_keys := mapset.NewSet[CompMetaDir]()
		for key := range db_aggregate.table_meta_dir {
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
			if count > 100 {
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

	// compare keys
	equal := CompareKeys(table_name, db_aggregate.table_idd_file, mem_idd_file_map)
	if !equal {
		return equal
	}

	// check contents of idd_file
	for db_key, db_value := range db_aggregate.table_idd_file {
		mem_value := mem_idd_file_map[db_key]
		if mem_value.Inode != db_value.Inode || mem_value.Size != db_value.Size ||
			mem_value.Mtime != db_value.Mtime || mem_value.Type != db_value.Type {
			equal = false
			Printf("key %s.%3d\n", db_key.meta_blob.String()[:12], db_key.position)
			Printf("  db   %8d %7d %s %-4s\n", db_value.Inode, db_value.Size,
				db_value.Mtime, db_value.Type)
			Printf("  mem  %8d %7d %s %-4s %s\n", mem_value.Inode, mem_value.Size,
				mem_value.Mtime, mem_value.Type, mem_value.name)
		}
	}
	return equal
}

func CheckForeignKeys(db_aggregate *DBAggregate, repositoryData *RepositoryData) bool {
	type ForeignKeys struct {
		table_name  	string
		column_name 	string
		group_number 	int
		ref_table     string
	}
	var check_tables = []ForeignKeys{
		{"index_repo", 	"Id_pack_id", 1, "packfiles"},
		{"meta_dir", 		"Id_snap_id", 2, "snapshots"},
		{"meta_dir", 		"Id_idd", 		3, "index_repo"},
		{"idd_file", 		"Id_blob", 		3, "index_repo"},
		{"contents", 		"Id_data_idd",3, "index_repo"},
		{"contents", 		"Id_blob",		3, "index_repo"},
		{"idd_file",		"Id_name",		4, "names"}}

	Printf("\n Check Foreign Key relationship\n")

	// start reflecting
	dyna_table := reflect.ValueOf(db_aggregate).Elem()
	previous_group := 0
	ref_table_keys1 	:= mapset.NewSet[int64]()
	all_good := true
	// loop over all ForeignKeys
	for _, entry := range check_tables {
		table_good := true
		Printf("checking %s.%s vs %s.id\n", entry.table_name, entry.column_name,
			entry.ref_table)
		current_group := entry.group_number
		if current_group != previous_group {
			ref_table := dyna_table.FieldByName("table_" + entry.ref_table)
			ref_table_keys1 = mapset.NewSet[int64]()
			iter := ref_table.MapRange()
			for iter.Next() {
				v2  := reflect.ValueOf(iter.Value())
				ptr := v2.FieldByName("ptr")

				// we need to o back to real data, refection is too difficult
				switch entry.ref_table {
				case "packfiles":
					data := *(*PackfilesRecordMem)(ptr.UnsafePointer())
					ref_table_keys1.Add(int64(data.Id))
				case "snapshots":
					data := *(*SnapshotRecordMem)(ptr.UnsafePointer())
					ref_table_keys1.Add(int64(data.Id))
				case "index_repo":
					data := *(*IndexRepoRecordMem)(ptr.UnsafePointer())
					ref_table_keys1.Add(int64(data.Id))
				case "names":
					data := *(*NamesRecordMem)(ptr.UnsafePointer())
					ref_table_keys1.Add(int64(data.Id))
				}
			}
			previous_group = current_group
		}

		// start iterating over dynamic table, values() only
		iter := dyna_table.FieldByName("table_" + entry.table_name).MapRange()
		for iter.Next() {
			ptr := reflect.ValueOf(iter.Value()).FieldByName("ptr")

			var data interface{}
			switch entry.table_name {
			// need to cast, straight indirection of 'ptr' impossible (or tricky??)
			case "index_repo":
				data = *(*IndexRepoRecordMem)(ptr.UnsafePointer())
			case "meta_dir":
				data = *(*MetaDirRecordMem)(ptr.UnsafePointer())
			case "idd_file":
				data = *(*IddFileRecordMem)(ptr.UnsafePointer())
			case "contents":
				data = *(*ContentsRecordMem)(ptr.UnsafePointer())
			}

			v3 := reflect.ValueOf(data)
			to_be_checked := v3.FieldByName(entry.column_name).Int()
			if !ref_table_keys1.Contains(to_be_checked) {
				table_good = false
				all_good   = false
				Printf("%s.%s[%v] not in %s.id\n", entry.table_name, entry.column_name,
					to_be_checked, entry.ref_table)
			}
		}
		Printf("table compares %v\n", table_good)
	}
	return all_good
}

// utility function - compare_keys
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

type ComparatorFunc func(*DBAggregate, *RepositoryData, *Newcomers) bool
func GenericCompare(table_name string, db_aggregate *DBAggregate, comparator ComparatorFunc,
repositoryData *RepositoryData, newComers *Newcomers, return_value *bool) error {
	Printf("checking table %s\n", table_name)
	*return_value = comparator(db_aggregate, repositoryData, newComers)
	return nil
}
