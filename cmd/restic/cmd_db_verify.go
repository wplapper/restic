package main

// compile with "go run build.go -tags debug""
// run with DEBUG_LOG=/home/wplapper/restic/debug.log fully-qualified-name/restic -r <repo> overview

// ref to onedrive: rclone:onedrive:restic_backups

import (
	// system
	"sort"
	"time"
	//"math"
	//"fmt"

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

type DBOptions struct {
	echo               bool
	print_count_tables bool
	altDB              string
	rollback           bool
}

var dbOptions DBOptions

type SnapshotRecordDB struct {
	Id           int
	Snap_id      string
	Snap_time    string
	Snap_host    string
	Snap_fsys    string
	Id_snap_root string
}

type SnapshotRecordMem struct {
	// raw part
	SnapshotRecordDB
	ID_mem *restic.ID
	root   *restic.ID
	Status string
}

type IndexRepoRecordDB struct {
	Id         int
	Idd        string
	Idd_size   int
	Index_type string
	Id_pack_id int // unused
}

type IndexRepoRecordMem struct {
	// raw part
	IndexRepoRecordDB
	idd      *restic.ID
	packfile *restic.ID
	Status   string
}

type NamesRecordDB struct {
	Id        int
	Name      string
	Name_type string
}

type NamesRecordMem struct {
	NamesRecordDB
	Status string
}

// database record mapping for sqlx.StructScan
type MetaDirRecordDB struct {
	Id         int
	Id_snap_id int // map back to snapshots
	Id_idd     int // map back to index_repo
}

type MetaDirRecordMem struct {
	MetaDirRecordDB
	Status string
}

type ContentsRecordDB struct {
	Id          int
	Id_data_idd int // map back to index_repo
	Id_blob     int // map back to index_repo
	Position    int
	Offset      int
	Id_fullpath int // deadbeef
}

type ContentsRecordMem struct {
	// raw part
	ContentsRecordDB
	id_data_idd *restic.ID
	Status      string
}

type IddFileRecordDB struct {
	Id       int
	Id_blob  int // map back to index_repo
	Position int
	Id_name  int
	Size     int
	Inode    int64
	Mtime    string
	Type     string
}

type IddFileRecordMem struct {
	// raw part
	IddFileRecordDB
	name   string
	Status string
}

type PackfilesRecordDB struct {
	Id          int
	Packfile_id string
}

type PackfilesRecordMem struct {
	PackfilesRecordDB
	Status string
}

type DBAggregate struct {
	repositoryData 		*RepositoryData
	db_conn        		*sqlx.DB
	table_counts   		map[string]int // count of all tables

	// the database tables - memory representation
	table_snapshots  	*map[string]SnapshotRecordMem
	table_index_repo 	*map[restic.ID]IndexRepoRecordMem
	table_meta_dir   	*map[CompMetaDir]MetaDirRecordMem
	table_packfiles  	*map[*restic.ID]PackfilesRecordMem
	table_idd_file   	*map[CompIddFile]IddFileRecordMem
	table_names      	*map[string]NamesRecordMem
	table_contents   	*map[CompContents]ContentsRecordMem

	// other tables reference these tables via FOREIGN KEY
	pk_snapshots  		*map[int]string     // meta_dir
	pk_index_repo 		*map[int]restic.ID  // meta_dir, idd_file, contents
	pk_names      		*map[int]string     // idd_file
	pk_packfiles  		*map[int]*restic.ID // index_repo
}

var db_aggregate DBAggregate

// Composite indices for maps
type CompMetaDir struct {
	// composite index on MetaDirRecordMem
	snap_id   string // consider pointers
	meta_blob restic.ID
}

type CompIddFile struct {
	meta_blob restic.ID
	position  int
}

type CompContents struct {
	meta_blob restic.ID
	position  int
	offset    int
}

// map repo to database
var DATABASE_NAMES = map[string]string{
	// master
	"/media/mount-points/Backup-ext4-Mate/restic_master":  "/media/mount-points/home/wplapper/restic/db/restic-master_nfs.db",
	"/media/mount-points/Backup-ext4-Mate/restic_master/": "/media/mount-points/home/wplapper/restic/db/restic-master_nfs.db",

	// onedrive
	"rclone:onedrive:restic_backups":
	//"/media/mount-points/home/wplapper/restic/db/restic-onedrive.db",
	"/home/wplapper/restic/db/restic-onedrive.db",

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
}

func runDBVerify(gopts GlobalOptions, args []string) error {

	// step 0: setup global stuff
	start := time.Now()
	_ = start
	repositoryData := init_repositoryData() // is a *RepositoryData
	db_aggregate.repositoryData = repositoryData
	EMPTY_NODE_ID = restic.Hash([]byte("{\"nodes\":[]}\n"))
	newComers := InitNewcomers()
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
	var table_name string
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

	// compare snapshots
	table_name = "snapshots"
	Printf("checking table %s\n", table_name)
	equal := check_db_snapshots(*db_aggregate.table_snapshots, &db_aggregate,
		repositoryData, newComers)
	if equal {
		Printf("table %s compares equal\n", table_name)
	} else {
		Printf("table %s mismatch!\n", table_name)
	}

	// compare index_repo
	table_name = "index_repo"
	Printf("checking table %s\n", table_name)
	equal = check_db_index_repo(*db_aggregate.table_index_repo, &db_aggregate,
		repositoryData, newComers)
	if equal {
		Printf("table %s compares equal\n", table_name)
	} else {
		Printf("table %s mismatch!\n", table_name)
	}

	// compare packfiles
	table_name = "packfiles"
	Printf("checking table %s\n", table_name)
	equal = check_db_packfiles(*db_aggregate.table_packfiles, &db_aggregate,
		repositoryData, newComers)
	if equal {
		Printf("table %s compares equal\n", table_name)
	} else {
		Printf("table %s mismatch!\n", table_name)
	}

	// compare contents
	table_name = "contents"
	Printf("checking table %s\n", table_name)
	equal = check_db_contents(*db_aggregate.table_contents, &db_aggregate,
		repositoryData, newComers)
	if equal {
		Printf("table %s compares equal\n", table_name)
	} else {
		Printf("table %s mismatch!\n", table_name)
	}

	// compare meta_dir
	table_name = "meta_dir"
	Printf("checking table %s\n", table_name)
	equal = check_db_meta_dir(*db_aggregate.table_meta_dir, &db_aggregate,
		repositoryData, newComers)
	if equal {
		Printf("table %s compares equal\n", table_name)
	} else {
		Printf("table %s mismatch!\n", table_name)
	}

	// compare names
	table_name = "names"
	Printf("checking table %s\n", table_name)
	equal = check_db_names(*db_aggregate.table_names, &db_aggregate,
		repositoryData, newComers)
	if equal {
		Printf("table %s compares equal\n", table_name)
	} else {
		Printf("table %s mismatch!\n", table_name)
	}

	// compare idd_file
	table_name = "idd_file"
	Printf("checking table %s\n", table_name)
	equal = check_db_idd_file(*db_aggregate.table_idd_file, &db_aggregate,
		repositoryData, newComers)
	if equal {
		Printf("table %s compares equal\n", table_name)
	} else {
		Printf("table %s mismatch!\n", table_name)
	}

	// cehck foreiggn key relationship
	equal = CheckForeignKeys(&db_aggregate, repositoryData)
	return nil
}

func check_db_snapshots(db_snapshots map[string]SnapshotRecordMem,
	db_aggregate *DBAggregate, repositoryData *RepositoryData, newComers *Newcomers) bool {
	// compare snapshots from repo with snapshots stored in the database
	// step 1: build memory table to allow the comparison
	mem_snapshots := CreateMemSnapshots(db_aggregate, repositoryData, newComers)

	// compare snapshot keys
	equal := CompareKeys("snapshots", db_snapshots, mem_snapshots)
	if !equal {
		set_db_keys  := mapset.NewSet[string]()
		set_mem_keys := mapset.NewSet[string]()
		for key := range db_snapshots {
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
			if count > 100 {
				break
			}
		}
		return equal
	}

	// compare snapshot values
	compare_equals := true
	for db_key, db_value := range db_snapshots {
		mem_value := mem_snapshots[db_key]
		if db_value.Snap_host != mem_value.Snap_host || db_value.Snap_time != mem_value.Snap_time {
			Printf("db  %s %s %s %s\n", db_value.Snap_time, db_value.Snap_host, db_value.Snap_fsys, db_value.root)
			Printf("mem %s %s %s %s\n", mem_value.Snap_time, mem_value.Snap_host, mem_value.Snap_fsys, mem_value.root)
			compare_equals = false
		}
	}
	return compare_equals
}

func check_db_index_repo(db_index_repo map[restic.ID]IndexRepoRecordMem,
	db_aggregate *DBAggregate, repositoryData *RepositoryData, newComers *Newcomers) bool {

	mem_repo_index_map := CreateMemIndexRepo(db_aggregate, repositoryData, newComers)
	equal := CompareKeys("index_repo", db_index_repo, mem_repo_index_map)
	if !equal {
		return equal
	}

	// compare table values
	compare_equals := true
	for db_key, db_value := range db_index_repo {
		mem_value := mem_repo_index_map[db_key]
		if db_value.Idd_size != mem_value.Idd_size || db_value.Index_type != mem_value.Index_type {
			Printf("v db  %7d\n", db_value.Idd_size)
			Printf("v mem %7d\n", mem_value.Idd_size)
			compare_equals = false
		}
	}
	return compare_equals
}

func check_db_names(db_names map[string]NamesRecordMem,
	db_aggregate *DBAggregate, repositoryData *RepositoryData, newComers *Newcomers) bool {
	// build a memory map of the names, whic come from three(3) different sources
	table_name := "names"
	mem_names_map := CreateMemNames(db_aggregate, repositoryData, newComers)
	equal := CompareKeys(table_name, db_names, mem_names_map)
	if equal {
		return equal
	}

	set_db_keys  := mapset.NewSet[string]()
	set_mem_keys := mapset.NewSet[string]()
	for key := range db_names {
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

func check_db_packfiles(db_packfiles map[*restic.ID]PackfilesRecordMem,
	db_aggregate *DBAggregate, repositoryData *RepositoryData, newComers *Newcomers) bool {
	//table_name := "packfiles"
	// build a memory map of the packfiles
	mem_packfiles_map := CreateMemPackfiles(db_aggregate, repositoryData, newComers)

	// compare keys
	return CompareKeys("packfiles", db_packfiles, mem_packfiles_map)
}

// check_db_contents
func check_db_contents(table_contents map[CompContents]ContentsRecordMem,
	db_aggregate *DBAggregate, repositoryData *RepositoryData, newComers *Newcomers) bool {
	//table_name := "contents"
	// build a memory map of the contents
	mem_contents_map := CreateMemContents(db_aggregate, repositoryData, newComers)

	// compare keys
	equal := CompareKeys("contents", table_contents, mem_contents_map)
	if !equal {
		Printf("**** Key mismatch for contents ***\n")
		set_db_keys  := mapset.NewSet[CompContents]()
		set_mem_keys := mapset.NewSet[CompContents]()
		for key := range table_contents {
			set_db_keys.Add(key)
		}
		for key := range table_contents {
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
	for db_key, db_value := range table_contents {
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
	for db_key, db_value := range table_contents {
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

func check_db_meta_dir(db_meta_dir map[CompMetaDir]MetaDirRecordMem,
	db_aggregate *DBAggregate, repositoryData *RepositoryData, newComers *Newcomers) bool {
	mem_meta_dir_map := CreateMemMetaDir(db_aggregate, repositoryData, newComers)

	// compare keys
	equal := CompareKeys("meta_dir", db_meta_dir, mem_meta_dir_map)
	if !equal {
		set_db_keys  := mapset.NewSet[CompMetaDir]()
		set_mem_keys := mapset.NewSet[CompMetaDir]()
		for key := range db_meta_dir {
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

func check_db_idd_file(db_idd_file map[CompIddFile]IddFileRecordMem,
	db_aggregate *DBAggregate, repositoryData *RepositoryData, newComers *Newcomers) bool {
	table_name := "idd_file"
	// build a memory map of the packfiles
	mem_idd_file_map := CreateMemIddFile(db_aggregate, repositoryData, newComers)

	// compare keys
	equal := CompareKeys(table_name, db_idd_file, mem_idd_file_map)
	if !equal {
		return equal
	}

	// check contents of idd_file
	for db_key, db_value := range db_idd_file {
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
	/*
		xref_table = (
				(index_repo.id_pack_id, packfiles.id),
				(meta_dir.id_snap_id,   snapshots.id),

				(meta_dir.id_idd,      index_repo.id),
				(idd_file.id_blob,     index_repo.id),
				(contents.id_data_idd, index_repo.id),
				(contents.id_blob,     index_repo.id),

				(idd_file.id_name,     names.id),
	*/
	Printf("\n Check Foreign Key reationship\n")
	all_good := true
	ref_table_keys := mapset.NewSet[int]()
	for _, value := range *db_aggregate.table_packfiles {
		ref_table_keys.Add(value.Id)
	}

	for key, value := range *db_aggregate.table_index_repo {
		if !ref_table_keys.Contains(value.Id_pack_id) {
			all_good = false
			Printf("index_repo[%v].id_pack_id not in packfiles\n", key)
		}
	}
	Printf("(index_repo.id_pack_id, packfiles.id) %v\n", all_good)

	//		(meta_dir.id_snap_id,   snapshots.id)
	all_good = true
	ref_table_keys = mapset.NewSet[int]()
	for _, value := range *db_aggregate.table_snapshots {
		ref_table_keys.Add(value.Id)
	}

	for key, value := range *db_aggregate.table_meta_dir {
		if !ref_table_keys.Contains(value.Id_snap_id) {
			all_good = false
			Printf("meta_dir[%v].Id_snap_id not in table_snapshots\n", key)
		}
	}
	Printf("(meta_dir.id_snap_id,   snapshots.id) %v\n", all_good)

	all_good = true
	ref_table_keys = mapset.NewSet[int]()
	for _, value := range *db_aggregate.table_index_repo {
		ref_table_keys.Add(value.Id)
	}

	for key, value := range *db_aggregate.table_meta_dir {
		if !ref_table_keys.Contains(value.Id_idd) {
			all_good = false
			Printf("meta_dir[%v].Id_idd not in index_repo\n", key)
		}
	}
	Printf("(meta_dir.id_idd,      index_repo.id) %v\n", all_good)

	// (idd_file.id_blob,     index_repo.id)
	all_good = true
	for key, value := range *db_aggregate.table_idd_file {
		if !ref_table_keys.Contains(value.Id_blob) {
			all_good = false
			Printf("idd_file[%v].id_blob not in index_repo\n", key)
		}
	}
	Printf("(idd_file.id_blob,     index_repo.id) %v\n", all_good)

	// (contents.id_data_idd, index_repo.id)
	all_good = true
	for key, value := range *db_aggregate.table_contents {
		if !ref_table_keys.Contains(value.Id_data_idd) {
			all_good = false
			Printf("idd_file[%v].Id_data_idd not in index_repo\n", key)
		}
	}
	Printf("(contents.id_data_idd, index_repo.id) %v\n", all_good)

	// (contents.id_blob, index_repo.id)
	all_good = true
	for key, value := range *db_aggregate.table_contents {
		if !ref_table_keys.Contains(value.Id_data_idd) {
			all_good = false
			Printf("idd_file[%v].Id_blob not in index_repo\n", key)
		}
	}
	Printf("(contents.id_blob,     index_repo.id) %v\n", all_good)

	// (idd_file.id_name,     names.id)
	ref_table_keys = mapset.NewSet[int]()
	for _, value := range *db_aggregate.table_names {
		ref_table_keys.Add(value.Id)
	}

	for key, value := range *db_aggregate.table_idd_file {
		if !ref_table_keys.Contains(value.Id_name) {
			all_good = false
			Printf("meta_dir[%v].id_name not in index_repo\n", key)
		}
	}
	Printf("(idd_file.id_name,     names.id) %v\n", all_good)
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
