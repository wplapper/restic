package main

import (
	//system
	"time"

	// restic
	"github.com/wplapper/restic/library/restic"

	// sets
	"github.com/deckarep/golang-set/v2"

	// sqlx for sqlite3
	"github.com/jmoiron/sqlx"
)

const (
	ONE_MEG = float64(1024.0 * 1024.0)
)

// type definions
type IntID int

type BlobFile2 struct {
	// name, size, type_, mtime, content and subtree ID
	size       uint64
	inode      uint64
	content    []restic.IntID
	subtree_ID restic.IntID
	name       string
	Type       string
	mtime      time.Time
}

type Index_Handle struct {
	blob_index restic.IntID
	pack_index restic.IntID
	size       int
	Type       restic.BlobType
}

type RemoveTable struct {
	Id int
}

type UpdateTable_index_repo struct {
	Id         int
	Id_pack_id int
}

type DBOptions struct {
	echo               bool
	print_count_tables bool
	altDB              string
	rollback           bool
	timing             bool
	fullpath           bool
}

//==============================================================================

// the following types represent database tables and their content in database and
// in memory
type SnapshotRecordMem struct {
	Id           int
	Snap_id      string
	Snap_time    string
	Snap_host    string
	Snap_fsys    string
	Id_snap_root string
	Status 			 string
}

type IndexRepoRecordMem struct {
	Id         int
	Idd        string
	Idd_size   int
	Index_type string
	Id_pack_id int			// back pointer to packfiles
	Status   	 string
}

type NamesRecordMem struct {
	Id        int
	Name      string
	Status    string
}

type MetaDirRecordMem struct {
	Id         int
	Id_snap_id int // map back to snapshots
	Id_idd     int // map back to index_repo
	Status     string
}

type ContentsRecordMem struct {
	Id          int
	Id_data_idd int // map back to index_repo.ids
	Id_blob     int // map back to index_repo.id
	Position    int
	Offset      int
	Status      string
}

type IddFileRecordMem struct {
	Id       int
	Id_blob  int // back pointer to index_repo
	Position int
	Id_name  int
	Size     int
	Inode    int64
	Mtime    string
	Type     string
	Status   string
}

type PackfilesRecordMem struct {
	Id          int
	Packfile_id string
	Status      string
}

type DirChildrenMem struct {
	Id        int
	Id_parent int
	Id_child  int
	Status    string
}

type DirNameIdMem struct {
	Id        int
	Id_name   int
	Status    string
}

type FullnameMem struct {
	Id        int
	Pathname  string
	Status    string
}

type DirPathIdMem struct {
	Id          int
	Id_pathname int
	Status      string
}

type TimeStamp struct {
	Id               int
	Restic_updated   time.Time
	Database_updated time.Time
	Ts_created       time.Time
}

// Composite indices for maps
type CompMetaDir struct {
	// composite index on MetaDirRecordMem
	snap_id   string // consider restic.IntID
	meta_blob restic.IntID
}

type CompIddFile struct {
	meta_blob restic.IntID
	position  int
}

type CompContents struct {
	meta_blob restic.IntID
	position  int
	offset    int
}

type CompDirChildren struct {
	meta_blob_parent restic.IntID
	meta_blob_child  restic.IntID
}

//==============================================================================

// the holding collections
type RepositoryData struct {
	// all snapshots
	snaps         []*restic.Snapshot
	snap_map      map[string]*restic.Snapshot
	directory_map map[restic.IntID][]BlobFile2
	fullpath      map[restic.IntID]string
	names         map[restic.IntID]string
	children      map[restic.IntID]restic.IntSet
	meta_dir_map  map[*restic.ID]restic.IntSet
	index_handle  map[restic.ID]Index_Handle

	// the last two entries manage the restic.ID to *restic.ID relationships
	blob_to_index map[restic.ID]restic.IntID
	// we use &index_to_blob to create a *restid.ID pointer
	index_to_blob []restic.ID
}

type Newcomers struct {
	// the containers of various memory tables
	Mem_snapshots    map[string]SnapshotRecordMem
	Mem_index_repo   map[restic.IntID]*IndexRepoRecordMem
	Mem_names        map[string]*NamesRecordMem
	Mem_idd_file     map[CompIddFile]*IddFileRecordMem
	Mem_meta_dir     map[CompMetaDir]*MetaDirRecordMem
	Mem_contents     map[CompContents]*ContentsRecordMem
	Mem_packfiles    map[restic.IntID]*PackfilesRecordMem
	Mem_dir_children map[CompDirChildren]*DirChildrenMem
	Mem_dir_name_id  map[restic.IntID]*DirNameIdMem
	Mem_dir_path_id  map[restic.IntID]*DirPathIdMem
	Mem_fullname     map[string]*FullnameMem

	// we also need sets for easy manipulation
	old_names        mapset.Set[string]
	old_packfiles    mapset.Set[restic.IntID]
}

type DBAggregate struct {
	repositoryData   *RepositoryData
	db_conn          *sqlx.DB
	tx               *sqlx.Tx
	table_counts     map[string]int // count of all tables

	// the database tables - memory representation
	Table_snapshots    map[string]SnapshotRecordMem
	Table_index_repo   map[restic.IntID]*IndexRepoRecordMem
	Table_meta_dir     map[CompMetaDir]*MetaDirRecordMem
	Table_packfiles    map[restic.IntID]*PackfilesRecordMem
	Table_idd_file     map[CompIddFile]*IddFileRecordMem
	Table_names        map[string]*NamesRecordMem
	Table_contents     map[CompContents]*ContentsRecordMem
	Table_dir_children map[CompDirChildren]*DirChildrenMem
	Table_dir_name_id  map[restic.IntID]*DirNameIdMem
	Table_dir_path_id  map[restic.IntID]*DirPathIdMem
	Table_fullname     map[string]*FullnameMem

	// other tables reference these tables via FOREIGN KEY
	pk_snapshots     map[int]string        // meta_dir
	pk_index_repo    map[int]restic.IntID  // meta_dir, idd_file, contents,
	                 // dir_children, dir_name_id, fullname, dir_path_id
	pk_names         map[int]string        // point back to names table
	pk_fullname      map[int]string
	// idd_files references names(id)
	// dir_path_id references fullname(id)
	// index_repo references packfiles(id)
}

// map repos to databases - really a const, but not according to the Go gospel
var DATABASE_NAMES = map[string]string{
	// master
	"/media/mount-points/Backup-ext4-Mate/restic_master":  "/media/mount-points/home/wplapper/restic/db/restic-master_nfs.db",
	"/media/mount-points/Backup-ext4-Mate/restic_master/": "/media/mount-points/home/wplapper/restic/db/restic-master_nfs.db",

	// onedrive
	"rclone:onedrive:restic_backups": "/media/mount-points/home/wplapper/restic/db/restic-onedrive.db",

	// data
	"/media/wplapper/internal-fast/restic_Data":  "/home/wplapper/restic/db/XPS-restic-data_nfs.db",
	"/media/wplapper/internal-fast/restic_Data/": "/home/wplapper/restic/db/XPS-restic-data_nfs.db",

	// test
	"/media/wplapper/internal-fast/restic_test":  "/home/wplapper/restic/db/XPS-restic-test.db",
	"/media/wplapper/internal-fast/restic_test/": "/home/wplapper/restic/db/XPS-restic-test.db",
}
