package main

import (
	//system
	"time"

	// restic
	"github.com/wplapper/restic/library/restic"

	// sets
	"github.com/wplapper/restic/library/mapset"

	// sqlx for sqlite3
	"github.com/jmoiron/sqlx"
)

// struct and type definitions

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
	size       uint
	Type       restic.BlobType
}

type RemoveTable struct {
	Id int
}

type UpdateTable_index_repo struct {
	Id int
	Id_pack_id int
}

type RemoveSqLTable struct {
	sql 			 string
	table_name string
}

type DBOptions struct {
	echo               bool
	print_count_tables bool
	altDB              string
	rollback           bool
}
//==============================================================================

// the following types represent databse tables and teir content in the
// database and in memory
type SnapshotRecordDB struct {
	Id           int
	Snap_id      string
	Snap_time    string
	Snap_host    string
	Snap_fsys    string
	Id_snap_root string
}

type SnapshotRecordMem struct {
	SnapshotRecordDB
	// mem
	ID_mem 			*restic.ID
	root   			*restic.ID
	Status 			string
}

type IndexRepoRecordDB struct {
	Id         int
	Idd        string
	Idd_size   int
	Index_type string
	Id_pack_id int
}

type IndexRepoRecordMem struct {
	IndexRepoRecordDB
	// mem
	idd      	*restic.ID
	packfile 	*restic.ID
	Status   	string
}

type NamesRecordDB struct {
	Id        int
	Name      string
	Name_type string
}

type NamesRecordMem struct {
	NamesRecordDB
	// mem
	Status 		string
}

type MetaDirRecordDB struct {
	Id         int
	Id_snap_id int // map back to snapshots
	Id_idd     int // map back to index_repo
}

type MetaDirRecordMem struct {
	MetaDirRecordDB
	// mem
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
	ContentsRecordDB
	// mem
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
	IddFileRecordDB
	// mem
	name   	string
	Status 	string
}

type PackfilesRecordDB struct {
	Id          int
	Packfile_id string
}

type PackfilesRecordMem struct {
	PackfilesRecordDB
	// mem
	Status 			string
}

type TimeStamp struct {
  Id                int
  Restic_updated    time.Time
  Database_updated  time.Time
  Ts_created        time.Time
}

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
//==============================================================================

// the holding collections
type RepositoryData struct {
	// all snapshots
	snaps 				[]*restic.Snapshot
	directory_map map[restic.IntID][]BlobFile2
	fullpath 			map[restic.IntID]string
	names 				map[restic.IntID]string
	children 			map[restic.IntID]restic.IntSet
	meta_dir_map 	map[*restic.ID]restic.IntSet
	index_handle 	map[restic.ID]Index_Handle

	// the last two entries manage the restic.ID to *restic.ID relationships
	blob_to_index map[restic.ID]restic.IntID
	index_to_blob []restic.ID
}

type Newcomers struct {
	// the contanets of various meory tables
	mem_snapshots  map[string]SnapshotRecordMem
	mem_index_repo map[restic.ID]IndexRepoRecordMem
	mem_names      map[string]NamesRecordMem
	mem_idd_file   map[CompIddFile]IddFileRecordMem
	mem_meta_dir   map[CompMetaDir]MetaDirRecordMem
	mem_contents   map[CompContents]ContentsRecordMem
	mem_packfiles  map[*restic.ID]PackfilesRecordMem

	// we aso need sets for easy manipulation
	new_snapshots  mapset.Set[string]
	new_index_repo mapset.Set[restic.ID]
	new_names      mapset.Set[string]
	new_idd_file   mapset.Set[CompIddFile]
	new_meta_dir   mapset.Set[CompMetaDir]
	new_contents   mapset.Set[CompContents]
	new_packfiles  mapset.Set[*restic.ID]

	old_snapshots  mapset.Set[string]
	old_index_repo mapset.Set[restic.ID]
	old_names      mapset.Set[string]
	old_idd_file   mapset.Set[CompIddFile]
	old_packfiles  mapset.Set[*restic.ID]
}

type DBAggregate struct {
	repositoryData 		*RepositoryData
	db_conn        		*sqlx.DB
	table_counts   		map[string]int // count of all tables

	// the database tables - memory representation
	table_snapshots  	map[string]SnapshotRecordMem
	table_index_repo 	map[restic.ID]IndexRepoRecordMem
	table_meta_dir   	map[CompMetaDir]MetaDirRecordMem
	table_packfiles  	map[*restic.ID]PackfilesRecordMem
	table_idd_file   	map[CompIddFile]IddFileRecordMem
	table_names      	map[string]NamesRecordMem
	table_contents   	map[CompContents]ContentsRecordMem

	// other tables reference these tables via FOREIGN KEY
	pk_snapshots  		map[int]string     // meta_dir
	pk_index_repo 		map[int]restic.ID  // meta_dir, idd_file, contents
	pk_names      		map[int]string     // idd_file
	pk_packfiles  		map[int]*restic.ID // index_repo
}

