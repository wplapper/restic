package main

import (
	//system
	"time"

	// restic
	"github.com/wplapper/restic/library/repository"
	"github.com/wplapper/restic/library/restic"

	// sqlx for sqlite3
	"github.com/jmoiron/sqlx"

	// mapset
	"github.com/deckarep/golang-set/v2"
)

const ONE_MEG = float64(1024.0 * 1024.0)

// type definions
type IntID uint32

type BlobFile2 struct {
	// name, size, type_, mtime, content and subtree ID
	size       uint64
	DeviceID   uint64
	inode      uint64
	content    []IntID
	subtree_ID IntID
	name       string
	Type       string
	mtime      time.Time
}

type Index_Handle struct {
	blob_index         IntID
	pack_index         IntID
	size               int
	Type               restic.BlobType
	UncompressedLength int
}

type PackSetType struct {
	Type    restic.BlobType
	PackSet mapset.Set[IntID]
}

type RootOfTree struct {
	meta_blob    IntID
	multiplicity int16
	name         string
}

// the holding collections
type RepositoryData struct {
	// all snapshots
	snaps         []*restic.Snapshot
	snap_map      map[string]*restic.Snapshot
	directory_map map[IntID][]BlobFile2
	fullpath      map[IntID]string                 // directory ID -> full directory path
	names         map[IntID]string                 // flat map of all directory names
	children      map[IntID]mapset.Set[IntID]      // parent -> directory children
	meta_dir_map  map[*restic.ID]mapset.Set[IntID] // flattened tree structure
	index_handle  map[restic.ID]Index_Handle

	// the next two entries manage the restic.ID to IntID relationships
	blob_to_index map[restic.ID]IntID
	index_to_blob []restic.ID
	// more data on demand
	//orphaned_index_handle map[restic.ID]Index_Handle
	data_map              map[IntID]mapset.Set[CompIddFile]
	reverse_fullpath      map[string]mapset.Set[IntID]
	blobs_per_packID      map[IntID]mapset.Set[IntID]
	all_blobs             mapset.Set[IntID]
	roots                 []RootOfTree
	repo                  *repository.Repository
	rename_children       map[string]string
}

type DBAggregate struct {
	repositoryData    *RepositoryData
	db_conn           *sqlx.DB
	tx                *sqlx.Tx
	table_counts      map[string]int // count of all tables
	// the database tables - memory representation
	Table_snapshots   map[string]SnapshotRecordMem
	Table_index_repo  map[IntID]*IndexRepoRecordMem
	Table_packfiles   map[IntID]*PackfilesRecordMem
	Table_names       map[string]*NamesRecordMem
	Table_meta_dir    map[CompMetaDir]*MetaDirRecordMem
	Table_idd_file    map[CompIddFile]*IddFileRecordMem
	Table_contents    map[CompContents]*ContentsRecordMem
	Table_dir_path_id map[IntID]*DirPathIdMem
	Table_fullname    map[string]*FullnameMem
	// other tables reference these tables via FOREIGN KEY
	pk_snapshots      map[int]string
	pk_index_repo     map[int]IntID
	pk_fullname       map[int]string
}

type CompIndexOffet struct {
	// the following triple maps a data blob
	data_blob     restic.ID
	meta_blob     restic.ID // unique, part1
	meta_blob_int IntID // unique, part1
	position  int       // unique, part2
	offset    int       // unique, part3

	data_blob_str string
	meta_blob_str string
 	name          string
}

type RenameNames struct {
  From_name string `json:"from_name"`
  To_name   string `json:"to_name"`
}
