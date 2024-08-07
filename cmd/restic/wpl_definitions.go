package main

import (
  //system
  "os"
  "time"

  // restic
  "github.com/wplapper/restic/library/repository"
  "github.com/wplapper/restic/library/restic"

  // mapset
  "github.com/deckarep/golang-set/v2"
)

const ONE_MEG = float64(1024.0 * 1024.0)

// type definions
type IntID uint32

type SnapshotWpl struct {
  ID       restic.ID
  Time     time.Time `json:"time"`
  Paths    []string  `json:"paths"`
  Hostname string    `json:"hostname,omitempty"`
  Tree     restic.ID
}

type BlobFile2 struct {
  // name, size, type_, mtime, content and subtree ID
  size       uint64
  inode      uint64
  DeviceID   uint64
  content    []IntID
  subtree_ID IntID
  name       string
  Type       string
  mtime      time.Time
  Mode       os.FileMode
  Links      uint64
  LinkTarget string
}

type Index_Handle struct {
  blob_index         IntID
  pack_index         IntID
  size               int
  Type               restic.BlobType
  UncompressedLength int
}

type RootOfTree struct {
  meta_blob    IntID
  multiplicity int16
  name         string
}

// the holding collections
type RepositoryData struct {
  // all snapshots
  Snaps         []SnapshotWpl
  SnapMap       map[string]SnapshotWpl
  DirectoryMap  map[IntID][]BlobFile2
  FullPath      map[IntID]string                // directory ID -> full directory path
  MetaDirMap    map[restic.ID]mapset.Set[IntID] // flattened tree structure
  IndexHandle   map[restic.ID]Index_Handle

  // the next two entries manage the restic.ID to IntID relationships
  BlobToIndex   map[restic.ID]IntID
  IndexToBlob   []restic.ID

  // more data on demand
  roots         []RootOfTree
  repo          *repository.Repository
}

type CompIndexOffet struct {
  // the following triple maps a data blob
  data_blob     restic.ID
  meta_blob     restic.ID
  meta_blob_int IntID     // unique, part1
  position      int       // unique, part2
  offset        int       // unique, part3

  data_blob_str string
  meta_blob_str string
  name          string
}

type SnapGroup struct {
  Hostname   string
  FileSystem string
}

type GroupInfo struct {
  snap_groups          map[SnapGroup][]SnapshotWpl
  group_numbers_sorted []int
  group_keys           []SnapGroup
  group_numbers        map[SnapGroup]int
  map_snap_2_ix        map[string]int
}

type CompIddFile struct {
  meta_blob IntID
  position  int
}

type CompPackfile struct {
  PackBlobSet  mapset.Set[IntID]
  PackfileType restic.BlobType
}

type DeviceAndInode struct {
  DeviceID   uint64
  Inode      uint64
}

// the list of well known repositories
var WplRepositoryList = []string{
  "/media/mount-points/Backup-ext4-Mate/restic_home",
  "/media/mount-points/Backup-ext4-Mate/restic_massive",
  "/media/mount-points/Backup-ext4-Mate/restic_rasp_winxp",

  "rclone:onedrive:restic_home",
  "rclone:onedrive:restic_massive",
  "rclone:onedrive:restic_rasp_winxp",
}
