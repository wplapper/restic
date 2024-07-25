package main

// collect some globals stats about this repository

import (
  // system
  "context"
  "crypto/sha256"
  "sort"
  "strings"
  "time"

  //argparse
  "github.com/spf13/cobra"

  // restic library
  //"github.com/wplapper/restic/library/pack"
  "github.com/wplapper/restic/library/repository/pack"
  "github.com/wplapper/restic/library/restic"

  // sets
  "github.com/deckarep/golang-set/v2"
)

type RepoDetailsOptions struct {
  SameTree  bool
  Prune     bool
  Detail    int
  Lost      bool
  Timing    bool
  MemoryUse bool
}

var repo_details_options RepoDetailsOptions

var cmdRepoDetails = &cobra.Command{
  Use:   "wpl-repo [flags]",
  Short: "counts various tables and report usage",
  Long: `counts various tables and report usage.

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
  DisableAutoGenTag: false,
  RunE: func(cmd *cobra.Command, args []string) error {
    return runRepoDetails(cmd.Context(), cmd, globalOptions)
  },
}

func init() {
  cmdRoot.AddCommand(cmdRepoDetails)
  f := cmdRepoDetails.Flags()
  f.BoolVarP(&repo_details_options.SameTree, "same-tree", "S", false, "show snapshots with the same tree")
  f.BoolVarP(&repo_details_options.Prune, "Prune", "P", false, "make Prune calculations")
  f.BoolVarP(&repo_details_options.Timing, "Timing", "T", false, "produce timings")
  f.BoolVarP(&repo_details_options.MemoryUse, "memory", "m", false, "produce memory usage")
  f.CountVarP(&repo_details_options.Detail, "Detail", "D", "print dir/file details")
}

func runRepoDetails(ctx context.Context, cmd *cobra.Command, gopts GlobalOptions) error {

  var repositoryData RepositoryData

  // startup
  start := time.Now()
  gOptions = gopts
  init_repositoryData(&repositoryData)

  // step 1: open repository
  repo, err := OpenRepository(ctx, gopts)
  if err != nil {
    return err
  }
  Verboseff("Repository is %s type is %T\n", globalOptions.Repo, repo)
  Printf("repo is %+v\n", *repo)

  // step 2: gather the base information
  err = gather_base_data_repo(repo, gopts, ctx, &repositoryData,
    repo_details_options.Timing)
  if err != nil {
    return err
  }

  // step 3: decide what to do
  if repo_details_options.SameTree {
    FindSameTree(ctx, repo, &repositoryData)
    return nil
  }

  // normal processing
  CountBlobs(ctx, repo, &repositoryData, repo_details_options, start)
  CountFiles(ctx, repo, &repositoryData, repo_details_options, start)
  CountTables(ctx, repo, &repositoryData, repo_details_options, start)
  return nil
}

// go through index table and count tree and data blobs, calculate compressed
// and uncompressed sizes
func CountBlobs(ctx context.Context, repo restic.Repository,
  repositoryData *RepositoryData, options RepoDetailsOptions, start time.Time) {
  count_tree_blobs := 0
  count_data_blobs := 0
  size_tree_blobs := 0
  size_data_blobs := 0
  uc_size_tree_blobs := 0
  uc_size_data_blobs := 0
  pack_set_meta := mapset.NewThreadUnsafeSet[restic.ID]()
  pack_set_data := mapset.NewThreadUnsafeSet[restic.ID]()

  // extract packfile information
  for _, ih := range repositoryData.IndexHandle {
    pidx := repositoryData.IndexToBlob[ih.pack_index]
    if ih.Type == restic.DataBlob {
      count_data_blobs++
      size_data_blobs += int(ih.size)
      uc_size_data_blobs += int(ih.UncompressedLength)
      pack_set_data.Add(pidx)
    } else if ih.Type == restic.TreeBlob {
      count_tree_blobs++
      size_tree_blobs += int(ih.size)
      uc_size_tree_blobs += int(ih.UncompressedLength)
      pack_set_meta.Add(pidx)
    }
  }

  // extract packfiles with their sizes from the Master Index
  // 'repo_packs' is map[restic.ID]int64
  repo_packs, _ := pack.Size(ctx, repo, false)

  // get sizes for meta and data packfiles
  size_meta_pack := int64(0)
  for ID := range pack_set_meta.Iter() {
    size_meta_pack += repo_packs[ID]
  }
  size_data_pack := int64(0)
  for ID := range pack_set_data.Iter() {
    size_data_pack += repo_packs[ID]
  }

  // report
  Printf("\n*** Global counts ***\n")
  Printf("%-25s %10d  %10.1f MiB\n", "  compressed tree blobs", count_tree_blobs,
    float64(size_tree_blobs)/ONE_MEG)
  Printf("%-25s %10d  %10.1f MiB\n", "  compressed data blobs", count_data_blobs,
    float64(size_data_blobs)/ONE_MEG)
  Printf("%-25s %10d  %10.1f MiB\n", "  compressed ALL  blobs",
    count_data_blobs+count_tree_blobs,
    float64(size_tree_blobs+size_data_blobs)/ONE_MEG)

  Printf("%-25s %10d  %10.1f MiB\n", "uncompressed tree blobs", count_tree_blobs,
    float64(uc_size_tree_blobs)/ONE_MEG)
  Printf("%-25s %10d  %10.1f MiB\n", "uncompressed data blobs", count_data_blobs,
    float64(uc_size_data_blobs)/ONE_MEG)
  Printf("%-25s %10d  %10.1f MiB\n", "uncompressed All  blobs",
    count_data_blobs+count_tree_blobs,
    float64(uc_size_tree_blobs+uc_size_data_blobs)/ONE_MEG)

  Printf("%-25s %10d  %10.1f MiB\n", "meta packfiles",
    pack_set_meta.Cardinality(),
    float64(size_meta_pack)/ONE_MEG)
  Printf("%-25s %10d  %10.1f MiB\n", "data packfiles",
    pack_set_data.Cardinality(),
    float64(size_data_pack)/ONE_MEG)
  Printf("%-25s %10d  %10.1f MiB\n", "ALL  packfiles",
    pack_set_meta.Cardinality()+pack_set_data.Cardinality(),
    float64(size_meta_pack+size_data_pack)/ONE_MEG)

  if options.Timing {
    timeMessage(options.MemoryUse, "%-30s %10.1f seconds\n", "CountBlobs",
      time.Now().Sub(start).Seconds())
  }
}

// count index, snapshot and meta file counts and sizes
func CountFiles(ctx context.Context, repo restic.Repository,
  repositoryData *RepositoryData, options RepoDetailsOptions, start time.Time) {
  count_snaps := 0
  size_snaps := int64(0)

  // snapshot files
  tree_set := mapset.NewThreadUnsafeSet[restic.ID]()
  repo.List(ctx, restic.SnapshotFile, func(id restic.ID, size int64) error {
    sn, err := restic.LoadSnapshot(ctx, repo, id)
    if err != nil {
      Printf("Skip loading snap record %s! - reason: '%v'\n", id, err)
      return nil
    }
    count_snaps++
    size_snaps += size
    tree_set.Add(*sn.Tree)
    return nil
  })

  // index files
  count_index := 0
  size_index := int64(0)
  repo.List(ctx, restic.IndexFile, func(id restic.ID, size int64) error {
    count_index++
    size_index += size
    return nil
  })

  // report
  Printf("\n")
  Printf("%-25s %10d  %10.1f MiB\n", "index files", count_index,
    float64(size_index)/ONE_MEG)
  Printf("%-25s %10d  %10.1f KiB\n", "snapshot files", count_snaps,
    float64(size_snaps)/1024.0)
  Printf("%-25s %10d\n", "count trees", tree_set.Cardinality())
  Printf("%s\n", strings.Repeat("=", 52))

  // ** finale **
  if options.Timing {
    timeMessage(options.MemoryUse, "%-30s %10.1f seconds\n", "CountFiles",
      time.Now().Sub(start).Seconds())
  }
}

// count table contents and other interesting information
func CountTables(ctx context.Context, repo restic.Repository,
  repositoryData *RepositoryData, options RepoDetailsOptions, start time.Time) {
  // meta_dir
  count_meta_dir_entries := 0
  for _, meta_blobs := range repositoryData.MetaDirMap {
    count_meta_dir_entries += meta_blobs.Cardinality()
  }

  // we have tuple(node.DeviceID, node.Inode) which is unique
  // but inodes can be re-used over time!
  inodes_meta := mapset.NewThreadUnsafeSet[DeviceAndInode]()
  inodes_datan := mapset.NewThreadUnsafeSet[DeviceAndInode]()
  inodes_datac := mapset.NewThreadUnsafeSet[[sha256.Size]byte]()
  names := mapset.NewThreadUnsafeSet[string]()

  // map inodes back to 'meta_blob_int'
  count_directory_map_entries := 0
  for _, file_list := range repositoryData.DirectoryMap {
    count_directory_map_entries += len(file_list)
    for _, meta := range file_list {
      device_inode := DeviceAndInode{meta.DeviceID, meta.inode}
      if meta.Type == "dir" {
        inodes_meta.Add(device_inode)
      } else if meta.Type == "file" {
        inodes_datac.Add(ConvertContent(meta.content))
        inodes_datan.Add(device_inode)
      }
      names.Add(meta.name)
    }
  }

  // report 1
  Printf("\n")
  Printf("%-25s %10d\n", "sum meta_dir", count_meta_dir_entries)
  Printf("%-25s %10d\n", "sum directory_map", count_directory_map_entries)
  Printf("%-25s %10d\n", "meta inodes", inodes_meta.Cardinality())
  Printf("%-25s %10d (unique content)\n", "data inodes", inodes_datac.Cardinality())
  Printf("%-25s %10d (inode count)\n", "data inodes", inodes_datan.Cardinality())
  Printf("%-25s %10d\n", "base names", names.Cardinality())

  // report 2:
  // build full map for all data blobs
  // data_map_org is map[restic.ID]mapset.Set[CompIndexOffet]
  data_map_org := MakeFullContentsMap2(repositoryData)
  // build a flat set
  data_map := mapset.NewThreadUnsafeSet[CompIndexOffet]()
  for _, sets := range data_map_org {
    for entry := range sets.Iter() {
      data_map.Add(entry)
    }
  }

  // count single occurrences in data tables
  size_singles := 0
  count_singles := 0
  for data_blob, sets := range data_map_org {
    if sets.Cardinality() == 1 {
      count_singles++
      size_singles += int(repositoryData.IndexHandle[data_blob].size)
    }
  }

  // report 3
  Printf("\n")
  Printf("%-25s %10d\n", "all content", len(data_map_org))
  Printf("%-25s %10d\n", "all content variations", data_map.Cardinality())
  Printf("%-25s %10d  %10.1f MiB\n", "singles in content", count_singles,
    float64(size_singles)/ONE_MEG)

  // check if repository needs pruning: more meta blobs in Index(), compared
  // to the union of all trees
  // full tree is mapped to repositoryData.FullPath
  // index is mapped repositoryData.IndexHandle
  set_index_handle := mapset.NewThreadUnsafeSet[IntID]()
  for _, ih := range repositoryData.IndexHandle {
    if ih.Type == restic.TreeBlob {
      set_index_handle.Add(ih.blob_index)
    }
  }
  set_index_handle.RemoveAll(EMPTY_NODE_ID_TRANSLATED)

  set_tree := mapset.NewThreadUnsafeSet[IntID]()
  for meta_blob_int := range repositoryData.FullPath {
    set_tree.Add(meta_blob_int)
  }
  set_tree.RemoveAll(EMPTY_NODE_ID_TRANSLATED)

  var diff mapset.Set[IntID]
  var l_diff int
  if !set_index_handle.Equal(set_tree) {
    Printf("\n*** Possibly needs pruning. data blobs only checked.***\n")
    Printf("from index %7d\n", set_index_handle.Cardinality())
    Printf("from tree  %7d\n", set_tree.Cardinality())

    diff = set_tree.Difference(set_index_handle)
    l_diff = diff.Cardinality()
    if l_diff > 1 {
      Printf("tree  has %5d more records than the index_records\n", l_diff)
      panic("\n*** This is a catastrophic inconsistency in restic management!! ***")
    }

    diff = set_index_handle.Difference(set_tree)
    l_diff = diff.Cardinality()
    if l_diff < 10 {
      Printf("difference %+v\n", diff)
    } else {
      Printf("Too many differences!\n")
    }

    if options.Prune {
      CheckPrune(repositoryData, diff, options)
    }
  }

  if options.Timing {
    timeMessage(options.MemoryUse, "%-30s %10.1f seconds\n", "CountTables",
      time.Now().Sub(start).Seconds())
  }
}

// find snapshots which own the same tree (root)
func FindSameTree(ctx context.Context, repo restic.Repository,
  repositoryData *RepositoryData) {
  printed := false
  // map tree root to snap_id
  double := map[restic.ID]mapset.Set[string]{}
  for snap_id, sn := range repositoryData.SnapMap {
    if _, ok := double[sn.Tree]; !ok {
      double[sn.Tree] = mapset.NewThreadUnsafeSet[string]()
    }
    double[sn.Tree].Add(snap_id)
  }

  // check for set length > 1
  for _, snap_id_set := range double {
    if snap_id_set.Cardinality() == 1 {
      continue
    }

    // multiple owners
    if !printed {
      printed = true
      Printf("\n*** Multiple snapshots for same tree ***\n")
    }

    // collect duplicate snapshots, sort and print
    owners := make([]SnapshotWpl, 0, snap_id_set.Cardinality())
    for snap_id := range snap_id_set.Iter() {
      owners = append(owners, repositoryData.SnapMap[snap_id])
    }
    sort.SliceStable(owners, func(i, j int) bool {
      return owners[i].Time.Before(owners[j].Time)
    })

    for _, sn := range owners {
      Printf("%s - %s %s:%s\n", sn.ID.Str(), sn.Time.Format(time.DateTime), sn.Hostname,
        sn.Paths[0])
    }
  }
}

// check for Prune: 'meta_diff' only contains meta_blob records
func CheckPrune(repositoryData *RepositoryData, meta_diff mapset.Set[IntID],
  options RepoDetailsOptions) {

  // diff set length is zero, which means that the two sets are equal.
  if meta_diff.Cardinality() == 0 {
    return
  }

  // need to copy fullpath, so we can work on the tree before it gets amended!
  fullpath := map[IntID]string{}
  for k, v := range repositoryData.FullPath {
    fullpath[k] = v
  }

  // get all data blobs from Index()
  set_data_handle := mapset.NewThreadUnsafeSet[IntID]()
  for _, ih := range repositoryData.IndexHandle {
    if ih.Type == restic.DataBlob {
      set_data_handle.Add(ih.blob_index)
    }
  }

  // 2. - find all data blobs from the mapped tree blobs - as in used blobs
  set_data_tree := mapset.NewThreadUnsafeSet[IntID]()
  for meta_blob_int := range fullpath {
    for _, meta := range repositoryData.DirectoryMap[meta_blob_int] {
      if meta.Type == "file" {
        set_data_tree.Append(meta.content...)
      }
    }
  }

  // 3. - in order to make details useful for blobs which are marked "to be deleted"
  //      but are not pruned yet, one has to discover the Lost meta blobs
  diff_data := set_data_handle.Difference(set_data_tree)
  unused_blobs := diff_data.Union(meta_diff)
  SizePrune(repositoryData, unused_blobs, false, nil, options.Detail)

  // 4. - report details if requested
  Detail := options.Detail
  data_map := map_data_blob_file(repositoryData)
  if Detail == 4 {
    Print_very_raw(repositoryData, unused_blobs)
  } else if Detail == 3 {
    Print_raw(repositoryData, unused_blobs, data_map)
  } else if Detail == 2 || Detail == 1 {
    Print_some_detail(repositoryData, unused_blobs, Detail, options.Lost, data_map)
  }
}

/*
// serialize 'content' step by step into sha256 by Write-ing() to it
func ConvertContent(content []IntID) (result restic.ID) {
  sha256sum := sha256.New()
  temp := make([]byte, 4)
  for _, data_blob_int := range content {
    //binary.LittleEndian.PutUint32(temp, data_blob_int): serialize uint32
    temp[0] = byte(data_blob_int)
    temp[1] = byte(data_blob_int >> 8)
    temp[2] = byte(data_blob_int >> 16)
    temp[3] = byte(data_blob_int >> 24)
    sha256sum.Write(temp)
  }

  // copy final target to [sha256.Size]byte,
  // type 'sha256sum.Sum(nil)' is []byte
  // 'copy' can copy between different types
  copy(result[:], sha256sum.Sum(nil))
  return result
}
*/
