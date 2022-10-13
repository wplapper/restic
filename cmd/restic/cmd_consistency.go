package main
// compile with "go run build.go -tags debug""
// run with DEBUG_LOG=/home/wplapper/restic/debug.log fully-qualified-name/restic -r ... consistency


import (
  // system
  "time"
  "sort"
  "context"
  "runtime"
  "fmt"

  //argparse
  "github.com/spf13/cobra"

  // restic library
  "github.com/wplapper/restic/library/restic"
  "github.com/wplapper/restic/library/repository"
  "github.com/wplapper/restic/library/walker"
  "github.com/wplapper/restic/library/debug"
)

var cmdConsistency = &cobra.Command{
  Use:   "consistency [flags]",
  Short: "check consistency of the repository",
  Long: `
check consistency of the repository.

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
  DisableAutoGenTag: true,
  RunE: func(cmd *cobra.Command, args []string) error {
    return runConsistency(globalOptions)
  },
}

func init() {
  //PrintMemUsage()
  cmdRoot.AddCommand(cmdConsistency)
}

func runConsistency(gopts GlobalOptions) error {

  // we need to encode the resut of an empty directory, for later comparison
  // step 1: we need the blob ID of the empty directory
  EMPTY_NODE_ID := restic.Hash([]byte("{\"nodes\":[]}\n"))
  EMPTY_NODE_SET := restic.NewIDSet(EMPTY_NODE_ID)

  // step 2: open repository
  start := time.Now()
  repo, err := OpenRepository(gopts)
  if err != nil {
    return err
  }
  Printf("%-40s %10.1f seconds\n", "open repository",
    time.Now().Sub(start).Seconds())

  // step 3: manage Index Records
  start = time.Now()
  err = HandleIndexRecords(gopts, repo)
  if err != nil {
    return err
  }
  Printf("%-40s %10.1f seconds\n", "read all index records",
      time.Now().Sub(start).Seconds())

  // step 4: get rif of EMPTY_NODE_ID
  //delete(meta_blobs_from_ix, EMPTY_NODE_ID)

  // step 5: gather all snapshots
  start = time.Now()
  snaps := make([]*restic.Snapshot, 0, 10)
  snaps, err = GatherAllSnapshots(gopts, repo, snaps)
  if err != nil {
      return err
  }
  master_tree_list := make([]restic.ID, 0, len(snaps))
  for _, sn := range snaps {
    master_tree_list = append(master_tree_list, *sn.Tree)
  }

  Printf("%-40s %10.1f seconds\n", "read all snapshots",
    time.Now().Sub(start).Seconds())

  // step 6: gather all IDs from Index
  start = time.Now()
  blobs_from_ix := restic.NewIDSet()
  for blob := range repo.Index().Each(gopts.ctx) {
    blobs_from_ix.Insert(blob.ID)
  }
  blobs_from_ix.Sub(EMPTY_NODE_SET)
  Printf("%-40s %10.1f seconds\n", "get all blobs from index",
      time.Now().Sub(start).Seconds())

  // step 7: get all used blobs from 'FindUsedBlobs'
  start = time.Now()
  usedBlobs_keep := restic.NewBlobSet()
  err = restic.FindUsedBlobs(gopts.ctx, repo, master_tree_list, usedBlobs_keep, nil)
  if err != nil {
    return err
  }

  all_blobs_from_trees := restic.NewIDSet()
  for blobs := range usedBlobs_keep {
    all_blobs_from_trees.Insert(blobs.ID)
  }
  Printf("%-40s %10.1f seconds\n", "get blobs from trees",
      time.Now().Sub(start).Seconds())

  // step 8: compare the sets
  if blobs_from_ix.Equals(all_blobs_from_trees) {
      Printf("perfect match!\n")
      return nil
  }
  Printf("Check the mess :(\n")
  return nil
}

func GatherAllSnapshots(gopts GlobalOptions, repo restic.Repository,
snaps []*restic.Snapshot) ([]*restic.Snapshot, error) {
  // collect all snap records
  repo.List(gopts.ctx, restic.SnapshotFile, func(id restic.ID, size int64) error {
      sn, err := restic.LoadSnapshot(gopts.ctx, repo, id)
      if err != nil {
          Printf("Skip snap record %s!\n", id)
          return err
      }
      snaps = append(snaps, sn)
      return nil
  })

  // now we can sort 'snaps': sort by sn.Time
  // sort is in-place!
  sort.SliceStable(snaps, func (i, j int) bool {
      return snaps[i].Time.Before(snaps[j].Time)
  })
  return snaps, nil
}

// processAllSnapshots runs through all snapshots and prints all directory names
// which have not been seen before.
func ProcessAllSnapshots(all_snaps []*restic.Snapshot, gopts GlobalOptions,
  repo restic.Repository) (map[*restic.ID]restic.IDSet,
                           map[*restic.ID]restic.IDSet, error) {

  // set operations on blobs
  seen_before := restic.NewIDSet()

  // define two dicts
  meta_dir := make(map[*restic.ID]restic.IDSet)
  data_dir := make(map[*restic.ID]restic.IDSet)

  ctx, cancel := context.WithCancel(gopts.ctx)
  defer cancel()

  // 'all_snap_IDs' is a slice of all snap_id's (as []string)
  all_snap_IDs := make([]string, 0, len(all_snaps))
  for _, sn := range all_snaps {
      all_snap_IDs = append(all_snap_IDs, sn.ID().Str())
  }

  // go over all snaps and call the 'Walker' for each snapshot
  for _, sn := range all_snaps {
    path := sn.Paths[0]
    if len(path) > 40 {
        path = path[:40]
    }
    Printf("snapshot: %s %-40s %-22s %s\n", sn.ID().Str(), path, sn.Hostname,
        sn.Time.String()[:19])

    // we need snap specific sets for snap_*Blobs
    snap_metaBlobs := restic.NewIDSet()
    snap_dataBlobs := restic.NewIDSet()

    // call walk.Walker
    debug.Log("calling Walker for snap %s", sn.ID().Str())
    seen_before_old := restic.NewIDSet()
    // copy the lot
    seen_before_old.Merge(seen_before)

    err := walker.Walk(ctx, repo, *sn.Tree, seen_before,
      func(parent_blob restic.ID, parent_nodepath string, node *restic.Node,
      err error) (bool, error) {
        // exclude errors, empty nodes and stuff we have handled before ...
        if err != nil {
          return false, err
        } else if node == nil { //|| seen_before.Has(parent_blob) {
            return false, nil
        }
        if ! snap_metaBlobs.Has(parent_blob) {
            snap_metaBlobs.Insert(parent_blob)
        }

        // gather dataBlobs from node.Content
        for _, blob := range node.Content {
            snap_dataBlobs.Insert(blob)
        }
        return false, nil
    })
    if err != nil {
        return nil, nil, err
    }
    //merge 'snap_metaBlobs' with 'seen_before'
    seen_before.Merge(snap_metaBlobs)
    //seen_after :=
    //Printf("seen_after length %d\n", len(seen_after))
    // create_meta_dir
    snap_metaBlobs.Merge(seen_before.Sub(seen_before_old))
    meta_dir[sn.ID()] = snap_metaBlobs
    data_dir[sn.ID()] = snap_dataBlobs
  }
  debug.Log("Finale")
  return meta_dir, data_dir, nil
}

func HandleIndexRecords(gopts GlobalOptions,
repo restic.Repository) error {
  // load index files and their contents
  // we need to load the Index first
  if err := repo.LoadIndex(gopts.ctx); err != nil {
    return err
  }

  repository.ForAllIndexes(gopts.ctx, repo, func(id restic.ID,
  idx *repository.Index, oldFormat bool, err error) error {
      if err != nil {
          return err
      }
      return nil
  })
  return nil
}

// PrintMemUsage outputs the current, total and OS memory being used.
// As well as the number of garbage collection cycles completed.
func PrintMemUsage() {
        var m runtime.MemStats
        runtime.ReadMemStats(&m)
        // For info on each, see: https://golang.org/pkg/runtime/#MemStats
        fmt.Printf("Alloc        = %v MiB", bToMb(m.Alloc))
        fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
        fmt.Printf("\tSys        = %v MiB", bToMb(m.Sys))
        fmt.Printf("\tNumGC      = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
    // full megabytes
    return b / 1024 / 1024
}
