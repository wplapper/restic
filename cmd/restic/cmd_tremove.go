package main
// compile with "go run build.go -tags debug""
// run with DEBUG_LOG=/home/wplapper/restic/debug.log fully-qualified-name/restic -r <repo> overview

// ref to onedrive: rclone:onedrive:restic_backups

import (
  // system
  "time"
  //"sort"
  //"strings"

  //argparse
  "github.com/spf13/cobra"

  // restic library
  "github.com/wplapper/restic/library/restic"
  //"github.com/wplapper/restic/library/repository"
)

type TRemoveOptions struct {
    cutoff int
    snaps []string
}

type Pack_and_size struct {
    Size    uint
    PackID  restic.ID
}

var tremoveOptions TRemoveOptions
const ONE_MEG = float64(1024.0 * 1024.0)

var cmdTRemove = &cobra.Command{
  Use:   "tremove [flags]",
  Short: "test temove one or more snampshots from the repo",
  Long: `
test temove one or more snampshots from the repo.

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
  DisableAutoGenTag: true,
  RunE: func(cmd *cobra.Command, args []string) error {
    return runTRemove(globalOptions, args)
  },
}

func init() {
  cmdRoot.AddCommand(cmdTRemove)
  flags := cmdTRemove.Flags()
  flags.IntVarP(&tremoveOptions.cutoff, "cutoff", "C", 182, "cutoff snaps which are older than <cutoff> days")
  //flags.StringArrayVar(&tremoveOptions.snaps, "snaps", nil, "consider these snapshots (can be specified multiple times)")
}

func runTRemove(gopts GlobalOptions, args []string) error {
  // analyse cutoff date
  cutoff := tremoveOptions.cutoff

  // step 1: open repository
  start := time.Now()
  repo, err := OpenRepository(gopts)
  if err != nil {
    return err
  }
  Printf("open repository %10.1f seconds.\n", time.Now().Sub(start).Seconds())

  // step 2: gather all snapshots
  start = time.Now()
  snaps := make([]*restic.Snapshot, 0, 10)
  // snaps is []*restic.Snapshot
  snaps, err = GatherAllSnapshots(gopts, repo, snaps)
  if err != nil {
      return err
  }
  master_tree_list := make([]restic.ID, 0, len(snaps))
  for _, sn := range snaps {
    master_tree_list = append(master_tree_list, *sn.Tree)
  }

  master_snapID_set := make(map[string]struct{})
  for _, snap_id := range args {
    master_snapID_set[snap_id] =struct{}{}
  }
  Printf("tremoveOptions.snaps %v\n", tremoveOptions.snaps)
  Printf("master_snapID_set %v\n", master_snapID_set)
  //Printf("read %d snapshot records in %10.1f seconds\n", len(snaps),
  //  time.Now().Sub(start).Seconds())

  // step 3: compare against cutoff date
  now := time.Now()
  snaps_to_be_deleted := make([]*restic.Snapshot, 0, 10)
  Printf("snapshots selected for deletion\n")
  for _, sn := range snaps {
      days := int(now.Sub(sn.Time).Seconds() / 86400)
      _, ok := master_snapID_set[sn.ID().Str()]
      if  days >= cutoff || ok {
          Printf("snap_ID %s %d days old %s:%s at %s\n", sn.ID().Str(),
            days, sn.Hostname, sn.Paths[0], sn.Time.String()[:19])
          snaps_to_be_deleted = append(snaps_to_be_deleted, sn)
      }
  }
  if len(snaps_to_be_deleted) == 0 {
      Printf("No snapshots selected. Terminating.\n")
      return nil
  }
  Printf("++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n\n")

  // step 4.1: manage Index Records
  start = time.Now()
  err = HandleIndexRecords(gopts, repo)
  if err != nil {
    return err
  }
  type pack_and_size struct {
      size    uint
      packID  restic.ID
  }
  //Printf("pure HandleIndexRecords in %10.1f seconds\n",
  //    time.Now().Sub(start).Seconds())

  // step 4.2: gather size info for each blob
  blobs_from_ix    := make(map[restic.ID]Pack_and_size)
  blobs_per_packID := make(map[restic.ID]restic.IDSet)
  for blob := range repo.Index().Each(gopts.ctx) {
    blobs_from_ix[blob.ID] = Pack_and_size{Size: blob.Length,
        PackID: blob.PackID}
    if len(blobs_per_packID[blob.PackID]) == 0 {
        blobs_per_packID[blob.PackID] = restic.NewIDSet()
    }
    blobs_per_packID[blob.PackID].Insert(blob.ID)
  }
  Printf("read all index records in %10.1f seconds\n",
      time.Now().Sub(start).Seconds())


  // loop over the snap_id's to be removed individually
  for _, sn := range snaps_to_be_deleted {
      selected := make([]*restic.Snapshot, 1, 1)
      selected[0] = sn
      Printf("\n*** snap_ID %s %s:%s at %s\n", sn.ID().Str(),
          sn.Hostname, sn.Paths[0], sn.Time.String()[:19])
      err = CalculatePruneSize(gopts, repo, selected, snaps, blobs_from_ix, blobs_per_packID)
      if err != nil {
          return err
      }
  }

  selected := make([]*restic.Snapshot, 0, len(snaps_to_be_deleted))
  // create total summary by 'removing' all of snaps_to_be_deleted
  for _, sn := range snaps_to_be_deleted {
    selected = append(selected, sn)
  }
  Printf("\n*** ALL ***\n")
  err = CalculatePruneSize(gopts, repo, selected, snaps, blobs_from_ix, blobs_per_packID)
  if err != nil {
      return err
  }
  return nil
}

func CalculatePruneSize(gopts GlobalOptions, repo restic.Repository,
  selected []*restic.Snapshot, snaps []*restic.Snapshot,
  blobs_from_ix map[restic.ID]Pack_and_size,
  blobs_per_packID map[restic.ID]restic.IDSet) error {

    // step 5: find blobs for the selected snaps
    //start := time.Now()
    usedBlobs_delete := restic.NewBlobSet()
    tree_list := make([]restic.ID, 0, len(selected))
    for _, sn := range selected {
      tree_list = append(tree_list, *sn.Tree)
    }
    err := restic.FindUsedBlobs(gopts.ctx, repo, tree_list, usedBlobs_delete, nil)
    if err != nil {
      return err
    }

    // step 6: buid the tree ist for the rest of the snapshots
    rest_tree_list := make([]restic.ID, 0, len(snaps) - len(selected))
    for _, sn := range snaps {
      found := false
      for _, sn2 := range selected {
          if sn == sn2 {
              found = true
              break
          }
      }
      if found {
          continue
      }
      rest_tree_list = append(rest_tree_list, *sn.Tree)
    }

    usedBlobs_keep := restic.NewBlobSet()
    err = restic.FindUsedBlobs(gopts.ctx, repo, rest_tree_list, usedBlobs_keep, nil)
    if err != nil {
      return err
    }

    // step 7: get the pack information
    unique_blobs := usedBlobs_delete.Sub(usedBlobs_keep)

    del_packs := make(map[restic.ID]restic.IDSet)
    // map these blobs back to packs
    for blob := range unique_blobs {
      packID := blobs_from_ix[blob.ID].PackID
      //initialize
      if len(del_packs[packID]) == 0 {
          del_packs[packID] = restic.NewIDSet()
      }
      del_packs[packID].Insert(blob.ID)
    }

    count_repack      := 0
    count_delpack     := 0
    count_repack_blobs := 0
    count_del_blobs   := 0
    count_prune_blobs := 0
    size_delete       := uint64(0)
    size_repack       := uint64(0)
    size_prune        := uint64(0)
    for PackID := range del_packs {
        if len(del_packs[PackID]) == len(blobs_per_packID[PackID]) {
            // straight delete!
            count_delpack++
            for blob := range del_packs[PackID] {
                size_delete += uint64(blobs_from_ix[blob].Size)
                count_del_blobs++
            }
        } else {
            // needs repacking
            count_repack++
            for blob := range blobs_per_packID[PackID] {
                size_repack += uint64(blobs_from_ix[blob].Size)
                count_repack_blobs++
            }
            for blob := range del_packs[PackID] {
                size_prune += uint64(blobs_from_ix[blob].Size)
                count_prune_blobs++
            }
        }
    }
    affected_packs := len(del_packs) - count_delpack
    Printf("straight delete %10d blobs %7d packs %10.3f Mib\n",
      count_del_blobs, count_delpack, float64(size_delete) / ONE_MEG)
    Printf("this removes    %10d blobs %7d packs %10.3f Mib\n",
      count_prune_blobs, len(del_packs), float64(size_prune) /ONE_MEG)
    Printf("repack          %10d blobs %7d packs %10.3f Mib\n",
      count_repack_blobs, affected_packs, float64(size_repack) /ONE_MEG)

    count_meta_blobs := 0
    count_data_blobs := 0
    size_meta_blobs  := uint64(0)
    size_data_blobs  := uint64(0)
    for blob := range unique_blobs {
        if blob.Type == restic.DataBlob {
            count_data_blobs++
            size_data_blobs += uint64(blobs_from_ix[blob.ID].Size)
        } else if blob.Type == restic.TreeBlob {
            count_meta_blobs++
            size_meta_blobs += uint64(blobs_from_ix[blob.ID].Size)
        }
    }
    Printf("meta  %7d blobs %10.3f MiB\n", count_meta_blobs,
        float64(size_meta_blobs) / ONE_MEG)
    Printf("data  %7d blobs %10.3f MiB\n", count_data_blobs,
        float64(size_data_blobs) / ONE_MEG)
    Printf("total %7d blobs %10.3f MiB\n", count_meta_blobs + count_data_blobs,
        float64(size_meta_blobs + size_data_blobs) / ONE_MEG)
    return nil
}
