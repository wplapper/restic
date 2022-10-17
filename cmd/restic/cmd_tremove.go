package main
// compile with "go run build.go -tags debug""
// run with DEBUG_LOG=/home/wplapper/restic/debug.log fully-qualified-name/restic -r <repo> overview

// ref to onedrive: rclone:onedrive:restic_backups

import (
  // system
  "time"
  "os"
  "runtime"
  "runtime/pprof"

  //argparse
  "github.com/spf13/cobra"

  // restic library
  "github.com/wplapper/restic/library/restic"
  "github.com/wplapper/restic/library/debug"
)

type TRemoveOptions struct {
    cutoff int
    snaps []string
}

type Pack_and_size struct {
    Size    uint
    PackID  restic.ID
}

//type restic.intID restic.restic.intID

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
  flags.IntVarP(&tremoveOptions.cutoff, "cutoff", "U", 182, "cutoff snaps which are older than <cutoff> days")
}

func runTRemove(gopts GlobalOptions, args []string) error {
  // analyse cutoff date
  cutoff := tremoveOptions.cutoff
  //Print("GlobalOptions %v\n", gopts)
  if globalOptions.cpuprofile != "" {
    f, err := os.Create(globalOptions.cpuprofile)
    if err != nil {
        Printf("could not create CPU profile: %v\n", err)
        return err
    }
    defer f.Close() // error handling omitted for example
    if err := pprof.StartCPUProfile(f); err != nil {
        Printf("could not start CPU profile: %v\n", err)
        return err
    }
    Printf("CPU sampling started\n")
    defer pprof.StopCPUProfile()
  }

  // step 1: open repository
  debug.Log("Start tremove")
  start := time.Now()
  repo, err := OpenRepository(gopts)
  if err != nil {
    return err
  }
  if gopts.verbosity > 0 {
    Printf("%-30s %10.1f seconds\n", "open repository",
        time.Now().Sub(start).Seconds())
  }
  debug.Log("repo open")

  // step 2: gather all snapshots
  start = time.Now()
  snaps := make([]*restic.Snapshot, 0, 10)
  // snaps is []*restic.Snapshot
  snaps, err = GatherAllSnapshots(gopts, repo)
  if err != nil {
      return err
  }
  debug.Log("read snapshots")

  // make the master tree list
  master_tree_list := make([]restic.ID, 0, len(snaps))
  for _, sn := range snaps {
    master_tree_list = append(master_tree_list, *sn.Tree)
  }

  // make a master snap dictiopnary (a map)
  master_snapID_set := make(map[string]struct{}, len(snaps))
  for _, snap_id := range args {
    master_snapID_set[snap_id] = struct{}{}
  }

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
  if gopts.verbosity > 0 {
    Printf("%-30s %10.1f seconds\n", "selected all snapshots",
      time.Now().Sub(start).Seconds())
  }
  Printf("++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n\n")

  // step 4.1: manage Index Records
  start = time.Now()
  err = HandleIndexRecords(gopts, repo)
  if err != nil {
    return err
  }
  debug.Log("HandleIndexRecords done")
  type pack_and_size struct {
      size    uint
      packID  restic.ID
  }

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
  if gopts.verbosity > 0 {
    Printf("%-30s %10.1f seconds\n", "read all index records",
      time.Now().Sub(start).Seconds())
  }

  // loop over the snap_id's to be removed individually
  for _, sn := range snaps_to_be_deleted {
      selected := make([]*restic.Snapshot, 1, 1)
      selected[0] = sn
      Printf("\n*** snap_ID %s %s:%s at %s\n", sn.ID().Str(),
          sn.Hostname, sn.Paths[0], sn.Time.String()[:19])
      err = CalculatePruneSize(gopts, repo, selected, snaps, blobs_from_ix,
          blobs_per_packID)
      if err != nil {
          return err
      }
  }

  // select all the snapshots for the summary record
  selected := make([]*restic.Snapshot, 0, len(snaps_to_be_deleted))
  // create total summary by 'removing' all of snaps_to_be_deleted
  for _, sn := range snaps_to_be_deleted {
    selected = append(selected, sn)
  }
  Printf("\n*** ALL ***\n")
  err = CalculatePruneSize(gopts, repo, selected, snaps, blobs_from_ix,
      blobs_per_packID)
  if err != nil {
      return err
  }

  if gopts.memprofile != "" {
      f, err := os.Create(gopts.memprofile)
      if err != nil {
          Printf("could not create memory profile: %v\n", err)

      }
      defer f.Close() // error handling omitted for example
      runtime.GC() // get up-to-date statistics
      if err := pprof.WriteHeapProfile(f); err != nil {
          Printf("could not write memory profile: %v\n", err)
      }
  }
  return nil
}
