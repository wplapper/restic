package main
// compile with "go run build.go -tags debug""
// run with DEBUG_LOG=/home/wplapper/restic/debug.log fully-qualified-name/restic -r ... consistency


import (
  // system
  "time"
  "strings"

  //argparse
  "github.com/spf13/cobra"

  // restic library
  "github.com/wplapper/restic/library/restic"
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

	gOptions = gopts
  // step 1: open repository
  start := time.Now()
  repo, err := OpenRepository(gopts)
  if err != nil {
    return err
  }
  timeMessage("%-30s %10.1f seconds\n", "open repository", time.Now().Sub(start).Seconds())

  zeroes, err := restic.ParseID(strings.Repeat("deadbeaf", 8))
  if err != nil {
      Printf("Faied! Raised error %v\n", err)
      return err
  }
  Printf("zeroes %v\n", zeroes)

  // step 2: manage Index Records
  start = time.Now()
  err = HandleIndexRecords(gopts, repo)
  if err != nil {
    return err
  }
  timeMessage("%-30s %10.1f seconds\n", "read all index records", time.Now().Sub(start).Seconds())

  // step 3: gather all snapshots
  start = time.Now()
  snaps := make([]*restic.Snapshot, 0, 10)
  snaps, err = GatherAllSnapshots(gopts, repo)
  if err != nil {
      return err
  }
  master_tree_list := make([]restic.ID, 0, len(snaps))
  for _, sn := range snaps {
    master_tree_list = append(master_tree_list, *sn.Tree)
  }

  if gopts.verbosity > 0 {
    Printf("%-30s %10.1f seconds\n", "read all snapshots",
      time.Now().Sub(start).Seconds())
  }

  // step 4: gather all IDs from Index
  start = time.Now()
  blobs_from_ix := restic.NewIDSet()
  for blob := range repo.Index().Each(gopts.ctx) {
    blobs_from_ix.Insert(blob.ID)
  }

  if gopts.verbosity > 0 {
    Printf("%-30s %10.1f seconds\n", "get all blobs from index",
        time.Now().Sub(start).Seconds())
  }

  // step 5: get all used blobs from 'FindUsedBlobs'
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
  if gopts.verbosity > 0 {
    Printf("%-30s %10.1f seconds\n", "get blobs from trees",
        time.Now().Sub(start).Seconds())
  }

  // step 6: compare the sets
  if blobs_from_ix.Equals(all_blobs_from_trees) {
      Printf("perfect match!\n")
      return nil
  }
  Printf("Check the mess :(\n")
  Printf("len blobs_from_ix        %7d\n", len(blobs_from_ix))
  Printf("len all_blobs_from_trees %7d\n", len(all_blobs_from_trees))
  return nil
}

