package main
// compile with "go run build.go -tags debug""
// run with DEBUG_LOG=/home/wplapper/restic/debug.log fully-qualified-name/restic -r <repo> overview

// ref to onedrive: rclone:onedrive:restic_backups

import (
  // system
  "time"
  "os"
  //"runtime"
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
    PackID  restic.IntID
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
	repositoryData := init_repositoryData()
	EMPTY_NODE_ID = restic.Hash([]byte("{\"nodes\":[]}\n"))
  gOptions = gopts

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
  repositoryData.snaps = snaps

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
  err = HandleIndexRecords(gopts, repo, repositoryData)
  if err != nil {
    return err
  }
  //debug.Log("HandleIndexRecords done")
  timeMessage("%-30s %10.1f seconds\n", "HandleIndexRecords", time.Now().Sub(start).Seconds())

  start = time.Now()
  GatherAllRepoData(gopts, repo, repositoryData)
  timeMessage("%-30s %10.1f seconds\n", "GatherAllRepoData", time.Now().Sub(start).Seconds())

  // step 4.2: gather size info for each blob
  start = time.Now()
  blobs_from_ix    := make(map[restic.IntID]Pack_and_size)
  blobs_per_packID := make(map[restic.IntID]restic.IntSet)
  for _, data := range repositoryData.index_handle {
    blobs_from_ix[data.blob_index] = Pack_and_size{Size: data.size,
        PackID: data.pack_index}
    if len(blobs_per_packID[data.pack_index]) == 0 {
        blobs_per_packID[data.pack_index] = restic.NewIntSet()
    }
    blobs_per_packID[data.pack_index].Insert(data.blob_index)
  }
  if gopts.verbosity > 0 {
    Printf("%-30s %10.1f seconds\n", "generate pack info",
      time.Now().Sub(start).Seconds())
  }

  // loop over the snap_id's to be removed individually
  for _, sn := range snaps_to_be_deleted {
      selected := make([]*restic.Snapshot, 1, 1)
      selected[0] = sn
      Printf("\n*** snap_ID %s %s:%s at %s\n", sn.ID().Str(),
          sn.Hostname, sn.Paths[0], sn.Time.String()[:19])
      err = CalculatePruneSize(gopts, repo, selected, blobs_from_ix,
          blobs_per_packID, repositoryData)
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
  err = CalculatePruneSize(gopts, repo, selected, blobs_from_ix,
      blobs_per_packID, repositoryData)
  if err != nil {
      return err
  }
  return nil
}

func CalculatePruneSize(gopts GlobalOptions, repo restic.Repository,
selected []*restic.Snapshot, blobs_from_ix map[restic.IntID]Pack_and_size,
blobs_per_packID map[restic.IntID]restic.IntSet,
repositoryData *RepositoryData) error {
	//start := time.Now()

  // step 1: find all meta- and data-blobs in given 'selected' slice
  used_blobs := restic.NewIntSet()
  for _, sn := range selected {
    // get the meta blobs
    data, ok := repositoryData.meta_dir_map[*sn.ID()]
    if !ok {
      Printf("not in repo %v\n", sn.ID())
      return  nil
    }

    used_blobs.Merge(data)
    // get the data blobs
    for meta_blob := range repositoryData.meta_dir_map[*sn.ID()] {
      for _, meta := range repositoryData.directory_map[meta_blob] {
        used_blobs.Merge(meta.content)
      }
    }
  }

	// step 6: build the tree list for the rest of the snapshots
	rest_tree_list := make([]*restic.Snapshot, 0, len(repositoryData.snaps))
	for _, sn := range repositoryData.snaps {
		found := false
		for _, sn2 := range selected {
			if *sn.ID() == *sn2.ID() {
				found = true
				break
			}
		}
		if found {
			continue
		}
		rest_tree_list = append(rest_tree_list, sn)
	}

	all_other_blobs := restic.NewIntSet()
  for _, sn := range rest_tree_list {
    // get the meta blobs
    all_other_blobs.Merge(repositoryData.meta_dir_map[*sn.ID()])
    // get the data blobs
    for meta_blob := range repositoryData.meta_dir_map[*sn.ID()] {
      for _, meta := range repositoryData.directory_map[meta_blob] {
        if meta.Type != "file" {
          continue
        }
        all_other_blobs.Merge(meta.content)
      }
    }
  }

	// step 7: get the pack information
	unique_blobs := used_blobs.Sub(all_other_blobs)
	delete_packs := make(map[restic.IntID]restic.IntSet, len(unique_blobs))
	// map these blobs back to pack IDs
	for blob := range unique_blobs {
    back_to_ID := repositoryData.index_to_blob[blob]
		pack_index := repositoryData.index_handle[back_to_ID].pack_index
		//initialize
		if len(delete_packs[pack_index]) == 0 {
			delete_packs[pack_index] = restic.NewIntSet()
		}
		delete_packs[pack_index].Insert(blob)
	}

	// step 8: summarize
	count_delete_packs  := 0
	count_repack_blobs  := 0
	count_delete_blobs  := 0
	count_partial_blobs := 0
	size_delete_blobs   := uint64(0)
	size_repack_blobs   := uint64(0)
	size_partial_blobs  := uint64(0)
	for pack_index := range delete_packs {
		if len(delete_packs[pack_index]) == len(blobs_per_packID[pack_index]) {
			// straight delete!
			count_delete_packs++
			for blob := range delete_packs[pack_index] {
        back_to_ID := repositoryData.index_to_blob[blob]
				size_delete_blobs += uint64(repositoryData.index_handle[back_to_ID].size)
				count_delete_blobs++
			}
		} else {
			// needs repacking
			for blob := range blobs_per_packID[pack_index] {
				size_repack_blobs += uint64(blobs_from_ix[blob].Size)
				count_repack_blobs++
			}
			for blob := range delete_packs[pack_index] {
        back_to_ID := repositoryData.index_to_blob[blob]
				size_partial_blobs += uint64(repositoryData.index_handle[back_to_ID].size)
				count_partial_blobs++
			}
		}
	}

	Printf("straight delete %10d blobs %7d packs %10.3f Mib\n",
		count_delete_blobs, count_delete_packs, float64(size_delete_blobs) / ONE_MEG)
	Printf("this removes    %10d blobs %7s       %10.3f Mib\n",
		count_partial_blobs, " ", float64(size_partial_blobs) /ONE_MEG)
	Printf("repack          %10d blobs %7d packs %10.3f Mib\n",
		count_repack_blobs, len(delete_packs) - count_delete_packs,
		float64(size_repack_blobs) /ONE_MEG)
	Printf("total prune     %10d blobs %7s       %10.3f Mib\n",
		count_partial_blobs + count_delete_blobs, " ",
		float64(size_partial_blobs + size_delete_blobs) /ONE_MEG)

	if gopts.verbosity > 0 {
		//Printf("%-30s %10.1f seconds\n", "selected all blobs",
		//	time.Now().Sub(start).Seconds())
		PrintMemUsage()
	}
	return nil
}
