package main
// compile with "go run build.go -tags debug""
// run with DEBUG_LOG=/home/wplapper/restic/debug.log fully-qualified-name/restic -r <repo> overview

// ref to onedrive: rclone:onedrive:restic_backups

import (
	// system
	"time"
	"sort"

	//argparse
	"github.com/spf13/cobra"

	// restic library
	"github.com/wplapper/restic/library/restic"

	// mapset
	"github.com/deckarep/golang-set/v2"
)

type TRemoveOptions struct {
		cutoff int
		snaps []string
		detail bool
		timing bool
}

type Pack_and_size struct {
		Size    int
		PackID  restic.IntID
}

type comp_index struct {
	meta_blob restic.IntID
	position int
}

var tremoveOptions TRemoveOptions

var cmdTRemove = &cobra.Command{
	Use:   "tremove [flags]",
	Short: "test temove one or more snapshots from the repo",
	Long: `
test temove one or more snapshots from the repo.
Print repackaging information.

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
	flags.BoolVarP(&tremoveOptions.detail, "detail", "D", false, "print dir/file details")
	flags.BoolVarP(&tremoveOptions.timing, "timing", "T", false, "produce timings")
}

func runTRemove(gopts GlobalOptions, args []string) error {
	// setup global data
	repositoryData := init_repositoryData()
	EMPTY_NODE_ID = restic.Hash([]byte("{\"nodes\":[]}\n"))
	gOptions = gopts

	// analyse cutoff date
	cutoff := tremoveOptions.cutoff
	detail := tremoveOptions.detail

	// step 1: open repository
	start := time.Now()
	repo, err := OpenRepository(gopts)
	if err != nil {
		return err
	}
	if tremoveOptions.timing {
		timeMessage("%-30s %10.1f seconds\n", "open repository",
			time.Now().Sub(start).Seconds())
	}

	// step 2: gather all snapshots
	//start = time.Now()
	repositoryData.snaps, err = GatherAllSnapshots(gopts, repo)
	if err != nil {
			return err
	}
	repositoryData.snap_map = make(map[string]*restic.Snapshot)
	for _, sn := range repositoryData.snaps {
		repositoryData.snap_map[sn.ID().Str()] = sn
	}

	// make a set from the input parameters (if any)
	snaps_from_cli := mapset.NewSet[string]()
	for _, snap_id := range args {
		snaps_from_cli.Add(snap_id)
	}

	// step 3: compare against cutoff date
	now := time.Now()
	snaps_to_be_deleted := make([]*restic.Snapshot, 0, 10)
	Printf("snapshots selected for deletion\n")
	for _, sn := range repositoryData.snaps {
		days := int(now.Sub(sn.Time).Seconds() / 86400)
		ok := snaps_from_cli.Contains(sn.ID().Str())
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
	if tremoveOptions.timing {
		timeMessage("%-30s %10.1f seconds\n", "selected all snapshots",
			time.Now().Sub(start).Seconds())
	}
	Printf("++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n\n")

	// step 4: manage Index Records
	// start = time.Now()
	if err = HandleIndexRecords(gopts, repo, repositoryData); err != nil {
		return err
	}
	if tremoveOptions.timing {
		timeMessage("%-30s %10.1f seconds\n", "HandleIndexRecords",
			time.Now().Sub(start).Seconds())
	}

	//start = time.Now()
	GatherAllRepoData(gopts, repo, repositoryData)
	if tremoveOptions.timing {
		timeMessage("%-30s %10.1f seconds\n", "GatherAllRepoData",
			time.Now().Sub(start).Seconds())
	}

	// calculate data_map once
	var data_map map[restic.IntID]comp_index
	if detail {
		data_map = map_data_blob_file(repositoryData)
	}

	// step 5: gather size info for each blob
	// collect all blobs which belong to the same pack
	//start = time.Now()
	blobs_per_packID := make(map[restic.IntID]restic.IntSet)
	for _, ih := range repositoryData.index_handle {
		if _, ok := blobs_per_packID[ih.pack_index]; !ok {
			blobs_per_packID[ih.pack_index] = restic.NewIntSet()
		}
		// collect the blobs belonging to the same packfile
		blobs_per_packID[ih.pack_index].Insert(ih.blob_index)
	}
	if tremoveOptions.timing {
		timeMessage("%-30s %10.1f seconds\n", "generate pack info",
			time.Now().Sub(start).Seconds())
	}

	// step 6.1: loop over the snap_id's to be removed individually
	selected := make([]*restic.Snapshot, 1, 1)
	for _, sn := range snaps_to_be_deleted {
		selected[0] = sn
		Printf("\n*** snap_ID %s %s:%s at %s\n", sn.ID().Str(),
				sn.Hostname, sn.Paths[0], sn.Time.String()[:19])
		if err = CalculatePruneSize(selected, blobs_per_packID, repositoryData, data_map, detail); err != nil {
				return err
		}
	}

	// select all the snapshots for the summary record
	selected = make([]*restic.Snapshot, len(snaps_to_be_deleted))
	// create total summary by 'removing' all of snaps_to_be_deleted
	for ix, sn := range snaps_to_be_deleted {
		selected[ix] = sn
	}
	Printf("\n*** ALL ***\n")
	if err = CalculatePruneSize(selected, blobs_per_packID, repositoryData, nil, false); err != nil {
			return err
	}
	if tremoveOptions.timing {
		timeMessage("%-30s %10.1f seconds\n", "group summary",
			time.Now().Sub(start).Seconds())
	}
	return nil
}

func CalculatePruneSize(selected []*restic.Snapshot, blobs_per_packID map[restic.IntID]restic.IntSet,
repositoryData *RepositoryData, data_map map[restic.IntID]comp_index, detail bool) error {
	// step 1: find all meta- and data-blobs in given 'selected' snapshot
	used_blobs := restic.NewIntSet()
	for _, sn := range selected {
		id_ptr := Ptr2ID(*sn.ID(), repositoryData)
		// get the meta blobs
		used_blobs.Merge(repositoryData.meta_dir_map[id_ptr])
		// get the data blobs belonging to these snapshots
		for meta_blob := range repositoryData.meta_dir_map[id_ptr] {
			for _, meta := range repositoryData.directory_map[meta_blob] {
				for _, cont := range meta.content {
						used_blobs.Insert(cont)
				}
			}
		}
	}

	// step 6: build the tree list for all other snapshots
	all_other_snapshots := make([]*restic.Snapshot, 0, len(repositoryData.snaps))
	//all_other_blobs := restic.NewIntSet()
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
		all_other_snapshots = append(all_other_snapshots, sn)
	}

	// continually take away from 'used_blobs' while using 'all_other_snapshots'
	for _, sn := range all_other_snapshots {
		id_ptr := Ptr2ID(*sn.ID(), repositoryData)
		for meta_blob := range repositoryData.meta_dir_map[id_ptr] {
			used_blobs.Delete(meta_blob)
			for _, meta := range repositoryData.directory_map[meta_blob] {
				for _, cont := range meta.content {
					used_blobs.Delete(cont)
				}
			}
		}
	}

	// step 7: calcuate which IDs are unique to this snapshot,
	// prepare calculation for bobs to be removed or moved to different packs
	//unique_blobs := used_blobs.Sub(all_other_blobs)
	delete_packs := make(map[restic.IntID]restic.IntSet, len(used_blobs))
	// map these blobs back to pack IDs
	for blob := range used_blobs {
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
	size_delete_blobs   := 0
	size_repack_blobs   := 0
	size_partial_blobs  := 0
	for pack_index := range delete_packs {
		if len(delete_packs[pack_index]) == len(blobs_per_packID[pack_index]) {
			// straight delete!
			count_delete_packs++
			// get sizes
			for blob := range delete_packs[pack_index] {
				back_to_ID := repositoryData.index_to_blob[blob]
				size_delete_blobs += repositoryData.index_handle[back_to_ID].size
				count_delete_blobs++
			}
		} else {
			// needs repacking, get sizes and counts
			for blob := range blobs_per_packID[pack_index] {
				back_to_ID := repositoryData.index_to_blob[blob]
				size_repack_blobs += repositoryData.index_handle[back_to_ID].size
				count_repack_blobs++
			}
			for blob := range delete_packs[pack_index] {
				back_to_ID := repositoryData.index_to_blob[blob]
				size_partial_blobs += repositoryData.index_handle[back_to_ID].size
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

	if !detail {
		return nil
	}

	// gather detail of deleted directories and files
	deleted_files := mapset.NewSet[string]()
	for blob := range used_blobs {
		filename := ""
		ih := repositoryData.index_handle[repositoryData.index_to_blob[blob]]
		if ih.Type == restic.TreeBlob {
			if repositoryData.fullpath[blob] == "/" {
				filename = "/"
			} else {
				filename = repositoryData.fullpath[blob] + "/"
			}
		} else if ih.Type == restic.DataBlob {
			cmp_ix := data_map[blob]
			name := repositoryData.directory_map[cmp_ix.meta_blob][cmp_ix.position].name
			filename = repositoryData.fullpath[cmp_ix.meta_blob] + "/" + name
		}
		deleted_files.Add(filename)
	}

	// convert deleted_files to Slice, so it can be sorted
	deleted_files_to_sort := make([]string, deleted_files.Cardinality())
	index := 0
	for filename := range deleted_files.Iter() {
		deleted_files_to_sort[index] = filename
		index++
	}

	sort.Strings(deleted_files_to_sort)
	for _,filename := range deleted_files_to_sort {
		Printf("%s\n", filename)
	}
	return nil
}

func map_data_blob_file(repositoryData *RepositoryData) map[restic.IntID]comp_index {
	// map data blobs back to meta_blob, position in directory_map
	// TODO: calcuate once!

	data_map := make(map[restic.IntID]comp_index)
	for meta_blob, file_list := range repositoryData.directory_map {
		for position, meta := range file_list {
			// generate composite index
			cmp_ix := comp_index{meta_blob: meta_blob, position: position}
			if meta.Type == "file" {
				for _,data_blob := range meta.content {
					data_map[data_blob] = cmp_ix
				}
			}
		}
	}
	return data_map
}
