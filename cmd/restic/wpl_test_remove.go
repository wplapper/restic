package main

// test remove a snapshot or mutiple snapshots using the repository.
// calculate the changes
// A lot is done here using set arithmetic. The meta-blobs and data-blob sets
// and their various differences are used to derive size information by using
// their compressed sizes - which are the important ones in the repository

import (
	// system
	"context"
	"encoding/json"
	"errors"
	"os"
	"sort"
	"strings"
	"time"

	//argparse
	"github.com/spf13/cobra"

	// restic library
	"github.com/wplapper/restic/library/restic"

	// mapset
	"github.com/deckarep/golang-set/v2"
)

type TRemoveOptions struct {
	Cutoff     int
	Snaps      []string
	Detail     int
	Timing     bool
	MemoryUse  bool
	Lost       bool
	Repacked   bool
	MultiRepo  string
	ConfigFile string
}

var tremoveOptions TRemoveOptions

var cmdTRemove = &cobra.Command{
	Use:   "wpl-remove [flags]",
	Short: "test remove one or more snapshots from the repository",
	Long: `test remove one or more snapshots from the repository.
Print repackaging information.

OPTIONS
=======
  - Cutoff: defaults to 270 days
  - Detail: prints file details of those blobs which are about to be removed
  - Lost:   checks if list files get replaced by newer versions of the file
  - repack: print lots about blobs to be Repacked
  - Timing: give some timings of the various subsections of code

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
	DisableAutoGenTag: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runTRemove(cmd.Context(), cmd, globalOptions, args)
	},
}

func init() {
	cmdRoot.AddCommand(cmdTRemove)
	f := cmdTRemove.Flags()
	f.IntVarP(&tremoveOptions.Cutoff, "cutoff", "C", 270, "cutoff Snaps which are older than <Cutoff> days")
	f.StringVarP(&tremoveOptions.MultiRepo, "multi-repo", "M", "", "base part of repositories local or onedrive")
	f.StringVarP(&tremoveOptions.ConfigFile, "config-file", "O", "/home/wplapper/restic/backup_config.json", "json config file")
	f.CountVarP(&tremoveOptions.Detail, "detail", "D", "print dir/file details")
	f.BoolVarP(&tremoveOptions.Lost, "lost", "L", false, "print Lost file details")
	f.BoolVarP(&tremoveOptions.Repacked, "repack", "R", false, "more info on repacks")
	f.BoolVarP(&tremoveOptions.Timing, "timing", "T", false, "produce timings")
	f.BoolVarP(&tremoveOptions.MemoryUse, "memory", "m", false, "produce memory usage")
}

func runTRemove(ctx context.Context, cmd *cobra.Command, gopts GlobalOptions, args []string) error {

	// setup global data
	gOptions = gopts

	if tremoveOptions.MultiRepo != "" {
		// quieten system talk
		globalOptions.Quiet = true
		globalOptions.verbosity = 0
	}

	// select action to take
	if tremoveOptions.MultiRepo != "" {
		DoMultiRepository(ctx, gopts, tremoveOptions)
	} else if gopts.Repo != "" && len(args) > 0 {
		DoOneRepositoryWithSnaps(ctx, gopts, tremoveOptions, args)
	} else {
		DoOneRepository(ctx, gopts, tremoveOptions, nil)
	}
	return nil
}

// calculate sizes for 'selected' snapshots
func CalculatePruneSize(selected []SnapshotWpl, repositoryData *RepositoryData,
	Detail int, Lost bool, Repacked bool, allSnapsAsSet mapset.Set[string],
	data_map map[IntID]mapset.Set[CompIddFile]) error {

	// step 1: find all meta- and data-blobs in given 'selected' snapshot
	all_other_snapshots := allSnapsAsSet.Clone() // = all_snapshots - selected
	for _, sn := range selected {
		all_other_snapshots.Remove(sn.ID.String())
	}

	// continually add to 'used_blobs' while using 'all_other_snapshots'.
	// all these blobs are still in use
	used_blobs := mapset.NewThreadUnsafeSet[IntID]()
	for snap_id := range all_other_snapshots.Iter() {
		id := repositoryData.SnapMap[snap_id].ID
		for meta_blob := range repositoryData.MetaDirMap[id].Iter() {
			used_blobs.Add(meta_blob)
			for _, meta := range repositoryData.DirectoryMap[meta_blob] {
				used_blobs.Append(meta.content...)
			}
		}
	}

	// gather all blobs and their total size
	all_blobs := mapset.NewThreadUnsafeSet[IntID]()
	for _, ih := range repositoryData.IndexHandle {
		all_blobs.Add(ih.blob_index)
	}

	// define the unused blobs
	unused_blobs := all_blobs.Difference(used_blobs)
	SizePrune(repositoryData, unused_blobs, Repacked, selected, Detail)

	if Detail == 4 {
		Print_very_raw(repositoryData, unused_blobs)
	} else if Detail == 3 {
		Print_raw(repositoryData, unused_blobs, data_map)
	} else if Detail == 2 || Detail == 1 {
		Print_some_detail(repositoryData, unused_blobs, Detail, Lost, data_map)
	}
	return nil
}

func Print_some_detail(repositoryData *RepositoryData, unused_blobs mapset.Set[IntID],
	Detail int, Lost bool, data_map map[IntID]mapset.Set[CompIddFile]) {
	// this is Detail = 1 / 2
	// gather Detail of deleted directories and files
	var cmp_ix CompIddFile
	var (
		filename string
		size     int
		repl     string
	)

	type file_info struct {
		size int
		repl string
	}

	reverse_fullpath := CreateReverseFullpath(repositoryData)
	deleted_files := map[string]file_info{}
	for blob := range unused_blobs.Iter() {
		ih := repositoryData.IndexHandle[repositoryData.IndexToBlob[blob]]
		if ih.Type == restic.TreeBlob && Detail > 1 {
			filename = repositoryData.FullPath[blob]
			deleted_files[filename] = file_info{size: 0}
		} else if ih.Type == restic.DataBlob {

			// in case of multiple entries this is an arbitrary choice!!
			cmp_ix = data_map[blob].ToSlice()[0]
			meta := repositoryData.DirectoryMap[cmp_ix.meta_blob][cmp_ix.position]
			name := meta.name
			size = int(meta.size)

			dir_name := repositoryData.FullPath[cmp_ix.meta_blob]
			if reverse_fullpath[dir_name].Cardinality() > 1 {
				filename = repositoryData.FullPath[cmp_ix.meta_blob] + "/" + name
				repl = "ok"
			} else {
				// no replacement available
				filename = repositoryData.FullPath[cmp_ix.meta_blob] + "/" + name
				repl = "--"
			}
			deleted_files[filename] = file_info{size: size, repl: repl}
		}
	}

	// gather keys from 'deleted_files'
	deleted_filenames_to_sort := make([]string, len(deleted_files))
	index := 0
	for filename := range deleted_files {
		deleted_filenames_to_sort[index] = filename
		index++
	}

	sort.Strings(deleted_filenames_to_sort)
	header_printed := false
	found_files := false
	for _, filename := range deleted_filenames_to_sort {
		comp := deleted_files[filename]
		size := comp.size
		repl := comp.repl
		if size > 0 {
			found_files = true
		}
		if !Lost {
			if size > 0 {
				if !header_printed {
					header_printed = true
					Printf("\n%10s %s %s\n", "size", "rp", "filename")
				}
				Printf("%10d %s %s\n", size, repl, filename)
			} else {
				if !header_printed {
					header_printed = true
					Printf("\n%10s %s %s\n", "size", "rp", "filename")
				}
				Printf("%13s %s\n", "", filename)
			}
		} else if size > 0 && repl == "--" && Lost {
			if !header_printed {
				header_printed = true
				Printf("\n%10s %s %s\n", "size", "rp", "filename")
			}
			Printf("%10d %s %s\n", size, repl, filename)
		}
	}

	if !header_printed && Lost && found_files {
		Printf("  All removed files have a newer version in the repository.\n")
	}
}

// print raw, but offset is ignored, so sorting is different
func Print_raw(repositoryData *RepositoryData, unused_blobs mapset.Set[IntID],
	data_map map[IntID]mapset.Set[CompIddFile]) {
	// gather some data for each of the 'unused_blobs'
	// type (tree/data), size (in bytes)
	// map data_blobs to meta_blob and position, hence fullpath plus basename
	type BlobInfo struct {
		blob      IntID
		Type      restic.BlobType
		size      int
		meta_blob IntID
		position  int
		blob_str  string
		path      string
	}

	to_be_sorted := make([]BlobInfo, 0, unused_blobs.Cardinality())
	for blob := range unused_blobs.Iter() {
		var raw_blob BlobInfo
		var cmp_ix CompIddFile
		ID := repositoryData.IndexToBlob[blob]
		ih := repositoryData.IndexHandle[ID]
		if ih.Type == restic.DataBlob {
			cmp_ix_set := data_map[blob]
			// in case of multiple entries this is an arbitrary choice!!
			cmp_ix = cmp_ix_set.ToSlice()[0]

			meta_blob := cmp_ix.meta_blob
			position := cmp_ix.position
			base := repositoryData.DirectoryMap[meta_blob][position].name
			raw_blob = BlobInfo{blob: blob, Type: ih.Type, size: ih.size,
				blob_str:  ID.String()[:12],
				meta_blob: meta_blob,
				position:  position,
				path:      repositoryData.FullPath[meta_blob] + "/" + base}
		} else {
			raw_blob = BlobInfo{blob: blob, Type: ih.Type, blob_str: ID.String()[:12],
				path: repositoryData.FullPath[blob] + "/"}
		}
		to_be_sorted = append(to_be_sorted, raw_blob)
	}

	// raw data collected, sort
	sort.SliceStable(to_be_sorted, func(i, j int) bool {
		if to_be_sorted[i].path < to_be_sorted[j].path {
			return true
		} else if to_be_sorted[i].path > to_be_sorted[j].path {
			return false
		} else if to_be_sorted[i].position < to_be_sorted[j].position {
			return true
		} else if to_be_sorted[i].position > to_be_sorted[j].position {
			return false
		} else {
			return to_be_sorted[i].blob_str < to_be_sorted[j].blob_str
		}
	})

	var path string
	Printf("\n%12s %8s %8s %s\n", "m/d blob", "size", "position", "path")
	for _, raw_blob := range to_be_sorted {
		if len(raw_blob.path) < 3 {
			path = "/"
		} else {
			path = raw_blob.path[2:]
		}
		if raw_blob.Type == restic.DataBlob {
			Printf("%s %8d %8d %s\n", raw_blob.blob_str, raw_blob.size,
				raw_blob.position, path)
		} else if raw_blob.Type == restic.TreeBlob {
			Printf("%s %8s %8s %s\n", raw_blob.blob_str, "", "", path)
		}
	}
}

// print full raw Detail
func Print_very_raw(repositoryData *RepositoryData, unused_blobs mapset.Set[IntID]) {
	full_map := MakeFullContentsMap2(repositoryData)

	type BlobInfo struct {
		blob      IntID // blob ID from unused_blobs
		Type      restic.BlobType
		size      int
		meta_blob IntID // for a data blob there is triple (meta_blob, position, offset)
		position  int
		offset    int
		blob_str  string
		path      string
		mblob_str string
		pfile_str string
	}

	to_be_sorted := make([]BlobInfo, 0, unused_blobs.Cardinality())
	for blob := range unused_blobs.Iter() {
		var raw_blob BlobInfo

		// fetch any blob from full_map set
		blob_ID := repositoryData.IndexToBlob[blob]
		ih := repositoryData.IndexHandle[blob_ID]
		pack_ID_str := repositoryData.IndexToBlob[ih.pack_index].String()[:12]
		if data_sett, ok := full_map[blob_ID]; ok {
			// in case of multiple entries this is an arbitrary choice!!
			full_info := data_sett.ToSlice()[0]
			if ih.Type == restic.DataBlob {
				meta_blob := repositoryData.BlobToIndex[full_info.meta_blob]

				raw_blob = BlobInfo{blob: blob, Type: ih.Type, size: ih.size,
					meta_blob: meta_blob, position: full_info.position, offset: full_info.offset,
					blob_str:  blob_ID.String()[:12],
					mblob_str: full_info.meta_blob.String()[:12],
					path: repositoryData.FullPath[meta_blob] + "/" +
						repositoryData.DirectoryMap[meta_blob][full_info.position].name,
					pfile_str: pack_ID_str,
				}
			}
		} else if ih.Type == restic.TreeBlob {
			raw_blob = BlobInfo{blob: blob,
				Type: ih.Type, mblob_str: blob_ID.String()[:12],
				path:      repositoryData.FullPath[blob] + "/",
				pfile_str: pack_ID_str,
			}
		}
		to_be_sorted = append(to_be_sorted, raw_blob)
	}

	sort.SliceStable(to_be_sorted, func(i, j int) bool {
		if to_be_sorted[i].path < to_be_sorted[j].path {
			return true
		} else if to_be_sorted[i].path > to_be_sorted[j].path {
			return false
		} else if to_be_sorted[i].position < to_be_sorted[j].position {
			return true
		} else if to_be_sorted[i].position > to_be_sorted[j].position {
			return false
		} else {
			return to_be_sorted[i].offset < to_be_sorted[j].offset
		}
	})

	Printf("\n%12s %12s %12s %8s %8s %6s %s\n", "packfile", "meta blob", "data blob", "size",
		"position", "offset", "path")
	for _, raw_blob := range to_be_sorted {
		if raw_blob.Type == restic.DataBlob {
			Printf("%s %12s %s %8d %8d %6d %s\n", raw_blob.pfile_str, raw_blob.mblob_str,
				raw_blob.blob_str, raw_blob.size, raw_blob.position, raw_blob.offset, raw_blob.path)
		} else if raw_blob.Type == restic.TreeBlob {
			Printf("%12s %s %12s %8s %8s %6s %s\n", raw_blob.pfile_str, raw_blob.mblob_str,
				raw_blob.blob_str, "", "", "", raw_blob.path)
		}
	}
}

func PrintRepackInfo(repositoryData *RepositoryData,
	repack_blobs_meta mapset.Set[IntID], repack_blobs_data mapset.Set[IntID],
	selected []SnapshotWpl) {

	type SortPath struct {
		ID_str string
		path   string
		depth  int16
	}

	root_set := mapset.NewThreadUnsafeSet[IntID]()
	for _, sn := range selected {
		root_set.Add(repositoryData.BlobToIndex[sn.Tree])
	}

	pack_info := GetPackIDs(repositoryData)
	location_depth2 := []int32{}
	result := dfs(CreateAllChildren(repositoryData), root_set)

	output_slice := make([]SortPath, 0, len(result))
	for _, meta_blob := range result {
		out := repositoryData.FullPath[meta_blob]
		lcomp := len(strings.Split(out, "/"))
		if lcomp <= 2 {
			out = "/"
			location_depth2 = append(location_depth2, int32(len(output_slice)))
		}
		output_slice = append(output_slice, SortPath{path: out, depth: int16(lcomp),
			ID_str: repositoryData.IndexToBlob[meta_blob].String()[:12]})
	}
	location_depth2 = append(location_depth2, int32(len(output_slice)))

	lower_index := int32(0)
	for _, offset := range location_depth2[1:] {
		upper_index := offset
		to_be_sorted := output_slice[lower_index:upper_index]
		sort.SliceStable(to_be_sorted, func(i, j int) bool {
			return to_be_sorted[i].path < to_be_sorted[j].path
		})

		for _, elem := range to_be_sorted {
			if elem.depth == 2 {
				Printf("\n")
			}
			Printf("%2d %s %s\n", elem.depth, elem.ID_str, elem.path)
		}
		lower_index = upper_index
	}

	type SortableMoreMulti struct {
		meta_blob     restic.ID
		data_blob     restic.ID
		position      int
		offset        int
		pack_ID_str   string
		meta_blob_str string
		data_blob_str string
		name          string
		multi         int
		size          int
	}

	full_map := MakeFullContentsMap2(repositoryData)
	output_slice2 := make([]SortableMoreMulti, 0, repack_blobs_data.Cardinality())
	for data_blob := range repack_blobs_data.Iter() {
		// in case of multiple entries this is an arbitrary choice!!
		// a better concept would be a domain, e.g. based o the repositoryData.MetaDirMap[snap_id]
		// intersected with the meta_blobs which have to be Repacked
		multi := full_map[repositoryData.IndexToBlob[data_blob]].Cardinality()
		data_sett := full_map[repositoryData.IndexToBlob[data_blob]].ToSlice()[0]
		dblob := repositoryData.IndexToBlob[data_blob]
		output_slice2 = append(output_slice2, SortableMoreMulti{
			meta_blob:     data_sett.meta_blob,
			data_blob:     dblob,
			position:      data_sett.position,
			offset:        data_sett.offset,
			meta_blob_str: data_sett.meta_blob_str,
			data_blob_str: data_sett.data_blob_str,
			name: repositoryData.FullPath[repositoryData.BlobToIndex[data_sett.meta_blob]] +
				"/" + data_sett.name,
			multi:       multi,
			pack_ID_str: repositoryData.IndexToBlob[pack_info[data_blob]].String()[:12],
			size:        repositoryData.IndexHandle[dblob].size,
		})
	}

	sort.SliceStable(output_slice2, func(i, j int) bool {
		if output_slice2[i].meta_blob_str < output_slice2[j].meta_blob_str {
			return true
		} else if output_slice2[i].meta_blob_str > output_slice2[j].meta_blob_str {
			return false
		} else if output_slice2[i].name < output_slice2[j].name {
			return true
		} else if output_slice2[i].name > output_slice2[j].name {
			return false
		} else if output_slice2[i].position < output_slice2[j].position {
			return true
		} else if output_slice2[i].position > output_slice2[j].position {
			return false
		} else if output_slice2[i].offset < output_slice2[j].offset {
			return true
		} else if output_slice2[i].offset > output_slice2[j].offset {
			return false
		} else {
			return output_slice2[i].pack_ID_str < output_slice2[j].pack_ID_str
		}
	})
	Printf("\n*** data blobs to be Repacked ***\n")
	Printf("%-12s %12s %12s %5s %6s %7s %3s %s\n", "packfile", "data_blob", "meta_blob",
		"posit", "offset", "size", "mul", "--- path ---")
	for _, elem := range output_slice2 {
		Printf("%12s %12s %12s %5d %6d %7d %3d %s\n", elem.pack_ID_str,
			elem.data_blob_str, elem.meta_blob_str, elem.position, elem.offset,
			elem.size, elem.multi, elem.name)
	}
}

func SizePrune(repositoryData *RepositoryData, unused_blobs mapset.Set[IntID],
	Repacked bool, selected []SnapshotWpl, Detail int) {

	// step 1: find packs which are going to be deleted (partial/full)
	//         map 'unused_blobs' back to their packfiles
	pack_info := GetPackIDs(repositoryData)
	delete_packs := map[IntID]mapset.Set[IntID]{}
	for blob := range unused_blobs.Iter() {
		pack_index := pack_info[blob]
		if _, ok := delete_packs[pack_index]; !ok {
			delete_packs[pack_index] = mapset.NewThreadUnsafeSet[IntID]()
		}
		delete_packs[pack_index].Add(blob)
	}

	// collect all blobs which belong to the same pack (for repacking calculation)
	blobs_per_packID := MakeBlobsPerPackID(repositoryData)

	var (
		count_del_meta, size_del_meta, count_del_data, size_del_data                 int
		count_rep_meta, size_rep_meta, count_rep_data, size_rep_data                 int
		count_partial_meta, size_partial_meta, count_partial_data, size_partial_data int
		count_delete_meta, size_delete_meta, count_delete_data, size_delete_data     int
		pack_del_meta, pack_del_data, pack_rep_meta, pack_rep_data                   int
	)

	// step 3: count in 'delete_packs'
	count_del_meta, size_del_meta, count_del_data, size_del_data = CountAndSizesMap(
		delete_packs, repositoryData)
	repack_blobs_meta := mapset.NewThreadUnsafeSet[IntID]()
	repack_blobs_data := mapset.NewThreadUnsafeSet[IntID]()
	repackPacks := mapset.NewThreadUnsafeSet[IntID]()
	for pack_index := range delete_packs {
		if delete_packs[pack_index].Cardinality() != blobs_per_packID[pack_index].PackBlobSet.Cardinality() {
			repackPacks.Add(pack_index)
			if blobs_per_packID[pack_index].PackfileType == restic.TreeBlob {
				repack_blobs_meta = repack_blobs_meta.Union(blobs_per_packID[pack_index].PackBlobSet)
			} else if blobs_per_packID[pack_index].PackfileType == restic.DataBlob {
				repack_blobs_data = repack_blobs_data.Union(blobs_per_packID[pack_index].PackBlobSet)
			}
		}
	}

	for pack_index := range delete_packs {
		pType := blobs_per_packID[pack_index].PackfileType
		// is it a full pack?
		if delete_packs[pack_index].Cardinality() == blobs_per_packID[pack_index].PackBlobSet.Cardinality() {
			// === straight delete ===
			CountAndSizesSet(delete_packs[pack_index], repositoryData,
				&count_delete_meta, &size_delete_meta, &count_delete_data, &size_delete_data)

			if pType == restic.TreeBlob {
				pack_del_meta++
			} else if pType == restic.DataBlob {
				pack_del_data++
			}
		} else {
			// === repacking ===
			CountAndSizesSet(blobs_per_packID[pack_index].PackBlobSet, repositoryData,
				&count_rep_meta, &size_rep_meta, &count_rep_data, &size_rep_data)
			CountAndSizesSet(delete_packs[pack_index], repositoryData,
				&count_partial_meta, &size_partial_meta, &count_partial_data, &size_partial_data)

			if pType == restic.TreeBlob {
				pack_rep_meta++
			} else if pType == restic.DataBlob {
				pack_rep_data++
			}
		}
	}

	// sum up
	count_partial_blobs := count_partial_meta + count_partial_data
	size_partial_blobs := size_partial_meta + size_partial_data
	count_repack_blobs := count_rep_meta + count_rep_data
	size_repack_blobs := size_rep_meta + size_rep_data

	count_delete_blobs := count_delete_meta + count_delete_data
	size_delete_blobs := size_delete_meta + size_delete_data
	count_delete_packs := pack_del_meta + pack_del_data

	// report
	Printf("\nstraight delete %10d blobs %7d packs %10.3f MiB\n",
		count_delete_blobs, count_delete_packs, float64(size_delete_blobs)/ONE_MEG)
	Printf("this removes    %10d blobs %7s       %10.3f MiB\n",
		count_partial_blobs, " ", float64(size_partial_blobs)/ONE_MEG)

	Printf("meta    delete  %10d blobs %7d packs %10.3f MiB\n",
		count_del_meta, pack_del_meta, float64(size_del_meta)/ONE_MEG)
	Printf("data    delete  %10d blobs %7d packs %10.3f MiB\n",
		count_del_data, pack_del_data, float64(size_del_data)/ONE_MEG)

	Printf("\nmeta    repack  %10d blobs %7d packs %10.3f MiB\n",
		count_rep_meta, pack_rep_meta, float64(size_rep_meta)/ONE_MEG)
	Printf("data    repack  %10d blobs %7d packs %10.3f MiB\n",
		count_rep_data, pack_rep_data, float64(size_rep_data)/ONE_MEG)
	Printf("repack          %10d blobs %7d packs %10.3f MiB\n",
		count_repack_blobs, len(delete_packs)-count_delete_packs,
		float64(size_repack_blobs)/ONE_MEG)

	Printf("\ntotal prune     %10d blobs %7s       %10.3f MiB\n",
		count_partial_blobs+count_delete_blobs, " ",
		float64(size_partial_blobs+size_delete_blobs)/ONE_MEG)

	if Repacked {
		PrintRepackInfo(repositoryData, repack_blobs_meta, repack_blobs_data, selected)
	}

	if Detail == 4 {
		Printf("Repack packfiles # entries %7d\n", repackPacks.Cardinality())
		count := 1
		for ID := range repackPacks.Iter() {
			Printf("%s ", repositoryData.IndexToBlob[ID].String()[:12])
			if count%8 == 0 {
				Printf("\n")
			}
			count++
		}
		Printf("\n")
	}
}

func DoOneRepository(ctx context.Context, gopts GlobalOptions,
	options TRemoveOptions, snapList mapset.Set[string]) error {
	// local vars
	Cutoff := options.Cutoff
	Detail := options.Detail
	Repacked := options.Repacked
	Lost := tremoveOptions.Lost
	if Lost {
		Detail = 1
	}

	repo, err := OpenRepository(ctx, gopts)
	if err != nil {
		return err
	}
	var repositoryData RepositoryData
	init_repositoryData(&repositoryData)
	err = gather_base_data_repo(repo, gopts, ctx, &repositoryData, options.Timing)
	if err != nil {
		return err
	}

	// step 3: compare against Cutoff date
	now := time.Now()
	start := now
	snaps_to_be_deleted := []SnapshotWpl{}
	allSnaps := []string{}
	Printf("snapshots selected for deletion in repository %s\n", gopts.Repo)

	var days int
	for _, sn := range repositoryData.Snaps {
		// move snap_time clock back to midnight
		sn_year, sn_month, sn_day := sn.Time.Date()
		takeIt := false
		days = int(now.Sub(time.Date(sn_year, sn_month, sn_day, 0, 0, 0, 0, time.UTC)).Hours() / 24)
		if snapList == nil {
			takeIt = days >= Cutoff
		} else {
			takeIt = snapList.Contains(sn.ID.Str())
		}

		if takeIt {
			Printf("snap_ID %s %d days old %s:%s at %s\n", sn.ID.Str(),
				days, sn.Hostname, sn.Paths[0], sn.Time.Format(time.DateTime))
			snaps_to_be_deleted = append(snaps_to_be_deleted, sn)
		}
		allSnaps = append(allSnaps, sn.ID.String())
	}

	if len(snaps_to_be_deleted) == 0 {
		Printf("No snapshots selected.\n")
		return nil
	}

	if options.Timing {
		timeMessage(tremoveOptions.MemoryUse, "%-30s %10.1f seconds\n",
			"selected all snapshots", time.Since(start).Seconds())
	}
	allSnapsAsSet := mapset.NewThreadUnsafeSet(allSnaps...)
	Printf("++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n\n")

	// calculate data_map once
	var data_map map[IntID]mapset.Set[CompIddFile]
	if Detail > 0 {
		data_map = map_data_blob_file(&repositoryData)
		//Printf("len(data_map) = %7d\n", len(data_map))
	}

	// step 6.1: loop over the snap_id's to be removed individually
	var selected = make([]SnapshotWpl, 1, 50)

	// sort 'snaps_to_be_deleted' by sn.
	sort.SliceStable(snaps_to_be_deleted, func(i, j int) bool {
		return snaps_to_be_deleted[i].Time.After(snaps_to_be_deleted[j].Time)
	})
	for _, sn := range snaps_to_be_deleted {
		selected[0] = sn
		Printf("\n*** snap_ID %s %s:%s at %s\n", sn.ID.Str(),
			sn.Hostname, sn.Paths[0], sn.Time.Format(time.DateTime))
		CalculatePruneSize(selected, &repositoryData, Detail, Lost, false, allSnapsAsSet,
			data_map)
	}

	Printf("\n*** ALL ***\n")
	CalculatePruneSize(snaps_to_be_deleted, &repositoryData, 0, false, Repacked,
		allSnapsAsSet, data_map)
	if options.Timing {
		timeMessage(options.MemoryUse, "%-30s %10.1f seconds\n",
			"group summary", time.Since(start).Seconds())
	}
	return nil
}

// do all repositories defined as a group in the configuration file
func DoMultiRepository(ctx context.Context, gopts GlobalOptions, options TRemoveOptions) error {
	if gopts.Repo != "" {
		Printf("Repository specified as well! repo is %s. Terminating\n", gopts.Repo)
		return errors.New("repo specified as well!")
	}

	var (
		err         error
		json_config map[string]map[string]StringSlice
		ok          bool
	)

	// read json from file
	json_string, err := os.ReadFile(options.ConfigFile)
	if err != nil {
		Printf("Cant read json config file '%s' - %v\n", options.ConfigFile, err)
		return err
	}

	err = json.Unmarshal(json_string, &json_config)
	if err != nil {
		Printf("Cant decode config file %v\n", err)
		return err
	}

	// we need the target
	_, ok = json_config["BASE_CONFIG"][options.MultiRepo]
	if !ok {
		Printf("Can't currently deal with other base_parts '%s'\n", options.MultiRepo)
		return errors.New("Can't currently deal with other multiple repositories")
	}
	target := json_config["BASE_CONFIG"][options.MultiRepo][0]

	// loop over all repositories
	for _, repo_basename := range json_config["BASE_CONFIG"]["repos"] {
		Printf("\n")
		gopts.Repo = target + repo_basename
		DoOneRepository(ctx, gopts, options, nil)
	}
	return nil
}

// Do one Repository with given snapshots
func DoOneRepositoryWithSnaps(ctx context.Context, gopts GlobalOptions,
	options TRemoveOptions, args []string) error {

	saveQuiet := globalOptions.Quiet
	saveVerbo := globalOptions.verbosity
	globalOptions.Quiet = true
	globalOptions.verbosity = 0
	repo, err := OpenRepository(ctx, gopts)
	if err != nil {
		return err
	}

	_, snapMap, err := GatherAllSnapshots(ctx, repo)
	if err != nil {
		return err
	}
	repo.Close()
	globalOptions.Quiet = saveQuiet
	globalOptions.verbosity = saveVerbo

	// all Snaps in repo as Set
	allSnapSet := mapset.NewThreadUnsafeSet[string]()
	allSnapsShort := mapset.NewThreadUnsafeSet[string]()
	for snap_id := range snapMap {
		allSnapSet.Add(snap_id)
		allSnapsShort.Add(snap_id[:8])
	}

	// check for existence in allSnapIDs
	toBeConsidered := mapset.NewThreadUnsafeSet[string]()
	for _, snap_id := range args {
		if allSnapsShort.Contains(snap_id) {
			toBeConsidered.Add(snap_id)
		}
	}

	if toBeConsidered.Cardinality() == 0 {
		Printf("No valid Snaps found. Terminating!\n")
		return nil
	}
	DoOneRepository(ctx, gopts, tremoveOptions, toBeConsidered)
	return nil
}

func CountAndSizesMap(inputMap map[IntID]mapset.Set[IntID], repositoryData *RepositoryData) (
	cm int, sm int, cd int, sd int) {

	//cm, cd, sm, sd = 0,0,0,0
	for _, dataSet := range inputMap {
		for metaBlob := range dataSet.Iter() {
			ih := repositoryData.IndexHandle[repositoryData.IndexToBlob[metaBlob]]
			if ih.Type == restic.TreeBlob {
				cm++
				sm += ih.size
			} else if ih.Type == restic.DataBlob {
				cd++
				sd += ih.size
			}
		}
	}
	return cm, sm, cd, sd
}

func CountAndSizesSet(inputSet mapset.Set[IntID], repositoryData *RepositoryData,
	cm *int, sm *int, cd *int, sd *int) {

	for metaBlob := range inputSet.Iter() {
		ih := repositoryData.IndexHandle[repositoryData.IndexToBlob[metaBlob]]
		if ih.Type == restic.TreeBlob {
			*cm = *cm + 1
			*sm += ih.size
		} else if ih.Type == restic.DataBlob {
			*cd = *cd + 1
			*sd += ih.size
		}
	}
	return
}
