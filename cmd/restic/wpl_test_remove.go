package main

// test remove a snapshot or mutiple snapshots using the repository
// calculate the changes
// A lot is done here using set arithmetic. The meta-blobs and data-blob sets
// and their various differences are used to derive size information by using
// their compressed sizes - which are the important ones in the repository

import (
	// system
	"context"
	"errors"
	"sort"
	"time"
	"strings"

	//argparse
	"github.com/spf13/cobra"

	// restic library
	"github.com/wplapper/restic/library/repository"
	"github.com/wplapper/restic/library/restic"

	// mapset
	"github.com/deckarep/golang-set/v2"
)

type TRemoveOptions struct {
	cutoff     int
	snaps      []string
	detail     int
	timing     bool
	memory_use bool
	lost       bool
	repacked   bool
}

var tremoveOptions TRemoveOptions

var cmdTRemove = &cobra.Command{
	Use:   "tremove [flags]",
	Short: "test temove one or more snapshots from the repo",
	Long: `
test temove one or more snapshots from the repo.
Print repackaging information.

Available options:
  - cutoff: defaults to 270 days
  - detail: prints file details of those blobs which are about to be removed
  - lost: checks if list files get replaced by newer versions of the file
  - repack: print lots about blobs to be repacked
  - timing: give some timings of the various subsections of code

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
	f.IntVarP(&tremoveOptions.cutoff, "cutoff", "C", 270, "cutoff snaps which are older than <cutoff> days")
	f.CountVarP(&tremoveOptions.detail, "detail", "D", "print dir/file details")
	f.BoolVarP(&tremoveOptions.lost, "lost", "L", false, "print lost file details")
	f.BoolVarP(&tremoveOptions.repacked, "repack", "R", false, "more info on repacks")
	f.BoolVarP(&tremoveOptions.timing, "timing", "T", false, "produce timings")
}

func runTRemove(ctx context.Context, cmd *cobra.Command, gopts GlobalOptions,
	args []string) error {

	// setup global data
	var repositoryData RepositoryData
	gOptions = gopts
	init_repositoryData(&repositoryData)

	// analyse cutoff date
	cutoff   := tremoveOptions.cutoff
	detail   := tremoveOptions.detail
	lost     := tremoveOptions.lost
	repacked := tremoveOptions.repacked
	if lost {
		detail = 1
	}

	// step 1: open repository
	start := time.Now()
	repo, err := OpenRepository(ctx, gopts)
	if err != nil {
		return err
	}
	Verboseff("Repository is %s\n", globalOptions.Repo)
	if tremoveOptions.timing {
		timeMessage(tremoveOptions.memory_use, "%-30s %10.1f seconds\n", "open repository",
			time.Now().Sub(start).Seconds())
	}
	repositoryData.repo = repo

	err = gather_base_data_repo(repo, gopts, ctx, &repositoryData, tremoveOptions.timing)
	if err != nil {
		return err
	}

	// push 'cutoff' high if any snaps are given in the parameter list
	if len(args) > 0 {
		cutoff = 9999
	}
	snaps_from_cli := mapset.NewSet(args...)

	// step 3: compare against cutoff date
	now := time.Now()
	snaps_to_be_deleted := make([]*restic.Snapshot, 0, 50)
	snap_slice := make([]string, 0)
	Printf("snapshots selected for deletion in repository %s\n", gopts.Repo)
	for _, sn := range repositoryData.snaps {
		// move snap_time clock back to midnight
		sn_year, sn_month, sn_day := sn.Time.Date()
		days := int(now.Sub(time.Date(sn_year, sn_month, sn_day,
			0, 0, 0, 0, time.UTC)).Hours() / 24)

		if days >= cutoff || snaps_from_cli.Contains(sn.ID().Str()) {
			Printf("snap_ID %s %d days old %s:%s at %s\n", sn.ID().Str(),
				days, sn.Hostname, sn.Paths[0], sn.Time.String()[:19])
			snaps_to_be_deleted = append(snaps_to_be_deleted, sn)
		}
		snap_slice = append(snap_slice, sn.ID().Str())
	}

	if len(snaps_to_be_deleted) == 0 {
		return errors.New("No snapshots selected.")
	}

	if tremoveOptions.timing {
		timeMessage(tremoveOptions.memory_use, "%-30s %10.1f seconds\n",
			"selected all snapshots", time.Now().Sub(start).Seconds())
	}
	snap_set := mapset.NewSet(snap_slice...)
	Printf("++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n\n")

	// calculate data_map once
	if detail > 0 {
		repositoryData.data_map = map_data_blob_file(&repositoryData)

		// inverse repositoryData.fullpath map which is map[IntID]string
		// we need this for the analysis if there is a replacement file in the repo
		repositoryData.reverse_fullpath = make(map[string]mapset.Set[IntID])
		for int_blob, path := range repositoryData.fullpath {
			if _, ok := repositoryData.reverse_fullpath[path]; !ok {
				repositoryData.reverse_fullpath[path] = mapset.NewSet[IntID]()
			}
			repositoryData.reverse_fullpath[path].Add(int_blob)
		}
	}

	// gather all blobs and their total size
	repositoryData.all_blobs = mapset.NewSet[IntID]()
	for _, ih := range repositoryData.index_handle {
		repositoryData.all_blobs.Add(ih.blob_index)
	}
	// collect all blobs which belong to the same pack (for repacking calculation)
	repositoryData.blobs_per_packID = make_blobs_per_packID(&repositoryData)

	// step 6.1: loop over the snap_id's to be removed individually
	selected := make([]*restic.Snapshot, 1, 1)

	// sort 'snaps_to_be_deleted' by sn.
	sort.SliceStable(snaps_to_be_deleted, func(i, j int) bool {
		return snaps_to_be_deleted[i].Time.After(snaps_to_be_deleted[j].Time)
	})
	for _, sn := range snaps_to_be_deleted {
		selected[0] = sn
		Printf("\n*** snap_ID %s %s:%s at %s\n", sn.ID().Str(),
			sn.Hostname, sn.Paths[0], sn.Time.String()[:19])
		CalculatePruneSize(selected, &repositoryData, detail, lost, false, snap_set, ctx, repo)
	}

	Printf("\n*** ALL ***\n")
	CalculatePruneSize(snaps_to_be_deleted, &repositoryData, 0, false, repacked, snap_set, ctx, repo)
	if tremoveOptions.timing {
		timeMessage(tremoveOptions.memory_use, "%-30s %10.1f seconds\n",
			"group summary", time.Now().Sub(start).Seconds())
	}

	return nil
}

// calculate sizes for 'selected' snapshots
func CalculatePruneSize(selected []*restic.Snapshot, repositoryData *RepositoryData,
	detail int, lost bool, repacked bool, snap_set mapset.Set[string], ctx context.Context,
	repo *repository.Repository) error {

	// step 1: find all meta- and data-blobs in given 'selected' snapshot
	all_other_snapshots := snap_set.Clone() // = all_snapshots - selected
	for _, sn := range selected {
		all_other_snapshots.Remove(sn.ID().Str())
	}

	// continually add to 'used_blobs' while using 'all_other_snapshots'.
	// all these blobs are still in use
	used_blobs := mapset.NewSet[IntID]()
	for snap_id := range all_other_snapshots.Iter() {
		id_ptr := Ptr2ID(*(repositoryData.snap_map[snap_id]).ID(), repositoryData)
		for meta_blob := range repositoryData.meta_dir_map[id_ptr].Iter() {
			used_blobs.Add(meta_blob)
			for _, meta := range repositoryData.directory_map[meta_blob] {
				used_blobs.Append(meta.content...)
			}
		}
	}

	// define the unused blobs
	unused_blobs := repositoryData.all_blobs.Difference(used_blobs)
	SizePrune(repositoryData, unused_blobs, repacked, selected, detail)

	if detail == 4 {
		print_very_raw(repositoryData, unused_blobs)
	} else if detail == 3 {
		print_raw(repositoryData, unused_blobs)
	} else if detail == 2 || detail == 1 {
		print_some_detail(repositoryData, unused_blobs, detail, lost)
	}
	return nil
}

// This function creates a data map which is global for the repository. It
// contains a mapping from a data blob to the containing meta blob a the
// offset in the file list, used for gathering the file name to which this data
// blob belongs. Data blob can belong to multiple files.
func map_data_blob_file(repositoryData *RepositoryData) map[IntID]mapset.Set[CompIddFile] {
	// map data blobs back to meta_blob, position in directory_map
	data_map := make(map[IntID]mapset.Set[CompIddFile])
	for meta_blob, file_list := range repositoryData.directory_map {
		for position, meta := range file_list {
			// generate composite index
			cmp_ix := CompIddFile{meta_blob: meta_blob, position: position}
			if meta.Type == "file" {
				for _, data_blob := range meta.content {
					if _, ok := data_map[data_blob]; !ok {
						data_map[data_blob] = mapset.NewSet[CompIddFile]()
					}
					data_map[data_blob].Add(cmp_ix)
				}
			}
		}
	}
	return data_map
}

func print_some_detail(repositoryData *RepositoryData, unused_blobs mapset.Set[IntID],
	detail int, lost bool) {
	// this is detail = 1 / 2
	// gather detail of deleted directories and files
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

	deleted_files := make(map[string]file_info)
	for blob := range unused_blobs.Iter() {
		ih := repositoryData.index_handle[repositoryData.index_to_blob[blob]]
		if ih.Type == restic.TreeBlob && detail > 1 {
			if len(repositoryData.fullpath[blob]) < 3 {
				filename = "/"
			} else {
				filename = repositoryData.fullpath[blob][2:]
			}
			deleted_files[filename] = file_info{size: 0}
		} else if ih.Type == restic.DataBlob {
			// in case of multiple entries this is an arbitrary choice!!
			cmp_ix = repositoryData.data_map[blob].ToSlice()[0]
			meta := repositoryData.directory_map[cmp_ix.meta_blob][cmp_ix.position]
			name := meta.name
			size = int(meta.size)

			dir_name := repositoryData.fullpath[cmp_ix.meta_blob]
			if repositoryData.reverse_fullpath[dir_name].Cardinality() > 1 {
				filename = repositoryData.fullpath[cmp_ix.meta_blob][2:] + "/" + name
				repl = "ok"
			} else {
				// no replacement available
				filename = repositoryData.fullpath[cmp_ix.meta_blob][2:] + "/" + name
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
	found_files    := false
	for _, filename := range deleted_filenames_to_sort {
		comp := deleted_files[filename]
		size := comp.size
		repl := comp.repl
		if size > 0 {
			found_files = true
		}
		if ! lost {
			if size > 0 {
				if ! header_printed {
					header_printed = true
					Printf("\n%10s %s %s\n", "size", "rp", "filename")
				}
				Printf("%10d %s %s\n", size, repl, filename)
			} else {
				if ! header_printed {
					header_printed = true
					Printf("\n%10s %s %s\n", "size", "rp", "filename")
				}
				Printf("%13s %s\n", "", filename)
			}
		} else if size > 0 && repl == "--" && lost {
			if ! header_printed {
				header_printed = true
				Printf("\n%10s %s %s\n", "size", "rp", "filename")
			}
			Printf("%10d %s %s\n", size, repl, filename)
		}
	}

	if ! header_printed && lost && found_files {
		Printf("  All removed files have a newer version in the repository.\n")
	}
}

// print raw, but offset is ignored, so sorting IS different
func print_raw(repositoryData *RepositoryData, unused_blobs mapset.Set[IntID]) {
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
		ID := repositoryData.index_to_blob[blob]
		ih := repositoryData.index_handle[ID]
		if ih.Type == restic.DataBlob {
			cmp_ix_set := repositoryData.data_map[blob]
			// in case of multiple entries this is an arbitrary choice!!
			cmp_ix = cmp_ix_set.ToSlice()[0]

			meta_blob := cmp_ix.meta_blob
			position := cmp_ix.position
			base := repositoryData.directory_map[meta_blob][position].name
			raw_blob = BlobInfo{blob: blob, Type: ih.Type, size: ih.size,
				blob_str:  ID.String()[:12],
				meta_blob: meta_blob,
				position:  position,
				path:      repositoryData.fullpath[meta_blob] + "/" + base}
		} else {
			raw_blob = BlobInfo{blob: blob, Type: ih.Type, blob_str: ID.String()[:12],
				path: repositoryData.fullpath[blob] + "/"}
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

// print full raw detail
func print_very_raw(repositoryData *RepositoryData, unused_blobs mapset.Set[IntID]) {
	full_map := make_full_contents_map_v2(repositoryData)

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
		blob_ID := repositoryData.index_to_blob[blob]
		ih := repositoryData.index_handle[blob_ID]
		pack_ID_str := repositoryData.index_to_blob[ih.pack_index].String()[:12]
		if data_sett, ok := full_map[blob_ID]; ok {
			// in case of multiple entries this is an arbitrary choice!!
			full_info := data_sett.ToSlice()[0]
			if ih.Type == restic.DataBlob {
				meta_blob := repositoryData.blob_to_index[full_info.meta_blob]

				raw_blob = BlobInfo{blob: blob, Type: ih.Type, size: ih.size,
					meta_blob: meta_blob, position: full_info.position, offset: full_info.offset,
					blob_str:  blob_ID.String()[:12],
					mblob_str: full_info.meta_blob.String()[:12],
					path:      repositoryData.fullpath[meta_blob] + "/" +
						repositoryData.directory_map[meta_blob][full_info.position].name,
					pfile_str: pack_ID_str,
				}
			}
		} else if ih.Type == restic.TreeBlob {
			raw_blob = BlobInfo{blob: blob,
				Type: ih.Type, mblob_str: blob_ID.String()[:12],
				path: repositoryData.fullpath[blob] + "/",
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

	var path string
	Printf("\n%12s %12s %12s %8s %8s %6s %s\n", "packfile", "meta blob", "data blob", "size",
		"position", "offset", "path")
	for _, raw_blob := range to_be_sorted {
		if len(raw_blob.path) < 3 {
			path = "/"
		} else {
			path = raw_blob.path[2:]
		}
		if raw_blob.Type == restic.DataBlob {
			Printf("%s %12s %s %8d %8d %6d %s\n", raw_blob.pfile_str, raw_blob.mblob_str,
				raw_blob.blob_str, raw_blob.size, raw_blob.position, raw_blob.offset, path)
		} else if raw_blob.Type == restic.TreeBlob {
			Printf("%12s %s %12s %8s %8s %6s %s\n", raw_blob.pfile_str, raw_blob.mblob_str,
				raw_blob.blob_str, "", "", "", path)
		}
	}
}

func print_repack_info(repositoryData *RepositoryData,
repack_blobs_meta mapset.Set[IntID], repack_blobs_data mapset.Set[IntID],
selected []*restic.Snapshot) {

	type SortPath struct {
		ID_str string
		path   string
		depth  int16
	}

	root_set := mapset.NewSet[IntID]()
	for _,sn := range selected {
		root_set.Add(repositoryData.blob_to_index[*sn.Tree])
	}

	pack_info := get_packIDs(repositoryData)
	Printf("*** topological sort ***\n")
	location_depth2 := make([]int32, 0)
	result := dfs(repositoryData.children, root_set, repositoryData)

	output_slice := make([]SortPath, 0, len(result))
	for _, meta_blob := range result {
		out := repositoryData.fullpath[meta_blob]
		lcomp := len(strings.Split(out, "/"))
		if lcomp <= 2 {
			out = "/"
			location_depth2 = append(location_depth2, int32(len(output_slice)))
		} else {
			out = out[2:]
		}
		output_slice = append(output_slice, SortPath{path: out, depth: int16(lcomp),
			ID_str: repositoryData.index_to_blob[meta_blob].String()[:12]})
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

	full_map := make_full_contents_map_v2(repositoryData)
	output_slice2 := make([]SortableMoreMulti, 0, repack_blobs_data.Cardinality())
	for data_blob := range repack_blobs_data.Iter() {
		// in case of multiple entries this is an arbitrary choice!!
		// a better concept would be a domain, e.g. based o the repositoryData.meta_dir_map[snap_id]
		// intersected with the meta_blobs which have to be repacked
		multi := full_map[repositoryData.index_to_blob[data_blob]].Cardinality()
		data_sett := full_map[repositoryData.index_to_blob[data_blob]].ToSlice()[0]
		//for elem := range full_map[repositoryData.index_to_blob[data_blob]].Iter() {
		//	if elem.meta_blob ==
		dblob := repositoryData.index_to_blob[data_blob]
		output_slice2 = append(output_slice2, SortableMoreMulti{
			meta_blob:     data_sett.meta_blob,
			data_blob:     dblob,
			position:      data_sett.position,
			offset:	       data_sett.offset,
			meta_blob_str: data_sett.meta_blob_str,
			data_blob_str: data_sett.data_blob_str,
			name:          repositoryData.fullpath[repositoryData.blob_to_index[data_sett.meta_blob]][2:] +
				             "/" + data_sett.name,
			multi:         multi,
			pack_ID_str:   repositoryData.index_to_blob[pack_info[data_blob]].String()[:12],
			size:          repositoryData.index_handle[dblob].size,
		})
	}

	sort.SliceStable(output_slice2, func(i, j int) bool {
		if        output_slice2[i].meta_blob_str < output_slice2[j].meta_blob_str {
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
		} else if output_slice2[i].offset   < output_slice2[j].offset {
			return true
		} else if output_slice2[i].offset   > output_slice2[j].offset {
			return false
		} else {
			return output_slice2[i].pack_ID_str < output_slice2[j].pack_ID_str
		}
	})
	Printf("\n*** data blobs to be repacked ***\n")
	Printf("%-12s %12s %12s %5s %6s %7s %3s %s\n", "packfile", "data_blob", "meta_blob",
		"posit", "offset", "size", "mul", "--- path ---")
	for _, elem := range output_slice2 {
		Printf("%12s %12s %12s %5d %6d %7d %3d %s\n", elem.pack_ID_str,
			elem.data_blob_str, elem.meta_blob_str, elem.position, elem.offset,
			elem.size, elem.multi, elem.name)
	}

	// reset
	result = nil
	full_map = nil
	root_set = nil
	pack_info = nil
	output_slice = nil
	output_slice2 = nil
}

// map blob_int to its packID (int)
func get_packIDs(repositoryData *RepositoryData) map[IntID]IntID {
	result := make(map[IntID]IntID)
	for _, ih := range repositoryData.index_handle {
		result[ih.blob_index] = ih.pack_index
	}
	return result
}

func SizePrune(repositoryData *RepositoryData, unused_blobs mapset.Set[IntID],
	repacked bool, selected []*restic.Snapshot, detail int) {

	// step 1: find packs which are going to be deleted (partial/full)
	//         map 'unused_blobs' back to their packfiles
	delete_packs := make(map[IntID]mapset.Set[IntID], unused_blobs.Cardinality())
	repack_blobs_meta := mapset.NewSet[IntID]()
	repack_blobs_data := mapset.NewSet[IntID]()
	for blob := range unused_blobs.Iter() {
		ix := repositoryData.index_to_blob[blob]
		pack_index := repositoryData.index_handle[ix].pack_index
		//initializef pack_index
		if _, ok := delete_packs[pack_index]; !ok {
			delete_packs[pack_index] = mapset.NewSet[IntID]()
		}
		delete_packs[pack_index].Add(blob)
	}

	// step 2: summarize
	count_delete_packs := 0
	count_repack_blobs := 0
	count_delete_blobs := 0
	count_partial_blobs := 0
	size_repack_blobs := 0
	size_delete_blobs := 0
	size_partial_blobs := 0
	count_del_meta := 0
	count_del_data := 0
	size_del_meta := 0
	size_del_data := 0
	count_rep_meta := 0
	count_rep_data := 0
	size_rep_meta := 0
	size_rep_data := 0
	pack_del_meta := 0
	pack_del_data := 0
	pack_rep_meta := 0
	pack_rep_data := 0

	// step 3: count in 'delete_packs'
	for pack_index := range delete_packs {
		for blob := range delete_packs[pack_index].Iter() {
			ih := repositoryData.index_handle[repositoryData.index_to_blob[blob]]
			if ih.Type == restic.TreeBlob {
				count_del_meta++
				size_del_meta += ih.size
			} else if ih.Type == restic.DataBlob {
				count_del_data++
				size_del_data += ih.size
			}
		}
	}

	repackPacks := mapset.NewSet[IntID]()
	for pack_index := range delete_packs {
		// is it a full pack?

		if _, ok := delete_packs[pack_index]; ! ok {
			Printf("delete_packs.pack_index %7d missing\n", pack_index)
		}
		if _, ok := repositoryData.blobs_per_packID[pack_index]; ! ok {
			Printf("blobs_per_packID.pack_index %7d missing\n", pack_index)
		}

		if delete_packs[pack_index].Cardinality() ==
			repositoryData.blobs_per_packID[pack_index].Cardinality() {
			// straight delete!
			count_delete_packs++
			// get sizes
			for blob := range delete_packs[pack_index].Iter() {
				ih := repositoryData.index_handle[repositoryData.index_to_blob[blob]]
				count_delete_blobs++
				size_delete_blobs += ih.size
			}

			a_blob := repositoryData.blobs_per_packID[pack_index].ToSlice()[0]
			pType := repositoryData.index_handle[repositoryData.index_to_blob[a_blob]].Type
			if pType == restic.TreeBlob {
				pack_del_meta++
			} else if pType == restic.DataBlob {
				pack_del_data++
			}
		} else {
			// needs repacking, get sizes and counts
			repackPacks.Add(pack_index)
			a_blob := repositoryData.blobs_per_packID[pack_index].ToSlice()[0]
			psType := repositoryData.index_handle[repositoryData.index_to_blob[a_blob]].Type
			if psType == restic.TreeBlob {
				pack_rep_meta++
			} else if psType == restic.DataBlob {
				pack_rep_data++
			}

			for blob := range repositoryData.blobs_per_packID[pack_index].Iter() {
				ih := repositoryData.index_handle[repositoryData.index_to_blob[blob]]
				size_repack_blobs += ih.size
				count_repack_blobs++

				if ih.Type == restic.TreeBlob {
					count_rep_meta++
					size_rep_meta += ih.size
					repack_blobs_meta.Add(blob)
				} else if ih.Type == restic.DataBlob {
					count_rep_data++
					size_rep_data += ih.size
					repack_blobs_data.Add(blob)
				}
			}

			// count partial packs
			for blob := range delete_packs[pack_index].Iter() {
				ih := repositoryData.index_handle[repositoryData.index_to_blob[blob]]
				size_partial_blobs += ih.size
				count_partial_blobs++
			}
		}
	}

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
		count_repack_blobs, len(delete_packs) - count_delete_packs,
		float64(size_repack_blobs)/ONE_MEG)

	Printf("\ntotal prune     %10d blobs %7s       %10.3f MiB\n",
		count_partial_blobs + count_delete_blobs, " ",
		float64(size_partial_blobs + size_delete_blobs)/ONE_MEG)

	if repacked {
		print_repack_info(repositoryData, repack_blobs_meta, repack_blobs_data, selected)
	}

	if detail == 4 {
		Printf("Repack packfiles # entries %7d\n", repackPacks.Cardinality())
		count := 1
		for ID := range repackPacks.Iter() {
			Printf("%s ", repositoryData.index_to_blob[ID].String()[:12])
			if count % 8 == 0 {
				Printf("\n")
			}
			count++
		}
		Printf("\n")
	}
}
