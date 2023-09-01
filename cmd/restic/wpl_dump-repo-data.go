package main

import (
	// system
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	//argparse
	"github.com/spf13/cobra"

	// restic library
	"github.com/wplapper/restic/library/pack"
	"github.com/wplapper/restic/library/restic"

	// sets
	"github.com/deckarep/golang-set/v2"
)

const (
	SNAPSHOTS = 1 << iota
	INDEX     = 1 << iota
	PACKFILES = 1 << iota
	META_DIR  = 1 << iota
	FILEDATA  = 1 << iota
	CONTENTS  = 1 << iota
	TREE      = 1 << iota
	PACK2TREE = 1 << iota
	SUBTREE   = 1 << iota
	MULTIROOT = 1 << iota
	DIRECTORY = 1 << iota
	FULLPATH  = 1 << iota
	//DIRPATHID = 1 << iota
)

type SortableID struct {
	ID     restic.ID
	ID_str string
}

type WplDumpOptions struct {
	debug            string
	include          string
	where            string
	more_data        bool
	recurse          bool
	fullID	         bool
	multi_root_stats bool
	children         int
	sorting          string
}

var dump_options WplDumpOptions

var cmdWplDump = &cobra.Command{
	Use:   "wpl-dump [flags]",
	Short: "dig into the internals of the various repository tables and show their contents",
	Long: `dig into the internals of the various repository tables and show their contents.

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
	DisableAutoGenTag: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runWplDump(cmd.Context(), cmd, globalOptions, args)
	},
}

func init() {
	cmdRoot.AddCommand(cmdWplDump)
	f := cmdWplDump.Flags()
	f.BoolVarP(&dump_options.more_data, "more-data", "M", false, "print more data")
	f.BoolVarP(&dump_options.recurse, "recurse", "R", false, "recurse into sub-directories")
	f.BoolVarP(&dump_options.fullID, "full", "f", false, "Full IDs instead of shortened IDs")

	f.StringVarP(&dump_options.debug, "debug", "D", "", "debug f")
	f.StringVarP(&dump_options.include, "filter-string", "I", "", "filter string")
	f.StringVarP(&dump_options.where, "filter-type", "T", "", "filter type, one of [pmdbs]")
	f.StringVarP(&dump_options.sorting, "sorting", "S", "", "sorting option, one of [pmdbs]")
	f.BoolVarP(&dump_options.multi_root_stats, "multi-root", "U", false, "multi root statistics")
	f.IntVarP(&dump_options.children, "children", "C", 0, "generate top list of lots of children")
}

func runWplDump(ctx context.Context, cmd *cobra.Command, gopts GlobalOptions,
	args []string) error {

	var (
		err            error
		repositoryData RepositoryData
	)

	// startup
	gOptions = gopts
	init_repositoryData(&repositoryData)

	// step 1: open repository
	repo, err := OpenRepository(ctx, gopts)
	if err != nil {
		return err
	}

	err = gather_base_data_repo(repo, gopts, ctx, &repositoryData, false)
	if err != nil {
		return err
	}

	process_debug_functions(&dump_options, &repositoryData, repo, ctx, args)
	return nil
}

/* =============================================================================
 *
 * call the debugging functions to detail the important structures of
 * the repository
 *
 * =============================================================================
 */

// print all snapshots in time ascending order - debug function SNAPSHOTS
func PrintSnapshotsWpl(repositoryData *RepositoryData, options *WplDumpOptions) {

	// the snapshot list has already been sorted by Time order, just print the stuff
	Printf("\n*** Snapshots  (SNAPSHOTS) ***\n")
	Printf("\n%-8s  %-19s  %s:%s\n", "snap_ID", "date and time", "host", "filesystem")
	for _, sn := range repositoryData.Snaps {
		Printf("%s  %s  %s:%s\n", sn.ID().Str(), sn.Time.String()[:19],
			sn.Hostname, sn.Paths[0])
	}
}

type IndexHandlePrint struct {
	ID          restic.ID
	pack_ID     restic.ID
	blob_ID_str string
	pack_ID_str string

	Length             int
	UncompressedLength int
	Type               string
}

// print all Index handles - sort by blob (string) - debug function INDEX
func PrintIndexHandles(repositoryData *RepositoryData, options *WplDumpOptions) {
	to_be_sorted := make([]IndexHandlePrint, 0, len(repositoryData.IndexHandle))

	filter, filter_map, filter_string := filter_processing(options)
	var btype string
	Printf("\n*** Index handles  (INDEX) ***\n")
	for ID, ih := range repositoryData.IndexHandle {
		if ih.Type == restic.TreeBlob {
			btype = "tree"
		} else if ih.Type == restic.DataBlob {
			btype = "data"
		} else {
			btype = "????"
		}

		var blob_ID_str, pack_ID_str string
		pack_ID := repositoryData.IndexToBlob[ih.pack_index]
		if ! options.fullID {
			blob_ID_str = ID.String()[:12]
			pack_ID_str = pack_ID.String()[:12]
		} else {
			blob_ID_str = ID.String()
			pack_ID_str = pack_ID.String()
		}
		if filter && ((filter_map["b"] && blob_ID_str != filter_string) ||
			(filter_map["p"] && pack_ID_str != filter_string)) {
			continue
		}
		entry := IndexHandlePrint{ID: ID, pack_ID: pack_ID,
			blob_ID_str: blob_ID_str, pack_ID_str: pack_ID_str,
			Length: ih.size, UncompressedLength: ih.UncompressedLength, Type: btype}
		to_be_sorted = append(to_be_sorted, entry)
	}

	// sort & print
	sort.Slice(to_be_sorted, func(i, j int) bool {
		return to_be_sorted[i].blob_ID_str < to_be_sorted[j].blob_ID_str
	})

	Printf("%-12s  %-12s  %4s %8s %9s\n", "blob-ID", "pack-ID", "type",
		"Length", "UC-Length")
	for _, entry := range to_be_sorted {
		Printf("%s  %s  %s %8d %9d\n", entry.blob_ID_str, entry.pack_ID_str,
			entry.Type, entry.Length, entry.UncompressedLength)
	}
	to_be_sorted = nil
}

type PackFilesOnly struct {
	ID     restic.ID
	ID_str string
	Type   string
}

// print packfiles index - debug function PACKFILES
func PrintPackfilesIndex(repositoryData *RepositoryData, options *WplDumpOptions,
	repo restic.Repository, ctx context.Context) {

	// build a map of packfile -> blob mapping
	packfiles_set := mapset.NewSet[PackFilesOnly]()
	packfiles_counts := make(map[restic.ID]int)
	var ptype string
	for _, ih := range repositoryData.IndexHandle {
		if ih.Type == restic.TreeBlob {
			ptype = "tree"
		} else if ih.Type == restic.DataBlob {
			ptype = "data"
		} else {
			ptype = "unkn"
		}
		ID := repositoryData.IndexToBlob[ih.pack_index]
		ID_str := ID.String()
		packfiles_set.Add(PackFilesOnly{Type: ptype, ID: ID, ID_str: ID_str})
		if _, ok := packfiles_counts[ID]; !ok {
			packfiles_counts[ID] = 1
		} else {
			packfiles_counts[ID]++
		}
	}
	// get length of packs
	repo_packs := pack.Size(ctx, repo.Index(), false)

	//get the stats file loaded into memory
	handle := restic.Handle{Type: restic.WplFile, Name: "wpl/packfiles.stat"}
	checked_packs, _ := ReadStatsFile(repo, ctx, handle)

	packfiles := packfiles_set.ToSlice()
	sort.Slice(packfiles, func(i, j int) bool {
		return packfiles[i].ID_str < packfiles[j].ID_str
	})

	SIZE := 12
	if options.fullID {
		SIZE = 64
	}

	Printf("\n*** List of all packfiles (PACKFILES) ***\n")
	Printf("\n%-[1]*[2]s %9[3]s %4s %6s %5s\n", SIZE, "packfile-ID", "length", "type",
		"bcount", "check")
	var fullID string
	for _, entry := range packfiles {
		ID := entry.ID
		ID_str := entry.ID_str
		fullID = ID_str[:SIZE]
		Printf("%s %9d %s %6d %v\n", fullID, repo_packs[ID], entry.Type,
			packfiles_counts[ID], checked_packs[ID_str])
	}
	packfiles_set = nil
	packfiles_counts = nil
}

// print meta_dir mapping - reverse: create map meta_blobs -> snapshots
// debug function META_DIR
func PrintMetaDirMapReverse(repositoryData *RepositoryData,
	options *WplDumpOptions) {
	// create reverse map

	meta_blob_2_snap := make(map[restic.ID]mapset.Set[string])
	for ID, blob_set := range repositoryData.MetaDirMap {
		id := ID.Str()
		for blob_int := range blob_set.Iter() {
			blob := repositoryData.IndexToBlob[blob_int]
			if _, ok := meta_blob_2_snap[blob]; !ok {
				meta_blob_2_snap[blob] = mapset.NewSet[string]()
			}
			meta_blob_2_snap[blob].Add(id)
		}
	}

	// sort meta_blob_2_snap key
	filter, filter_map, filter_string := filter_processing(options)
	Printf("\n*** Tree Blob Usage by Snap (META_DIR) ***\n")
	to_be_sorted := make([]SortableID, 0, len(meta_blob_2_snap))
	for ID := range meta_blob_2_snap {
		to_be_sorted = append(to_be_sorted, SortableID{ID: ID, ID_str: ID.String()[:12]})
	}

	sort.Slice(to_be_sorted, func(i, j int) bool {
		return to_be_sorted[i].ID_str < to_be_sorted[j].ID_str
	})

	for _, entry := range to_be_sorted {
		ID := entry.ID
		blob_str := entry.ID_str
		if filter && filter_map["m"] && entry.ID_str != filter_string {
			continue
		}
		snaps_to_be_sorted := meta_blob_2_snap[ID].ToSlice()
		sort.Strings(snaps_to_be_sorted)

		path := repositoryData.FullPath[repositoryData.BlobToIndex[ID]]
		if len(path) > 2 {
			path = path[2:]
		}
		Printf("\n * tree blob %s (%s) is used by:\n", blob_str, path)
		count := 1
		for _, snap_id := range snaps_to_be_sorted {
			if filter && filter_map["s"] && snap_id != filter_string {
				continue
			}
			Printf("%s ", snap_id)
			if count % 8 == 0 {
				Printf("\n")
			}
			count++
		}
		Printf("\n")
	}
	meta_blob_2_snap = nil
	to_be_sorted = nil
}

// map data blob to containing meta blob, position and offset
// debug function CONTENTS
func PrintContentIndex(repositoryData *RepositoryData, options *WplDumpOptions) {
	// create map_data to contain triple(meta_blob, position, offset)

	data_map_org := mapset.NewSet[CompIndexOffet]()
	for _, cmp_ix_set := range MakeFullContentsMap2(repositoryData) {
		for cmp_ix := range cmp_ix_set.Iter() {
			data_map_org.Add(cmp_ix)
		}
	}
	data_map := data_map_org.ToSlice()

	sort.Slice(data_map, func(i, j int) bool {
		if data_map[i].data_blob_str < data_map[j].data_blob_str {
			return true
		} else if data_map[i].data_blob_str > data_map[j].data_blob_str {
			return false
		} else {
			return data_map[i].meta_blob_str < data_map[j].meta_blob_str
		}
	})

	// this filter works on meta_blobs or data_blobs
	filter, filter_map, filter_string := filter_processing(options)
	Printf("\n*** Data Blob Mapping  (CONTENTS)  ***\n")
	Printf("%-12s %-12s %-8s %-6s %-8s %-8s %s\n", "data-blob", "meta-blob", "position",
		"offset", "csize", "uc-size", "--- path ---")
	for _, elem := range data_map {
		if filter && ((filter_map["d"] && elem.data_blob_str != filter_string) ||
			(filter_map["m"] && elem.meta_blob_str != filter_string)) {
			continue
		}
		dir_path := repositoryData.FullPath[repositoryData.BlobToIndex[elem.meta_blob]][2:]
		ih := repositoryData.IndexHandle[elem.data_blob]
		Printf("%s %s %8d %6d %8d %8d %s/%s\n", elem.data_blob_str, elem.meta_blob_str,
			elem.position, elem.offset, ih.size, ih.UncompressedLength, dir_path, elem.name)
	}
}

type MetaBlobKeys struct {
	meta_blob     IntID
	meta_blob_str string
}

// extract file data - debug function FILEDATA
func PrintFileData(repositoryData *RepositoryData, options *WplDumpOptions) {
	// extract from directory_map map[IntID][]BlobFile2
	// generate sorted keys
	meta_blob_keys := make([]MetaBlobKeys, 0, len(repositoryData.DirectoryMap))
	for meta_blob := range repositoryData.DirectoryMap {
		if meta_blob == EMPTY_NODE_ID_TRANSLATED { continue }

		var meta_blob_str string
		if ! options.fullID {
			meta_blob_str = repositoryData.IndexToBlob[meta_blob].String()[:12]
		} else {
			meta_blob_str = repositoryData.IndexToBlob[meta_blob].String()
		}
		data := MetaBlobKeys{meta_blob: meta_blob, meta_blob_str: meta_blob_str}
		meta_blob_keys = append(meta_blob_keys, data)
	}

	// sort
	sort.Slice(meta_blob_keys, func(i, j int) bool {
		return meta_blob_keys[i].meta_blob_str < meta_blob_keys[j].meta_blob_str
	})

	// this filter works on meta_blobs
	filter, filter_map, filter_string := filter_processing(options)

	Printf("\n*** Tree Data Storage  (FILEDATA) ***\n")
	for _, entry := range meta_blob_keys {
		meta_blob := entry.meta_blob
		meta_blob_str := entry.meta_blob_str
		if filter && filter_map["m"] && meta_blob_str != filter_string {
			continue
		}
		file_sub_tree := repositoryData.DirectoryMap[meta_blob]
		Printf("\ntree %s loaded with %4d elements %s\n", entry.meta_blob_str,
			len(file_sub_tree), repositoryData.FullPath[meta_blob][2:])
		for ix, meta := range file_sub_tree {
			if meta.Type != "dir" {
				Printf("%4d %s %9d %s\n", ix, meta.mtime.String()[:19], meta.size, meta.name)
			} else {
				subtree := repositoryData.IndexToBlob[meta.subtree_ID].String()[:12]
				Printf("%4d %s %9d %s/  (%s)\n", ix, meta.mtime.String()[:19], meta.size,
					meta.name, subtree)
			}
		}
	}
	meta_blob_keys = nil
}

// print the topology tree - debug function TREE
func PrintTree(repositoryData *RepositoryData, options *WplDumpOptions) {
	// we need to sort the snapshot, because they contain the root of the
	// individual trees
	seen := mapset.NewSet(EMPTY_NODE_ID_TRANSLATED)
	to_be_sorted := repositoryData.Snaps[:]
	sort.Slice(to_be_sorted, func(i, j int) bool {
		if to_be_sorted[i].Hostname < to_be_sorted[j].Hostname {
			return true
		} else if to_be_sorted[i].Hostname > to_be_sorted[j].Hostname {
			return false
		} else if to_be_sorted[i].Paths[0] < to_be_sorted[j].Paths[0] {
			return true
		} else if to_be_sorted[i].Paths[0] > to_be_sorted[j].Paths[0] {
			return false
		} else {
			return to_be_sorted[i].Time.Before(to_be_sorted[j].Time)
		}
	})

	// this filter works on snap_ids
	filter, filter_map, filter_string := filter_processing(options)
	Printf("\n*** Topoplogoy Tree  (TREE) ***\n")
	for _, sn := range to_be_sorted {
		if filter && filter_map["s"] && sn.ID().Str() != filter_string {
			continue
		}
		root := sn.Tree
		Printf("\nsnap %s at %s -- %s:%s\n", sn.ID().Str(), sn.Time.String()[:19],
			sn.Hostname, sn.Paths[0])
		PrintSubTree(repositoryData.BlobToIndex[*root], root, seen, repositoryData, options, 0)
	}
	seen = nil
	to_be_sorted = nil
}

func PrintSubTree(blob IntID, ID *restic.ID, seen mapset.Set[IntID],
	repositoryData *RepositoryData, options *WplDumpOptions, level int) {
	// print node, then descend if not seen before, other print ... and return
	SIZE := 12
	if options.fullID { SIZE = 64 }

	if blob == EMPTY_NODE_ID_TRANSLATED { return }
	has_been_seen := seen.Contains(blob)
	filename := repositoryData.FullPath[blob]
	if len(filename) > 2 { filename = filename[2:] }

	Printf("%s %s ", ID.String()[:SIZE], filename)
	if has_been_seen {
		Printf("...\n")
	} else {
		Print("\n")
		seen.Add(blob)
		// print all files before all directories
		if options.more_data {
			for _, node := range repositoryData.DirectoryMap[blob] {
				if node.Type == "file" {
					Printf("     %7d %s\n", node.size, node.name)
				}
			}
		}
		for _, node := range repositoryData.DirectoryMap[blob] {
			if node.Type == "dir" {
				sub_tree := node.subtree_ID
				PrintSubTree(sub_tree, &repositoryData.IndexToBlob[sub_tree], seen,
					repositoryData, options, level+1)
			}
		}
	}
}

type SortableMore struct {
	meta_blob     restic.ID
	data_blob     restic.ID
	position      int
	offset        int
	pack_ID_str   string
	meta_blob_str string
	data_blob_str string
	name          string
	size          int
}

// process debug function PACK2TREE
func PrintPackTree(repositoryData *RepositoryData, options *WplDumpOptions) {

	/* The equivalent sql statement for the SQLite database
	SELECT DISTINCT
		substr(packfiles.packfile_id, 1, 12) AS pack_ID,
		substr(ix_repo_data.idd, 1, 12)      AS data_ID,
		substr(ix_repo_meta.idd, 1, 12)      AS meta_ID,
		fullname.pathname                    AS path
	FROM packfiles
	JOIN index_repo AS ix_repo_data
	JOIN index_repo AS ix_repo_meta
	JOIN fullname
	JOIN dir_path_id
	JOIN contents
	JOIN idd_file
	WHERE
		dir_path_id.id_pathname = fullname.id       AND
		dir_path_id.id          = ix_repo_meta.id   AND
		contents.id_data_idd    = ix_repo_data.id   AND
		contents.position       = idd_file.position AND
		contents.id_blob        = idd_file.id_blob  AND
		ix_repo_meta.id         = idd_file.id_blob  AND
		ix_repo_data.id_pack_id = packfiles.id
	ORDER BY pack_ID, data_ID, meta_ID;
	*/

	Printf("\n*** Data Packfile To Tree Mapping  (PACK2TREE) ***\n")
	filter, filter_map, filter_string := filter_processing(options)

	// step 1: get a full map of the contents metadata
	// full_contents_map is a map[restic.ID]mapset.Set[CompIndexOffet]
	full_contents_map := MakeFullContentsMap2(repositoryData)

	// step 2: gather packfile IDs and attached data blobs
	pack_2_blob := make(map[restic.ID]mapset.Set[restic.ID])
	for ID, ih := range repositoryData.IndexHandle {
		if filter && filter_map["d"] && ID.String()[:12] != filter_string {
			continue
		}

		// step 3: fill set
		if ih.Type == restic.DataBlob {
			pack_ID := repositoryData.IndexToBlob[ih.pack_index]
			if filter && filter_map["p"] && pack_ID.String()[:12] != filter_string {
				continue
			}
			if _, ok := pack_2_blob[pack_ID]; !ok {
				pack_2_blob[pack_ID] = mapset.NewSet[restic.ID]()
			}
			pack_2_blob[pack_ID].Add(ID)
		}
	}

	// step 4: prepare the data to be sorted
	if options.more_data {
		Printf("%-12s %-12s %-12s %5s %5s %8s %s\n", "packfile", "data_blob", "meta_blob",
			"pos", "offst", "size", "--- path ---")
	} else {
		Printf("%-12s %-12s %-12s %s\n", "packfile", "data_blob", "meta_blob",
			"--- directory path ---")
	}

	sorted_info := make([]SortableMore, 0)
	for pack_ID := range pack_2_blob {
		pack_ID_str := pack_ID.String()[:12]
		for data_blob := range pack_2_blob[pack_ID].Iter() {
			full_data := full_contents_map[data_blob].ToSlice()[0]
			meta_blob := full_data.meta_blob
			size := repositoryData.IndexHandle[data_blob].size
			if filter && filter_map["m"] && meta_blob.String()[:12] != filter_string {
				continue
			}
			new_entry := SortableMore{
				pack_ID_str:   pack_ID_str,
				meta_blob:     meta_blob,
				name:          full_data.name,
				meta_blob_str: meta_blob.String()[:12],
				data_blob_str: data_blob.String()[:12],
				position:      full_data.position,
				offset:        full_data.offset,
				size:          size,
			}
			sorted_info = append(sorted_info, new_entry)
		}
	}

	sorting := options.sorting
	sort.Slice(sorted_info, func(i, j int) bool {
		if sorting == "" || sorting == "m" {
			if sorted_info[i].meta_blob_str < sorted_info[j].meta_blob_str {
				return true
			} else if sorted_info[i].meta_blob_str > sorted_info[j].meta_blob_str {
				return false
			} else if sorted_info[i].position < sorted_info[j].position {
				return true
			} else if sorted_info[i].position > sorted_info[j].position {
				return false
			} else {
				return sorted_info[i].offset < sorted_info[j].offset
			}
		} else if sorting == "p" {
			if        sorted_info[i].pack_ID_str < sorted_info[j].pack_ID_str {
				return true
			} else if sorted_info[i].pack_ID_str > sorted_info[j].pack_ID_str {
				return false
			} else if sorted_info[i].meta_blob_str < sorted_info[j].meta_blob_str {
				return true
			} else if sorted_info[i].meta_blob_str > sorted_info[j].meta_blob_str {
				return false
			} else if sorted_info[i].position < sorted_info[j].position {
				return true
			} else if sorted_info[i].position > sorted_info[j].position {
				return false
			} else {
				return sorted_info[i].offset < sorted_info[j].offset
			}
		}	else {
			return sorted_info[i].data_blob_str < sorted_info[j].data_blob_str
		}
	})

	// big print loop
	for _, data := range sorted_info {
		path := repositoryData.FullPath[repositoryData.BlobToIndex[data.meta_blob]]
		if options.more_data {
			Printf("%s %s %s %5d %5d %8d %s/%s\n", data.pack_ID_str, data.data_blob_str, data.meta_blob_str,
				data.position, data.offset,data.size, path, data.name)
		} else {
			Printf("%s %s %s %s\n", data.pack_ID_str, data.data_blob_str, data.meta_blob_str,
				path)
		}
	}

	if ! options.more_data {
		return
	}

	// check some funny multiplicity records for more details
	Printf("\n*** check funny multiplicity records ***\n")
	for data_blob, data_sets := range full_contents_map {
		multi := data_sets.Cardinality()
		if multi <= 1 {
			continue
		}

		if repositoryData.IndexHandle[data_blob].size == 0 {
			continue
		}

		data_slice := data_sets.ToSlice()
		entry_0 := data_slice[0]
		offset  := entry_0.offset
		path    := repositoryData.FullPath[repositoryData.BlobToIndex[entry_0.meta_blob]]
		name    := entry_0.name
		for ix, elem := range data_slice[1:] {
			cpath := repositoryData.FullPath[repositoryData.BlobToIndex[elem.meta_blob]]
			if elem.offset != offset || cpath != path || name != elem.name {
				Printf("  0 %12s %12s o=%5d %s/%s\n", elem.data_blob_str, entry_0.meta_blob_str, offset, path, name)
				Printf("%3d %12s %12s o=%5d %s/%s\n", ix+1, elem.data_blob_str, elem.meta_blob_str, elem.offset, cpath, elem.name)
				break
			}
		}
	}
	full_contents_map = nil
}

// map debugging index to debugging name
var DEBUG_NAMES = map[int]string{
	SNAPSHOTS: "SNAPSHOTS",
	INDEX:     "INDEX",
	FILEDATA:  "FILEDATA",
	TREE:      "TREE",
	META_DIR:  "META_DIR",
	CONTENTS:  "CONTENTS",
	PACKFILES: "PACKFILES",
	PACK2TREE: "PACK2TREE",
	SUBTREE:   "SUBTREE",
	MULTIROOT: "MULTIROOT",
	DIRECTORY: "DIRECTORY",
	FULLPATH:  "FULLPATH",
	//DIRPATHID: "DIRPATHID",
}

var DEBUG_DESCRIPTION = map[int]string{
	SNAPSHOTS: "print snapshot details",
	INDEX:     "decode Master Index Handles",
	FILEDATA:  "decode tree records",
	TREE:      "show topology for each snapshot",
	META_DIR:  "prints which meta-blob is used in each snapshot",
	CONTENTS:  "relationship between data-blobs and meta-blobs and filedata",
	PACKFILES: "show packfiles and their lengths",
	PACK2TREE: "map packfiles to data-blobs, meta-blobs and directory names",
	SUBTREE:   "show the metadata for a given subtree",
	MULTIROOT: "show multiroots directories",
	DIRECTORY: "show all directories with type classification",
	FULLPATH:  "mapping between meta_blobs and full pathnames",
	//DIRPATHID: "direct mapping between meta_blobs and path_ids",
}

var DBG_FILTER_AVAILABLE = map[int]string{
	INDEX:     "blob and packfiles",
	FILEDATA:  "meta_blobs",
	TREE:      "meta_blobs",
	META_DIR:  "meta_blobs",
	CONTENTS:  "meta_blobs and data_blobs",
	PACK2TREE: "packfiles, meta_blobs, data_blobs -- can be collapsed",
}

var DBG_CONST_ORDER = []int{SNAPSHOTS, INDEX, PACKFILES, META_DIR, CONTENTS,
	FILEDATA, TREE, PACK2TREE, SUBTREE, MULTIROOT, DIRECTORY, FULLPATH}

type DebugCall func(*RepositoryData, *WplDumpOptions)

var DebugFunctions = map[int]DebugCall{
	SNAPSHOTS: PrintSnapshotsWpl,
	INDEX:     PrintIndexHandles,
	FILEDATA:  PrintFileData,
	META_DIR:  PrintMetaDirMapReverse,
	CONTENTS:  PrintContentIndex,
	TREE:      PrintTree,
	PACK2TREE: PrintPackTree,
	MULTIROOT: PrintMultiRoots,
	DIRECTORY: PrintDirectoriesClass,
	FULLPATH:  PrintFullPath,
}

var DEBUG_OPTIONS_REVERSE map[string]int

func PrintDebugOptions(debug int) {
	//make a map between option and text
	debug_name_keys := make([]int, 0)
	for key := range DEBUG_NAMES {
		debug_name_keys = append(debug_name_keys, key)
	}
	sort.Ints(debug_name_keys)

	debug_option_list := make([]string, 0)
	for _, key := range debug_name_keys {
		active := debug & key
		if active > 0 {
			result := fmt.Sprintf("%s(0x%04x)", DEBUG_NAMES[active], active)
			debug_option_list = append(debug_option_list, result)
		}
	}
	Printf("active options are %s\n", strings.Join(debug_option_list, ", "))
}

func process_debug_functions(options *WplDumpOptions, repositoryData *RepositoryData,
	repo restic.Repository, ctx context.Context, args []string) {
	// build reverse mapping debug-string -> debug-option
	DEBUG_OPTIONS_REVERSE = make(map[string]int)
	for dbg_opt, dbg_str := range DEBUG_NAMES {
		DEBUG_OPTIONS_REVERSE[dbg_str] = dbg_opt
	}
	if len(DEBUG_NAMES) != len(DEBUG_DESCRIPTION) ||
		len(DEBUG_DESCRIPTION) != len(DBG_CONST_ORDER) {
		Printf("length DEBUG_NAMES       %3d\n", len(DEBUG_NAMES))
		Printf("length DEBUG_DESCRIPTION %3d\n", len(DEBUG_DESCRIPTION))
		Printf("length DBG_CONST_ORDER   %3d\n", len(DBG_CONST_ORDER))
		Printf("length DebugFunctions    %3d\n", len(DebugFunctions))
		panic("internal inconsistency - check DEBUG configuration!")
	}

	debug_options_str := options.debug
	debug_options_int := 0
	re := regexp.MustCompile("^[a-fA-F0-9]+$")
	if re.FindString(debug_options_str) != "" {
		temp, _ := strconv.ParseInt(debug_options_str, 16, 64)
		debug_options_int = int(temp)
	} else {
		re = regexp.MustCompile(" ")
		// split on '|'
		debug_options_int = 0
		fail := false
		for _, dbg_str := range strings.Split(re.ReplaceAllString(debug_options_str, ""), "|") {
			dbg_str = strings.ToUpper(dbg_str)
			if dbg_opt, ok := DEBUG_OPTIONS_REVERSE[dbg_str]; !ok {
				Printf("debug_option '%s' not recognized, skipping\n", dbg_str)
				fail = true
			} else {
				debug_options_int |= dbg_opt
			}
		}
		if fail {
			debug_options_int = 0
		}
	}

	if debug_options_int == 0 {
		Printf("\nAvailable debug options are:\n")

		// make sure that debug names are sorted
		dbg_names := make([]string, 0, len(DEBUG_OPTIONS_REVERSE))
		for dbg_str := range DEBUG_OPTIONS_REVERSE {
			dbg_names = append(dbg_names, dbg_str)
		}
		sort.Strings(dbg_names)

		for _, order := range DBG_CONST_ORDER {
			Printf("0x%04x %-14s %s\n", order, DEBUG_NAMES[order],
				DEBUG_DESCRIPTION[order])
		}

		Printf("\nFilters can be applied to the following functions\n")
		for _, order := range DBG_CONST_ORDER {
			if descr, ok := DBG_FILTER_AVAILABLE[order]; ok {
				Printf("0x%04x %-14s %s\n", order, DEBUG_NAMES[order], descr)
			}
		}

		Printf("\ndebug options can be joined together with '|'\n")
		return
	} else {
		PrintDebugOptions(debug_options_int)
	}

	// execute one or more of the debug functions
	// we have to take care of the different parameter lists
	for _, order := range DBG_CONST_ORDER {
		if order == PACKFILES && (debug_options_int&order) > 0 {
			PrintPackfilesIndex(repositoryData, options, repo, ctx)
		} else if order == SUBTREE && (debug_options_int&order) > 0 {
			PrintSubTreeFromArgs(repositoryData, options, args)
		} else if (debug_options_int & order) > 0 {
			DebugFunctions[order](repositoryData, options)
		}
	}
}

// filter_processing checks for the existence of filter parameters and
// validates a minimal amount of correctness
func filter_processing(options *WplDumpOptions) (bool, map[string]bool, string) {
	filter := false
	filter_map := make(map[string]bool)
	filter_string := ""
	if options.include != "" {
		re := regexp.MustCompile("^[a-fA-F0-9]+$")
		if re.FindString(options.include) == "" {
			Printf("%s not a hex number, ignored!\n", options.include)
			filter = false
		} else {
			filter = true
			filter_string = options.include
		}
		check_filter_type(strings.ToLower(options.where), filter_map)
	}
	return filter, filter_map, filter_string
}

// check if filter type conforms to requirements
func check_filter_type(filter_type_raw string, filter_map map[string]bool) {
	// p=packfile, m=meta_blob, d=data_blob, s=snap, b=generic blob
	re := regexp.MustCompile("[pmdsb]")
	for ix := 0; ix < len(filter_type_raw); ix++ {
		letter := filter_type_raw[ix : ix+1]
		checked := re.FindString(letter)
		if checked != "" {
			filter_map[checked] = true
			if checked == "b" {
				filter_map["m"] = true
				filter_map["d"] = true
			}
		}
	}
}

func PrintSubTreeFromArgs(repositoryData *RepositoryData, options *WplDumpOptions,
	args []string) {
	for _, cli_ID := range args {
		cli_ID = strings.ToLower(cli_ID)
		len_ID := len(cli_ID)
		if len_ID < 8 {
			Printf("ID %s too short, give at least 8 characters\n", cli_ID)
			continue
		}

		// linear search through all meta blob records to find the correct tree
		for ID, ih := range repositoryData.IndexHandle {
			if ih.Type == restic.TreeBlob {
				ID_str := ID.String()
				if ID_str[:len_ID] == cli_ID {
					intID := repositoryData.BlobToIndex[ID]
					Printf("selected subtree %s is %s\n", cli_ID,
						repositoryData.FullPath[intID][2:])
					PrintSelectedSubtree(intID, repositoryData, options.recurse)
					break
				}
			}
		}
	} // end loop over cli args
}

// select a subtree and print its details
func PrintSelectedSubtree(intID IntID, repositoryData *RepositoryData, recurse bool) {
	Printf("%3s %19s %4s %10s %8s %4s %s\n", "idx", "date and time", "type", "size",
		"inode", "conts", "name")
	for ix, meta := range repositoryData.DirectoryMap[intID] {
		if meta.Type == "file" {
			Printf("%3d %s %4s %10d %8d %5d %s\n", ix, meta.mtime.String()[:19], meta.Type,
				meta.size, meta.inode, len(meta.content), meta.name)
		} else if meta.Type == "dir" {
			Printf("%3d %s %-4s %10s %8d %5s %s\n", ix, meta.mtime.String()[:19],
				meta.Type, "", meta.inode, "", meta.name)

			if recurse {
				cli_ID := repositoryData.IndexToBlob[meta.subtree_ID].String()[:12]
				Printf("selected subtree %s is %s\n", cli_ID,
					repositoryData.FullPath[meta.subtree_ID][2:])
				PrintSelectedSubtree(meta.subtree_ID, repositoryData, recurse)
			}
		}
	}
}

// print details about multi roots
func PrintMultiRoots(repositoryData *RepositoryData, options *WplDumpOptions) {

	children := CreateAllChildren(repositoryData)
	sum_children := 0
	parents := make(map[IntID]mapset.Set[IntID])
	max_mrix := 1
	count_leaf_directories := 0
	multi_root_stats := make(map[int]int) // map multiplicity to how often it appears
	multi_root_stats[1] = 0
	for parent, child_set := range children {
		sum_children += child_set.Cardinality()
		for child := range child_set.Iter() {
			if _, ok := parents[child]; ! ok {
				parents[child] = mapset.NewSet[IntID]()
			}
			parents[child].Add(parent)
		}
	}

	for child, parent_set := range parents {
		pc := parent_set.Cardinality()
		if pc > 1 {
			repositoryData.roots = append(repositoryData.roots, RootOfTree{
				meta_blob: child, multiplicity: int16(parent_set.Cardinality()),
				name: repositoryData.FullPath[child][2:],
			})

			if _, ok := multi_root_stats[pc]; ! ok {
				multi_root_stats[pc] = 0
			}
			multi_root_stats[pc]++

			// find maximum multiplicity
			if pc > max_mrix {
				max_mrix = pc
			}
		} else {
			multi_root_stats[1]++
			if children[child].Cardinality() == 0 {
				count_leaf_directories++
			}
		}
	}

	Printf("%-25s %10d\n", "parents", len(parents))
	Printf("%-25s %10d\n", "all children", sum_children)
	Printf("%-25s %10d\n", "start of subtrees", len(repositoryData.roots))
	Printf("%-25s %10d\n", "count_leaf_directories", count_leaf_directories)

	if options.children > 0 {
		max_children := dump_options.children
		to_be_sorted := repositoryData.roots[:]

		// sort by descending 'multiplicity', then by 'name'
		sort.SliceStable(to_be_sorted, func(i, j int) bool {
			if        to_be_sorted[i].multiplicity > to_be_sorted[j].multiplicity {
				return true
			} else if to_be_sorted[i].multiplicity < to_be_sorted[j].multiplicity {
				return false
			} else {
				return to_be_sorted[i].name < to_be_sorted[j].name
			}
		})

		Printf("\n*** start of subtrees ***\n")
		for offset, element := range to_be_sorted {
			if offset > max_children {
				break
			}
			Printf("%s %3d %s\n", repositoryData.IndexToBlob[element.meta_blob].String()[:12],
			element.multiplicity, element.name)
		}
	}

	if options.multi_root_stats {
		Printf("\n*** Multiplicity counts ***\n")
		for ix:= 1; ix <= max_mrix; ix++ {
			if count, ok := multi_root_stats[ix]; ok {
				Printf("multiplicity %3d appears %5d times\n", ix, count)
			}
		}
	}
	multi_root_stats = nil
}

// go through all the directories as a flat list, classify them and print
// dump funtion DIRECTORY
func PrintDirectoriesClass(repositoryData *RepositoryData, options *WplDumpOptions) {

	const (
		CLASS_ROOT   = 1 << iota // 1
		CLASS_SINGLE = 1 << iota // 2
		CLASS_MULTI  = 1 << iota // 4
		CLASS_LEAF   = 1 << iota // 8
	)

	type Printable struct {
		meta_blob_int IntID
		meta_blob_str string
		name          string
		class         int8
		multiplicity  int16
		mbstr_parent  string
		mbstr_gchild  string
	}

	// step 1: create parent relationship
	sum_children := 0
	children := CreateAllChildren(repositoryData)
	parents := make(map[IntID]mapset.Set[IntID])
	fullpath := make(map[IntID]string)
	max_mrix := 1
	count_leaf_directories := 0
	multi_root_stats := make(map[int]int) // map multiplicity to how often it appears
	multi_root_stats[1] = 0
	to_be_sorted := make([]Printable, 0, len(children))
	for parent, child_set := range children {
		sum_children += child_set.Cardinality()
		for child := range child_set.Iter() {
			if _, ok := parents[child]; ! ok {
				parents[child] = mapset.NewSet[IntID]()
			}
			parents[child].Add(parent)
		}
	}

	// step 2: create snap tree roots
	var class int8
	for _, sn := range repositoryData.Snaps {
		meta_blob_int := repositoryData.BlobToIndex[*sn.Tree]
		grand_child := children[meta_blob_int].ToSlice()[0]
		//snap_id := sn.ID().Str()
		to_be_sorted = append(to_be_sorted, Printable{
			meta_blob_int: meta_blob_int,
			meta_blob_str: (*sn.Tree).String()[:12],
			name:          repositoryData.FullPath[meta_blob_int],
			class:         int8(CLASS_ROOT | CLASS_LEAF),
			multiplicity:  int16(1),
			mbstr_gchild:  repositoryData.FullPath[grand_child][2:] + " [" +
				repositoryData.IndexToBlob[grand_child].String()[:12] + "]",
		})
	}

	// step 3: need a mapping from meta_blob_int back to snap_id
	meta_dir_map_reverse := MakeMetaDirMapReverse(repositoryData)

	// step 4: create directory names
	var (
		know_snap bool
		snap_id, new_path, mbstr_gchild string
		grand_child IntID
	)
	for meta_blob_int, path := range repositoryData.FullPath {
		if meta_dir_map_reverse[meta_blob_int].Cardinality() == 1 {
			know_snap = true
			snap_id = meta_dir_map_reverse[meta_blob_int].ToSlice()[0]
		} else {
			know_snap = false
		}

		if len(path) > 2 {
			if know_snap {
				new_path = path[2:] + " (of " + snap_id + ")"
			} else {
				new_path = path[2:]
			}
		}
		fullpath[meta_blob_int] = new_path
	}

	// step 5: collect all data about current child, parents and single children
	var gchild_str string
	for child, parent_set := range parents {
		pc := parent_set.Cardinality()
		if children[child].Cardinality() == 1 {
			count_leaf_directories++
			class = CLASS_LEAF
			grand_child = children[child].ToSlice()[0]
			gchild_str = fullpath[grand_child] + " [" +
					repositoryData.IndexToBlob[grand_child].String()[:12] +"]"
		} else {
			class = int8(0)
			gchild_str = ""
		}

		if pc > 1 {
			if _, ok := multi_root_stats[pc]; ! ok {
				multi_root_stats[pc] = 0
			}
			multi_root_stats[pc]++

			to_be_sorted = append(to_be_sorted, Printable{
				meta_blob_int: child,
				meta_blob_str: repositoryData.IndexToBlob[child].String()[:12],
				mbstr_gchild:  gchild_str,
				name:          fullpath[child],
				class:         int8(CLASS_MULTI) | class,
				multiplicity:  int16(pc),
			})

			// find maximum multiplicity
			if pc > max_mrix {
				max_mrix = pc
			}
		} else {
			multi_root_stats[1]++
			parent := parent_set.ToSlice()[0]
			to_be_sorted = append(to_be_sorted, Printable{
				meta_blob_int: child,
				meta_blob_str: repositoryData.IndexToBlob[child].String()[:12],
				name:          fullpath[child],
				class:         int8(CLASS_SINGLE) | class,
				multiplicity:  int16(1),
				mbstr_parent:  repositoryData.IndexToBlob[parent].String()[:12],
				mbstr_gchild:  fullpath[grand_child] + " [" +
				  repositoryData.IndexToBlob[grand_child].String()[:12] +"]",
			})
		}
	}

	// count MULTI / SINGLE
	count_single := 0
	count_multi  := 0
	for _, parent_set := range parents {
		pc := parent_set.Cardinality()
		if pc == 1 {
			count_single++
		} else {
			count_multi++
		}
	}

	// count meta_dir_reverse entries
	count_meta_dir := 0
	for _, sett := range meta_dir_map_reverse {
		count_meta_dir += sett.Cardinality()
	}

	// report
	Printf("%-25s %10d\n", "parents", len(parents))
	Printf("%-25s %10d\n", "all children", sum_children)
	Printf("%-25s %10d\n", "all meta_dir", count_meta_dir)
	Printf("%-25s %10d\n", "count roots", len(repositoryData.Snaps))
	Printf("%-25s %10d\n", "count single", count_single)
	Printf("%-25s %10d\n", "count multi", count_multi)
	Printf("%-25s %10d\n", "count_leaf_directories", count_leaf_directories)

	// sort the lot by ascending meta_blob_id
	sort.SliceStable(to_be_sorted, func (i, j int) bool {
			return to_be_sorted[i].meta_blob_str < to_be_sorted[j].meta_blob_str
	})

	// print first list
	Printf("\n*** directories sorted by ID ***\n")
	Printf("%-12s class multi --- directory path ---\n", "meta_blob")
	class_str := make([]byte, 4, 4)
	//var parent_str string
	for _, elem := range to_be_sorted {
		for ix := 0; ix < 4; ix++ {
			class_str[ix] = ' '
		}
		if        (elem.class & CLASS_LEAF) == CLASS_LEAF {
			class_str[3] = 'L'
		}
		if        (elem.class & CLASS_ROOT) == CLASS_ROOT {
			class_str[0] = 'R'
		} else if (elem.class & CLASS_SINGLE) == CLASS_SINGLE {
			class_str[2] = 'S'
		} else if (elem.class & CLASS_MULTI) == CLASS_MULTI {
			class_str[1] = 'M'
		}

		Printf("%12s %s %5d %s\n", elem.meta_blob_str, string(class_str), elem.multiplicity,
			elem.name)
	}

	// sort the lot again, this time by ascending name
	sort.SliceStable(to_be_sorted, func (i, j int) bool {
			return to_be_sorted[i].name < to_be_sorted[j].name
	})

	// print second list
	Printf("\n*** directories sorted by directory path ***\n")
	Printf("%-12s class multi --- directory path ---\n", "meta_blob")
	for _, elem := range to_be_sorted {
		for ix := 0; ix < 4; ix++ {
			class_str[ix] = ' '
		}
		mbstr_gchild = ""
		if        (elem.class & CLASS_LEAF) == CLASS_LEAF {
			class_str[3] = 'L'
			mbstr_gchild = elem.mbstr_gchild
		}
		if        (elem.class & CLASS_ROOT) == CLASS_ROOT {
			class_str[0] = 'R'
		} else if (elem.class & CLASS_SINGLE) == CLASS_SINGLE {
			class_str[2] = 'S'
			//parent_str = "[" + elem.mbstr_parent + "]"
		} else if (elem.class & CLASS_MULTI) == CLASS_MULTI {
			class_str[1] = 'M'
		}

		if class_str[3] == 'L' {
			Printf("%12s %s %5d %s -> %s\n", elem.meta_blob_str, string(class_str), elem.multiplicity,
				elem.name, mbstr_gchild)
		} else {
			Printf("%12s %s %5d %s\n", elem.meta_blob_str, string(class_str), elem.multiplicity,
				elem.name)
		}
	}
	meta_dir_map_reverse = nil
	parents = nil
	fullpath = nil
	to_be_sorted = nil
}

// debug function FULLPATH
func PrintFullPath(repositoryData *RepositoryData, options *WplDumpOptions) {
	Printf("\n*** Fullpath analysis ***\n")

	// fullpath is map[IntID]string
	map_path_to_blob := make(map[string]mapset.Set[IntID])
	for meta_blob_int, path := range repositoryData.FullPath {
		meta_blob := repositoryData.IndexToBlob[meta_blob_int]
		if len(path) < 3 {
			path = "/./."
		}
		Printf("%s %s\n", meta_blob.String()[:12], path[2:])
		if _, ok := map_path_to_blob[path]; ! ok {
			map_path_to_blob[path] = mapset.NewSet[IntID]()
		}
		map_path_to_blob[path].Add(meta_blob_int)
	}

	// investigate repositoryData.DirectoryMap for duplicate names
	// note: all the tree roots get mapped to the name "/. "which is outside
	// of the checks done here.
	Printf("\n*** directories with double names ***\n")
	checks := make(map[IntID]mapset.Set[string])
	for _, idd_file_list := range repositoryData.DirectoryMap {
		for _, node := range idd_file_list {
			if node.subtree_ID == EMPTY_NODE_ID_TRANSLATED {
				// catches all files as well
				continue
			}
			// new blob into 'checks'
			if _, ok := checks[node.subtree_ID]; !ok {
				checks[node.subtree_ID] = mapset.NewSet[string]()
			}
			checks[node.subtree_ID].Add(node.name)
		}
	}

	// find muliple entries
	for sub_tree_int, sett := range checks {
		card := sett.Cardinality()
		if card == 1 {
			continue
		} else if card == 2 {
			names := sett.ToSlice()
			Printf("dir: %s From_name: \"%s\", To_name: \"%s\"\n",
				repositoryData.IndexToBlob[sub_tree_int].String()[:12],
				names[0], names[1])
		} else {
			Printf("muliplicity for %7d is %5d\n", sub_tree_int, card)
		}

		Printf("\n")
	}
}

