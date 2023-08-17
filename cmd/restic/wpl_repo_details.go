package main

// collect some globals stats about this repository

import (
	// system
	"context"
	"sort"
	"strings"
	"io/fs"
	"path/filepath"

	// sha256
	"crypto/sha256"
	"encoding/binary"
	"bytes"

	//argparse
	"github.com/spf13/cobra"

	// restic library
	"github.com/wplapper/restic/library/restic"
	"github.com/wplapper/restic/library/pack"

	// sets
	"github.com/deckarep/golang-set/v2"
)

type RepoDetailsOptions struct {
	same_tree  bool
	check_data bool
	prune      bool
	detail     int
	lost       bool
	timing     bool
	memory_use bool
}

var repo_details_options RepoDetailsOptions

var cmdRepoDetails = &cobra.Command{
	Use:   "repo-details [flags]",
	Short: "counts various tables and reort usage",
	Long: `counts various tables and reort usage.

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
	f.BoolVarP(&repo_details_options.same_tree, "same-tree", "S", false, "show snapshots with the same tree")
	f.BoolVarP(&repo_details_options.check_data, "check-data", "C", false, "compare list of all data file with packfiles")
	f.BoolVarP(&repo_details_options.prune, "prune", "P", false, "make prune calculations")
	f.BoolVarP(&repo_details_options.timing, "timing", "T", false, "produce timings")
	f.BoolVarP(&repo_details_options.memory_use, "memory", "M", false, "produce memory usage")
	f.CountVarP(&repo_details_options.detail, "detail", "D", "print dir/file details")
}

func runRepoDetails(ctx context.Context, cmd *cobra.Command, gopts GlobalOptions) error {

	var repositoryData RepositoryData

	// startup
	gOptions = gopts
	init_repositoryData(&repositoryData)

	// step 1: open repository
	repo, err := OpenRepository(ctx, gopts)
	if err != nil {
		return err
	}
	Verboseff("Repository is %s\n", globalOptions.Repo)

	// step 2: gather the base information
	err = gather_base_data_repo(repo, gopts, ctx, &repositoryData, false)
	if err != nil {
		return err
	}

	// step 3: decide what to do
	if repo_details_options.same_tree {
		findSameTree(ctx, repo, &repositoryData)
		return nil
	} else if repo_details_options.check_data {
		check_data(ctx, repo, &repositoryData, gopts.Repo)
		return nil
	}

	// normal processing
	countBlobs(ctx, repo, &repositoryData)
	countFiles(ctx, repo, &repositoryData)
	countTables(ctx, repo, &repositoryData, repo_details_options)
	return nil
}

// go through index table and count tree and data blobs, and compressed and
// uncompressed sizes
func countBlobs(ctx context.Context, repo restic.Repository, repositoryData *RepositoryData) {
	count_tree_blobs := 0
	count_data_blobs := 0
	size_tree_blobs := 0
	size_data_blobs := 0
	uc_size_tree_blobs := 0
	uc_size_data_blobs := 0
	pack_set_meta := mapset.NewSet[restic.ID]()
	pack_set_data := mapset.NewSet[restic.ID]()

	// extract packfile information
	repo.Index().Each(ctx, func(blob restic.PackedBlob) {
		if blob.Type == restic.TreeBlob {
			count_tree_blobs++
			size_tree_blobs += int(blob.Length)
			uc_size_tree_blobs += int(blob.UncompressedLength)
			pack_set_meta.Add(blob.PackID)
		} else if blob.Type == restic.DataBlob {
			count_data_blobs++
			size_data_blobs += int(blob.Length)
			uc_size_data_blobs += int(blob.UncompressedLength)
			pack_set_data.Add(blob.PackID)
		}
	})

	// extract packfiles with their sizes from the Master Index
	// 'repo_packs' is map[restic.ID]int64
	repo_packs := pack.Size(ctx, repo.Index(), false)

	// get sizes for meta and data packfiles
	size_meta_pack := int64(0)
	for ID := range pack_set_meta.Iter() {
		size, ok := repo_packs[ID]
		if !ok {
			Printf("meta ID not matched %v\n", ID)
			panic("size not found")
		}
		size_meta_pack += size
	}

	size_data_pack := int64(0)
	for ID := range pack_set_data.Iter() {
		size, ok := repo_packs[ID]
		if !ok {
			Printf("data ID not matched %v\n", ID)
			panic("size not found")
		}
		size_data_pack += size
	}

	// report
	Printf("\n*** Global counts ***\n")
	Printf("%-25s %10d  %10.1f MiB\n", "  compressed tree blobs", count_tree_blobs,
		float64(size_tree_blobs) / ONE_MEG)
	Printf("%-25s %10d  %10.1f MiB\n", "  compressed data blobs", count_data_blobs,
		float64(size_data_blobs) / ONE_MEG)
	Printf("%-25s %10d  %10.1f MiB\n", "  compressed ALL  blobs",
		count_data_blobs + count_tree_blobs,
		float64(size_tree_blobs + size_data_blobs) / ONE_MEG)

	Printf("%-25s %10d  %10.1f MiB\n", "uncompressed tree blobs", count_tree_blobs,
		float64(uc_size_tree_blobs) / ONE_MEG)
	Printf("%-25s %10d  %10.1f MiB\n", "uncompressed data blobs", count_data_blobs,
		float64(uc_size_data_blobs) / ONE_MEG)
	Printf("%-25s %10d  %10.1f MiB\n", "uncompressed All  blobs",
		count_data_blobs + count_tree_blobs,
		float64(uc_size_tree_blobs + uc_size_data_blobs) / ONE_MEG)

	Printf("%-25s %10d  %10.1f MiB\n", "meta packfiles",
		pack_set_meta.Cardinality(),
		float64(size_meta_pack) / ONE_MEG)
	Printf("%-25s %10d  %10.1f MiB\n", "data packfiles",
		pack_set_data.Cardinality(),
		float64(size_data_pack) / ONE_MEG)
	Printf("%-25s %10d  %10.1f MiB\n", "ALL  packfiles",
		pack_set_meta.Cardinality() + pack_set_data.Cardinality(),
		float64(size_meta_pack + size_data_pack) / ONE_MEG)

	// reset
	repo_packs = nil
	pack_set_data = nil
	pack_set_meta = nil
}

type DeviceInode struct {
	DeviceID uint64
	Inode    uint64
}

// count index, snapshot and meta file counts and sizes
func countFiles(ctx context.Context, repo restic.Repository, repositoryData *RepositoryData) {
	count_snaps := 0
	size_snaps  := int64(0)

	// snapshot files
	tree_set := mapset.NewSet[restic.ID]()
	repo.List(ctx, restic.SnapshotFile, func(id restic.ID, size int64) error {
		sn, err := restic.LoadSnapshot(ctx, repo, id)
		if err != nil {
			Printf("Skip loading snap record %s! - reason: '%v'\n", id, err)
			return nil
		}
		count_snaps++
		size_snaps += size
		tree_set.Add(*(sn.Tree))
		return nil
	})

	// index files
	count_index := 0
	size_index  := int64(0)
	repo.List(ctx, restic.IndexFile, func(id restic.ID, size int64) error {
		count_index++
		size_index += size
		return nil
	})

	// report
	Printf("\n")
	Printf("%-25s %10d  %10.1f MiB\n", "index files", count_index,
	 float64(size_index) / ONE_MEG)
	Printf("%-25s %10d  %10.1f KiB\n", "snapshot files", count_snaps,
		float64(size_snaps) / 1024.0)
	Printf("%-25s %10d\n", "count trees", tree_set.Cardinality())
	Printf("%s\n", strings.Repeat("=", 52))

	// clear up
	tree_set = nil
}

// for contents mapping
type MetaPositionOffet struct {
	// the following triple uniquely identifies temp data blob
	meta_blob_int IntID // unique, part1
	position      int   // unique, part2
	offset        int   // unique, part3
}

// count table contents and other interesting information
func countTables(ctx context.Context, repo restic.Repository,
repositoryData *RepositoryData, options RepoDetailsOptions) {
	// meta_dir
	count_meta_dir_entries := 0
	for _, meta_blobs := range repositoryData.MetaDirMap {
		count_meta_dir_entries += meta_blobs.Cardinality()
	}

	// directory_map
	count_directory_map_entries := 0

	// we have tuple(node.DeviceID, node.Inode) which is unique
	type content_ID struct {
		content_ID [32]byte
		Size       uint64
	}
	inodes_meta := mapset.NewSet[DeviceInode]()
	inodes_data := mapset.NewSet[[sha256.Size]byte]()
	names       := mapset.NewSet[string]()

	// map inodes back to 'meta_blob_int'
	for _, file_list := range repositoryData.DirectoryMap {
		count_directory_map_entries += len(file_list)
		for _, meta := range file_list {
			device_inode := DeviceInode{Inode: meta.inode, DeviceID: meta.DeviceID}
			if meta.Type == "dir" {
				inodes_meta.Add(device_inode)
			} else if meta.Type == "file" {
				inodes_data.Add(convertContent2(meta.content))
			}
			names.Add(meta.name)
		}
	}

	// report 1
	Printf("\n")
	Printf("%-25s %10d\n", "sum meta_dir", count_meta_dir_entries)
	Printf("%-25s %10d\n", "sum directory_map", count_directory_map_entries)
	Printf("%-25s %10d\n", "meta inodes", inodes_meta.Cardinality())
	Printf("%-25s %10d\n", "data inodes", inodes_data.Cardinality())
	Printf("%-25s %10d\n", "base names", names.Cardinality())

	// report 2:
	// build full map for all data blobs
	// data_map_org is map[restic.ID]mapset.Set[CompIndexOffet]
	data_map_org := MakeFullContentsMap2(repositoryData)
	// build a flat set
	data_map := mapset.NewSet[CompIndexOffet]()
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
			ih := repositoryData.IndexHandle[data_blob]
			size_singles += int(ih.size)
		}
	}

	// report 3
	Printf("\n")
	Printf("%-25s %10d\n", "all content", len(data_map_org))
	Printf("%-25s %10d\n", "all content variations", data_map.Cardinality())
	Printf("%-25s %10d  %10.1f MiB\n", "singles in content", count_singles,
		float64(size_singles) / ONE_MEG)

	// check if repository needs pruning: more meta blobs in Index(), compared
	// to the union of all trees
	// full tree is mapped to repositoryData.FullPath
	// index is mapped repositoryData.IndexHandle
	set_index_handle := mapset.NewSet[IntID]()
	for _, ih := range repositoryData.IndexHandle {
		if ih.Type == restic.TreeBlob {
			set_index_handle.Add(ih.blob_index)
		}
	}
	set_index_handle.Remove(EMPTY_NODE_ID_TRANSLATED)

	set_tree := mapset.NewSet[IntID]()
	for meta_blob_int := range repositoryData.FullPath {
		set_tree.Add(meta_blob_int)
	}

	var diff mapset.Set[IntID]
	var l_diff int
	if ! set_index_handle.Equal(set_tree) {
		Printf("\n*** Repository needs pruning! ***\n")

		diff = set_tree.Difference(set_index_handle)
		l_diff = diff.Cardinality()
		if l_diff > 0 {
			Printf("tree  has %5d more records than the index_records\n", l_diff)
			panic("\n*** This is catastrophic inconsistency in the set management!! ***")
		}
		diff = set_index_handle.Difference(set_tree)
		l_diff = diff.Cardinality()

		if repo_details_options.prune {
			checkPrune(repositoryData, diff, options)
		}
	}

	// reset
	diff         = nil
	names        = nil
	inodes_meta  = nil
	inodes_data  = nil
	data_map     = nil
	data_map_org = nil
	set_tree     = nil
	set_index_handle = nil
}

// find snapshots which own the same tree (root)
func findSameTree(ctx context.Context, repo restic.Repository,
repositoryData *RepositoryData) {
	printed := false
	// map tree root to snap_id
	double := map[restic.ID]mapset.Set[string]{}
	for snap_id, sn := range repositoryData.SnapMap {
		if _, ok := double[*sn.Tree]; ! ok {
			double[*sn.Tree] = mapset.NewSet[string]()
		}
		double[*sn.Tree].Add(snap_id)
	}

	// check for set length > 1
	for _, snap_id_set := range double {
		if snap_id_set.Cardinality() == 1 {
			continue
		}

		// multiple owners
		if ! printed {
			printed = true
			Printf("\n*** Multiple snapshots for same tree ***\n")
		}

		// collect duplicate snapshots, sort and print
		owners := make([]*restic.Snapshot, 0, snap_id_set.Cardinality())
		for snap_id := range snap_id_set.Iter() {
			owners = append(owners, repositoryData.SnapMap[snap_id])
		}
		sort.SliceStable(owners, func(i, j int) bool {
			return owners[i].Time.Before(owners[j].Time)
		})

		for _, sn := range owners {
			Printf("%s - %s %s:%s\n", sn.ID().Str(), sn.Time.String()[:19], sn.Hostname,
				sn.Paths[0])
		}
	}

	// reset
	double = nil
}

// walk through temp locally attached repository and check all entries against the
// packfiles set
// this will only work on locally defined repositories where file system functions
// work.
func check_data(ctx context.Context, repo restic.Repository,
repositoryData *RepositoryData, root string) {
	// gather Packfiles names from Index data
	pack_set := mapset.NewSet[string]()
	repo.Index().Each(ctx, func(blob restic.PackedBlob) {
		pack_set.Add(blob.PackID.String())
	})

	// prepare to walk down the data subdirectory
	// Walk will fail if you try to Walk temp rclone structure
	// will only work on local filesystems
	missing := false
	err := filepath.Walk(root + "/data",
		func(path string, info fs.FileInfo, err error) error {
		// skip on error
		if err != nil {
			Printf("prevent panic by handling failure accessing path %q: '%v'\n",
				path, err)
			return err
		}

		// skip directories
		if info.IsDir() {
			return nil
		}

		basename := filepath.Base(path)
		Verboseff("checking %s\n", basename)
		if pack_set.Contains(basename) {
			return nil
		}

		Printf("missing entry %s\n", basename)
		missing = true
		return nil
	})

	if err != nil {
		Printf("Walk returned '%v'\n", err)
		return
	}

	if ! missing {
		Printf("all data entries are OK!\n")
	} else {
		Printf("errors detected!\n")
	}
}

// check for prune: 'meta_diff' only contains meta_blob records
func checkPrune(repositoryData *RepositoryData, meta_diff mapset.Set[IntID],
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

	// this amends the repositoryData.FullPath
	diagnoseAndRepairOrphans(meta_diff, repositoryData)

	// 1. - get all data blobs from Index()
	set_data_handle := mapset.NewSet[IntID]()
	for _, ih := range repositoryData.IndexHandle {
		if ih.Type == restic.DataBlob {
			set_data_handle.Add(ih.blob_index)
		}
	}

	// 2. - data all blobs from the mapped tree blobs - as in used blobs
	set_data_tree := mapset.NewSet[IntID]()
	for meta_blob_int := range fullpath {
		for _, meta := range repositoryData.DirectoryMap[meta_blob_int] {
			if meta.Type == "file" {
				set_data_tree.Append(meta.content ...)
			}
		}
	}

	// 3. - in order to make details useful for blobs which are marked "to be deleted"
	//      but are not pruned yet one has to discover the lost meta blobs
	diff_data := set_data_handle.Difference(set_data_tree)
	unused_blobs := diff_data.Union(meta_diff)
	SizePrune(repositoryData, unused_blobs, false, nil, options.detail)

	// 4. - report details if requested
	detail := options.detail
	data_map := map_data_blob_file(repositoryData)
	if detail == 4 {
		print_very_raw(repositoryData, unused_blobs)
	} else if detail == 3 {
		print_raw(repositoryData, unused_blobs, data_map)
	} else if detail == 2 || detail == 1 {
		print_some_detail(repositoryData, unused_blobs, detail, options.lost, data_map)
	}

	//5. - reset for GC
	fullpath = nil
	diff_data = nil
	unused_blobs = nil
	set_data_tree = nil
	set_data_handle = nil
}

// this function generates identifiable names for 'lost' meta_blobs and
// tries to derive meaningful names from subdirectories
// output: update repositoryData.FullPath entries
func diagnoseAndRepairOrphans(missing_blobs mapset.Set[IntID],
repositoryData *RepositoryData) {

	// step 1: build temp child -> parent table
	children := CreateAllChildren(repositoryData)
	parents  := map[IntID]IntID{}
	for parent, children := range children {
		for child := range children.Iter() {
			parents[child] = parent
		}
	}

	// step 2: from named children create named parents iteratively
	changed := true
	for changed {
		changed = false
		// here we are with orphaned meta blobs - check for parent -> child relationship
		for parent := range missing_blobs.Iter() {
			if _, ok := repositoryData.FullPath[parent]; ok {
				continue
			}
			for child := range children[parent].Iter() {
				if _, ok := repositoryData.FullPath[child]; ! ok {
					continue
				}

				// we have temp unnamed child
				components := strings.Split(repositoryData.FullPath[child], "/")
				lcomp := len(components)
				if lcomp  < 3 {
					continue
				}
				// construct new parent
				new_pathp := "/./" + strings.Join(components[2:lcomp-1], "/")
				//Verboseff("new_pathp %s\n", new_pathp)
				repositoryData.FullPath[parent] = new_pathp

				// check for grandparent, need to check length first
				if lcomp  < 4 {
					continue
				}
				grand_parent := parents[parent]
				// we found gp -> parent
				_, ok2 := repositoryData.FullPath[grand_parent]
				if ! ok2 {
					//Printf("components %+v\n", components)
					new_pathgp := "/./" + strings.Join(components[2:lcomp - 2], "/")
					repositoryData.FullPath[grand_parent] = new_pathgp
				}
				changed = true
			}
		}
	}

	// step 3: investigate parents which still have no name.
	// check if they have temp common parent, which has temp name
	// create name as <known-gp>/<hex-string>
	for parent := range missing_blobs.Iter() {
		if _, ok := repositoryData.FullPath[parent]; ! ok {
			gp := parents[parent]
			if gp_name, ok2 := repositoryData.FullPath[gp]; ok2 {
				repositoryData.FullPath[parent] = gp_name + "/" +
					repositoryData.IndexToBlob[parent].String()[:12]
			}
		}
	}

	// step 4: generate the rest of the names
	// output the directory names found as absolute directories
	//Verboseff("\n===================\n")
	for parent := range missing_blobs.Iter() {
		if parent == EMPTY_NODE_ID_TRANSLATED {
			continue
		}
		if _, ok := repositoryData.FullPath[parent]; ! ok {
			repositoryData.FullPath[parent] = "/./" + repositoryData.IndexToBlob[parent].String()[:12]
		}
	}
	parents = nil
}

// currently not used in favour of 'convertContent2'
// convert content slice to temp hash, so it can be compared against other content
func convertContentsToSha256(content []IntID) [sha256.Size]byte {
	buf := new(bytes.Buffer)
	for _, data_blob_int := range content {
		binary.Write(buf, binary.LittleEndian, data_blob_int)
	}
	return sha256.Sum256(buf.Bytes())
}

// serialize 'content' step by step into sha256 by Write() to it
func convertContent2(content []IntID) (result [sha256.Size]byte) {
	sha256sum := sha256.New()
	temp := make([]byte, 4)
	for _, data_blob_int := range content {
		//binary.LittleEndian.PutUint32(temp, data_blob_int): serialize uint32
		temp[0] = byte(data_blob_int)
		temp[1] = byte(data_blob_int >>  8)
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
