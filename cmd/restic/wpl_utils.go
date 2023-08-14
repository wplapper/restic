package main

import (
	// system
	"context"
	"fmt"
	"golang.org/x/sync/errgroup"
	"runtime"
	"sort"
	"sync"
	"time"
	"encoding/json"
	"io/ioutil"

	// restic library
	"github.com/wplapper/restic/library/restic"

	// sets
	"github.com/deckarep/golang-set/v2"

	// stack and queue
	"github.com/golang-collections/collections/stack"
	"github.com/golang-collections/collections/queue"
)

// system wide variables and containers
var (
	gOptions                 GlobalOptions
  db_aggregate             DBAggregate
	newComers                *Newcomers
	repositoryData           *RepositoryData
	EMPTY_NODE_ID            restic.ID
	EMPTY_NODE_ID_TRANSLATED IntID
)

func init_repositoryData(repositoryData *RepositoryData) {
	repositoryData.snaps = []*restic.Snapshot{}
	repositoryData.snap_map = make(map[string]*restic.Snapshot)
	repositoryData.directory_map = make(map[IntID][]BlobFile2)
	repositoryData.fullpath = make(map[IntID]string)
	repositoryData.names = make(map[IntID]string)
	repositoryData.meta_dir_map = make(map[*restic.ID]mapset.Set[IntID])
	repositoryData.index_handle = make(map[restic.ID]Index_Handle)

	repositoryData.blob_to_index = make(map[restic.ID]IntID)
	repositoryData.index_to_blob = []restic.ID{}
	repositoryData.children = make(map[IntID]mapset.Set[IntID])
}

// GatherAllSnapshots retrieves all snapshots from the repository
// return: slice of all *sn, sorted by ascending snapshot time (*sn.Time)
func GatherAllSnapshots(gopts GlobalOptions, ctx context.Context,
	repo restic.Repository) ([]*restic.Snapshot, map[string]*restic.Snapshot, error) {
	// collect all snap records
	snaps := make([]*restic.Snapshot, 0)
	repo.List(ctx, restic.SnapshotFile, func(id restic.ID, size int64) error {
		sn, err := restic.LoadSnapshot(ctx, repo, id)
		if err != nil {
			Printf("Skip loading snap record %s! - reason: %v\n", id, err)
			return err
		}
		snaps = append(snaps, sn)
		return nil
	})

	// custom sort: we sort 'snaps' by sn.Time
	sort.SliceStable(snaps, func(i, j int) bool {
		return snaps[i].Time.Before(snaps[j].Time)
	})

	// fill snap_map
	snap_map := make(map[string]*restic.Snapshot, len(snaps))
	for _, sn := range snaps {
		// fill snap_map
		snap_map[sn.ID().Str()] = sn
	}
	return snaps, snap_map, nil
}

func HandleIndexRecords(gopts GlobalOptions, ctx context.Context, repo restic.Repository,
	repositoryData *RepositoryData) error {
	// load index files and their contents
	// 'LoadIndex' is in library/repository/repository.go, needs to happen first
	if err := repo.LoadIndex(ctx); err != nil {
		Printf("repo.LoadIndex - failed. Error is %v\n", err)
		return err
	}

	Convert_to_IntSet(gopts, ctx, repo, repositoryData)
	return nil
}

// this function converts restic.ID to IntID, forth and back
// It also correlates blobs and pack IDs
// build 'repositoryData.blob_to_index' and 'repositoryData.index_to_blob'
// from 'repo.Index().Each(gopts.ctx)', containing 'blob.ID' and 'blob.PackID'
func Convert_to_IntSet(gopts GlobalOptions, ctx context.Context, repo restic.Repository,
	repositoryData *RepositoryData) {

	EMPTY_NODE_ID = restic.Hash([]byte("{\"nodes\":[]}\n"))
	// blob is a 'restic.PackedBlob' which contains
	// ID,  PackID, Type, Length and Offset
	// build 'blob_to_index' and 'index_to_blob' for all known restic.ID(s)
	// sources are the index files, packfiles and the snapshot records
	pos := IntID(0)

	// loop over each index record by calling unnamed func for every entry
	// 'blob' is set by 'Each'
	repo.Index().Each(ctx, func(blob restic.PackedBlob) {
		if _, ok := repositoryData.blob_to_index[blob.ID]; ! ok {
			repositoryData.blob_to_index[blob.ID] = pos
			repositoryData.index_to_blob = append(repositoryData.index_to_blob, blob.ID)
			pos++
		}

		// add packID from packfiles to 'blob_to_index' and 'index_to_blob',
		if _, ok := repositoryData.blob_to_index[blob.PackID]; ! ok {
			repositoryData.blob_to_index[blob.PackID] = pos
			repositoryData.index_to_blob = append(repositoryData.index_to_blob, blob.PackID)
			pos++
		}

		// build our index_handle from 'blob' record
		length := int(blob.Length)
		repositoryData.index_handle[blob.ID] = Index_Handle{Type: blob.Type, size: length,
			pack_index:         repositoryData.blob_to_index[blob.PackID],
			blob_index:         repositoryData.blob_to_index[blob.ID],
			UncompressedLength: int(blob.UncompressedLength),
		}
	})

	// add the *restic.IDs from the snapshots list to these list / maps
	for _, sn := range repositoryData.snaps {
		snap := *sn.ID()
		if _, ok := repositoryData.blob_to_index[snap]; ! ok {
			repositoryData.blob_to_index[snap] = pos
			repositoryData.index_to_blob = append(repositoryData.index_to_blob, snap)
			pos++
		}
	}

	// we need to set EMPTY_NODE_ID_TRANSLATED
	ok := false
	EMPTY_NODE_ID_TRANSLATED, ok = repositoryData.blob_to_index[EMPTY_NODE_ID]
	if !ok {
		repositoryData.blob_to_index[EMPTY_NODE_ID] = pos
		repositoryData.index_to_blob = append(repositoryData.index_to_blob, EMPTY_NODE_ID)
		EMPTY_NODE_ID_TRANSLATED = pos
	}
}

// FindChildren steps through the directory_map and finds subdirectories.
// The children get attached their parent
func FindChildren(repositoryData *RepositoryData) {
	//Printf("FindChildren start, number of parents %7d\n", len(repositoryData.directory_map))
	// 1. collect the children

	for parent, idd_file_list := range repositoryData.directory_map {
		if parent == EMPTY_NODE_ID_TRANSLATED {
			continue
		}

		repositoryData.children[parent] = mapset.NewSet[IntID]()
		for _, node := range idd_file_list {
			if node.subtree_ID == EMPTY_NODE_ID_TRANSLATED {
				continue
			}

			sub_node := node.subtree_ID
			name := node.name
			if _, ok := repositoryData.rename_children[name]; ok {
				//Printf("Need to rename from %s to %s?\n", name, target)
				// name = target
			}
			repositoryData.names[sub_node] = name
			repositoryData.children[parent].Add(sub_node)
		}
	}

	// 2. create tree root names for fullpath
	repositoryData.roots = make([]RootOfTree, 0, len(repositoryData.snaps))
	initials := mapset.NewSet[IntID]()
	for _, sn := range repositoryData.snaps {
		tree := repositoryData.blob_to_index[*sn.Tree]
		repositoryData.fullpath[tree] = "/."
		repositoryData.roots = append(repositoryData.roots,
			RootOfTree{meta_blob: tree, name: "/", multiplicity: 1})
		initials.Add(tree)
	}

	// 3. create full name tree for all meta_blobs (repositoryData.fullpath)
	//    dfs sorts the children topologically, so parents come befor the children
	for _, meta_blob := range dfs(repositoryData.children, initials, repositoryData) {
		for child := range repositoryData.children[meta_blob].Iter() {
			repositoryData.fullpath[child] = repositoryData.fullpath[meta_blob] + "/" +
				repositoryData.names[child]
		}
	}

	// rename repository root entries after fullpath has been set
	for _, sn := range repositoryData.snaps {
		ix := repositoryData.blob_to_index[*sn.Tree]
		repositoryData.fullpath[ix] = fmt.Sprintf("/./ (root of %s)", sn.ID().Str())
	}
	initials = nil
}

// build a topology structure for one snapshot
// the function relies on 'children' being initialized properly
func topology_structure(sn restic.Snapshot, repositoryData *RepositoryData) {

	// for each snapshot, there are always 2 fixed elements:
	// the tree root and the empty directory
	tree_root := repositoryData.blob_to_index[*sn.Tree]
	seen := mapset.NewSet(tree_root, EMPTY_NODE_ID_TRANSLATED)

	// use 'queue' as a FIFO queue to copy the topologicl tree
	to_be_processed := queue.New()
	to_be_processed.Enqueue(tree_root)
	for to_be_processed.Len() > 0 {
		for child := range repositoryData.children[to_be_processed.Dequeue().(IntID)].Iter() {
			if ! seen.Contains(child) {
				seen.Add(child)
				to_be_processed.Enqueue(child)
			}
		}
	}

	// at the end of the loop, 'seen' contains all directories
	// referenced in the snapshot
	id_ptr := Ptr2ID(*sn.ID(), repositoryData)
	repositoryData.meta_dir_map[id_ptr] = seen
	// reset
	seen = nil
	to_be_processed = nil
}

// this methods runs through all the steps to gather the pertinent repository data
func GatherAllRepoData(gopts GlobalOptions, ctx context.Context,
	repo restic.Repository, repositoryData *RepositoryData) error {
	// step 1: build a slice of all meta_blob IDs in the repo
	if err := ForAllMyTrees(gopts, ctx, repo, repositoryData); err != nil {
		Printf("ForAllMyTrees returned %v\n", err)
		return err
	}

	// step 2: prepare children and parents from idd_file records
	FindChildren(repositoryData)

	// step 3: build topology structure for each snapshot in repository
	for _, sn := range repositoryData.snaps {
		topology_structure(*sn, repositoryData)
	}
	return nil
}

// auxiliary function to deliver meta blobs from the index to ForAllMyTrees
// for parallel processing
func DeliverTreeBlobs(repositoryData *RepositoryData, fn func(id restic.ID) error) error {
	for blob_ID, data := range repositoryData.index_handle {
		if data.Type == restic.TreeBlob {
			fn(blob_ID)
		}
	}
	return nil
}

// home built parallel call to restic.LoadTree. All trees are accessed by the
// method 'DeliverTreeBlobs' which accesses 'repositoryData.index_handle'
// which has been built beforehand
func ForAllMyTrees(gopts GlobalOptions, ctx context.Context,
	repo restic.Repository, repositoryData *RepositoryData) error {

	var m sync.Mutex
	wg, _ := errgroup.WithContext(ctx)
	chan_tree_blob := make(chan restic.ID)

	repositoryData.rename_children = make(map[string]string) // from_name -> to_name
	var renameNames []RenameNames
	data, err := ioutil.ReadFile("/home/wplapper/restic/directory_renames.json")
	if err != nil {
			Printf("I/O error while reading directory_renames.json - %v\n", err)
			panic("Cannot read json rename configuration file!")
	}
	err = json.Unmarshal(data, &renameNames)
	if err != nil {
			Printf("json error %v\n", err)
			panic("Cannot Unmarshal json rename  file!")
	}

	for _, elem := range renameNames {
		repositoryData.rename_children[elem.From_name] = elem.To_name
	}
	renameNames = nil


	wg.Go(func() error {
		defer close(chan_tree_blob)

		// this callback function get fed the 'id'
		return DeliverTreeBlobs(repositoryData, func(id restic.ID) error {
			select {
			case <-ctx.Done():
				return nil
			case chan_tree_blob <- id:
			}
			return nil
		})
	})

	// a worker receives a snapshot ID from chan_tree_blob, loads the tree
	// and runs fn with id, the snapshot and the error
	var name string
	worker := func() error {
		for id := range chan_tree_blob {
			tree, err := restic.LoadTree(ctx, repo, id)
			if err != nil {
				Printf("LoadTree returned %v\n", err)
				return err
			}

			idd_file_list := make([]BlobFile2, len(tree.Nodes))
			// do the work on the tree just received
			for offset_in_node_list, node := range tree.Nodes {
				// setup these two place holders
				content := make([]IntID, 0)
				subt_ID := EMPTY_NODE_ID_TRANSLATED

				switch node.Type {
				case "file":
					for _, cont := range node.Content {
						// get the index for our restic.ID storage
						ix_data, ok := repositoryData.blob_to_index[cont]
						if !ok {
							Printf("Fatal: %v not in blob_to_index\n", cont)
							panic("ForAllMyTrees: error during content processing")
						}
						content = append(content, ix_data)

					}
					//name = node.Name
				case "dir":
					//name = node.Name
					if target, ok := repositoryData.rename_children[name]; ok {
						Printf("renaming %s -> %s for %s\n", name, target, (*node.Subtree).String()[:12])
						//name = target
					}
					subt_ID = repositoryData.blob_to_index[*node.Subtree]
				default:
				  //name = node.Name
				}
				blob_file := BlobFile2{name: node.Name,
					Type: node.Type, size: node.Size,
					DeviceID: node.DeviceID, inode: node.Inode,
					mtime: node.ModTime, content: content, subtree_ID: subt_ID}
				idd_file_list[offset_in_node_list] = blob_file
			}

			ix := repositoryData.blob_to_index[id]
			// insert directory_map: this is the critical region, so lock it
			m.Lock()
			repositoryData.directory_map[ix] = idd_file_list
			m.Unlock()
		}
		return nil
	}

	// start all these parallel workers
	max_parallel := int(repo.Connections()) + runtime.GOMAXPROCS(0)
	//Printf("max_parallel = %2d\n", max_parallel)
	for i := 0; i < max_parallel; i++ {
		wg.Go(worker)
	}
	return wg.Wait()
}

// PrintMemUsage outputs the current, total and OS memory being used.
// As well as the number of garbage collection cycles completed.
func PrintMemUsage() {

	var m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m2)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	Printf("Alloc = %4d MiB", bToMb(m2.Alloc))
	//Printf("\tTotalAlloc = %4d MiB", bToMb(m2.TotalAlloc))
	Printf("\tSys = %4d MiB", bToMb(m2.Sys))
	Printf("\tHeap = %4d MiB\n", bToMb(m2.HeapInuse))
	//Printf("\tNumGC = %4d\n", m2.NumGC)
}

func bToMb(b uint64) uint64 {
	// full megabytes
	return b / 1024 / 1024
}

func timeMessage(memory_use bool, format string, args ...interface{}) {
	Verbosef(format, args...)
	if memory_use {
		PrintMemUsage()
	}
}

func Ptr2ID(id restic.ID, repositoryData *RepositoryData) *restic.ID {
	return Ptr2ID3(id, repositoryData, "2ID")
}

// return the pointer to a given ID
func Ptr2ID3(id restic.ID, repositoryData *RepositoryData, where string) *restic.ID {
	ix, ok := repositoryData.blob_to_index[id]
	if ok {
		return &(repositoryData.index_to_blob[ix])
	} else {
		// allocate new slot
		Printf("Ptr2ID.allocate new %s %s\n", where, id.String()[:12])
		repositoryData.blob_to_index[id] = IntID(len(repositoryData.index_to_blob))
		repositoryData.index_to_blob = append(repositoryData.index_to_blob, id)
		// be aware: length just changed during last 'append'
		return &(repositoryData.index_to_blob[IntID(
			len(repositoryData.index_to_blob)-1)])
	}
}

// collect the usual stuff from a repository:
// snapshots, Index records, node records
func gather_base_data_repo(repo restic.Repository, gopts GlobalOptions,
	ctx context.Context, repositoryData *RepositoryData, timing bool) error {

	var err error
	start := time.Now()
	if timing {
		timeMessage(true, "%-30s %10.1f seconds\n", "repository is open",
			time.Now().Sub(start).Seconds())
	}

	// step 2: gather all snapshots
	repositoryData.snaps, repositoryData.snap_map, err = GatherAllSnapshots(gopts, ctx, repo)
	if err != nil {
		return err
	}
	if timing {
		timeMessage(true, "%-30s %10.1f seconds\n", "gather snapshots",
			time.Now().Sub(start).Seconds())
	}

	// step 3: manage Index Records
	if err = HandleIndexRecords(gopts, ctx, repo, repositoryData); err != nil {
		return err
	}
	if timing {
		timeMessage(true, "%-30s %10.1f seconds\n", "read index records",
			time.Now().Sub(start).Seconds())
	}

	// step 4: read all meta blobs and create the incore tables
	GatherAllRepoData(gopts, ctx, repo, repositoryData)
	if timing {
		timeMessage(true, "%-30s %10.1f seconds\n", "GatherAllRepoData",
			time.Now().Sub(start).Seconds())
	}
	return nil
}

// depth first search using 'children_map' as the map, 'initials' as starting
// points and 'repositoryData' is used for debugging
func dfs(children_map map[IntID]mapset.Set[IntID], initials mapset.Set[IntID],
repositoryData *RepositoryData) []IntID {

	// define a stack entry
	type StackEntry struct {
		parent IntID
		children_iter *mapset.Iterator[IntID]
	}

	// get rid of this entry!
	initials.Remove(EMPTY_NODE_ID_TRANSLATED)

	// set up temp storage an results
	visited := mapset.NewSet[IntID]()
	results := make([]IntID, 0)

	for root := range initials.Iter() {
		// prime the search
		if ! visited.Contains(root) {
			visited.Add(root)

			// create a completely new stack
			stak := stack.New()
			stak.Push(StackEntry{parent: root, children_iter: children_map[root].Iterator()})

			for true {
				// this is the only exit from the loop
				if stak.Len() == 0 { break }
LOOP:
				entry  := stak.Peek().(StackEntry)
				parent := entry.parent
				new_data := false
				for child := range entry.children_iter.C {
					if ! visited.Contains(child) {
						visited.Add(child)
						stak.Push(StackEntry{parent: child, children_iter: children_map[child].Iterator()})
						new_data = true
					}
				}
				if new_data { goto LOOP }

				stak.Pop()
				if stak.Len() > 0 {
					results = append(results, parent)
				}
			}
			results = append(results, root)
		}
	}

	// reverse 'results' so that the roots are at the relative beginning of the slice
	l_result := len(results)
	inverse_result := make([]IntID, 0, l_result)
	for ix := range results {
		inverse_result = append(inverse_result, results[l_result - 1 - ix])
	}

	// reset and return
	visited = nil
	results = nil
	return inverse_result
}

func make_blobs_per_packID(repositoryData *RepositoryData) map[IntID]mapset.Set[IntID] {
	blobs_per_packID := make(map[IntID]mapset.Set[IntID])
	for _, ih := range repositoryData.index_handle {
		if _, ok := blobs_per_packID[ih.pack_index]; !ok {
			blobs_per_packID[ih.pack_index] = mapset.NewSet[IntID]()
		}
		blobs_per_packID[ih.pack_index].Add(ih.blob_index)
	}
	return blobs_per_packID
}
