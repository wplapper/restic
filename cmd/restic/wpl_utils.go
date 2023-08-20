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
	//"encoding/json"
	//"io/ioutil"

	// restic library
	"github.com/wplapper/restic/library/restic"
	"github.com/wplapper/restic/library/repository"

	// sets
	"github.com/deckarep/golang-set/v2"

	// stack and queue
	"github.com/golang-collections/collections/stack"
	"github.com/golang-collections/collections/queue"
)

// system wide variables and containers
var (
	gOptions                 GlobalOptions
	repositoryData           *RepositoryData
	EMPTY_NODE_ID            restic.ID
	EMPTY_NODE_ID_TRANSLATED IntID
)

func init_repositoryData(repositoryData *RepositoryData) {
	repositoryData.Snaps = []*restic.Snapshot{}
	repositoryData.SnapMap = make(map[string]*restic.Snapshot)
	repositoryData.DirectoryMap = make(map[IntID][]BlobFile2)
	repositoryData.FullPath = make(map[IntID]string)
	repositoryData.MetaDirMap = make(map[*restic.ID]mapset.Set[IntID])
	repositoryData.IndexHandle = make(map[restic.ID]Index_Handle)

	repositoryData.BlobToIndex = make(map[restic.ID]IntID)
	repositoryData.IndexToBlob = []restic.ID{}
}

// GatherAllSnapshots retrieves all snapshots from the repository
// return: slice of all *sn, sorted by ascending snapshot time (*sn.Time)
func GatherAllSnapshots(gopts GlobalOptions, ctx context.Context,
	repo *repository.Repository) ([]*restic.Snapshot, map[string]*restic.Snapshot, error) {
	// collect all snap records
	snaps := []*restic.Snapshot{}
	repo.List(ctx, restic.SnapshotFile, func(id restic.ID, size int64) error {
		sn, err := restic.LoadSnapshot(ctx, repo, id)
		if err != nil {
			Printf("GatherAllSnapshots.skip loading snap record %s! - reason: %v\n", id, err)
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

func HandleIndexRecords(gopts GlobalOptions, ctx context.Context, repo *repository.Repository,
	repositoryData *RepositoryData) error {
	// load index files and their contents
	// 'LoadIndex' is in library/repository/repository.go, needs to happen first
	if err := repo.LoadIndex(ctx); err != nil {
		Printf("repo.LoadIndex - failed. Error is %v\n", err)
		return err
	}

	ConverToIntSet(gopts, ctx, repo, repositoryData)
	return nil
}

// this function converts restic.ID to IntID, forth and back
// It also correlates blobs and pack IDs
// build 'repositoryData.BlobToIndex' and 'repositoryData.IndexToBlob'
// from 'repo.Index().Each(gopts.ctx)', containing 'blob.ID' and 'blob.PackID'
func ConverToIntSet(gopts GlobalOptions, ctx context.Context, repo *repository.Repository,
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
		if _, ok := repositoryData.BlobToIndex[blob.ID]; ! ok {
			repositoryData.BlobToIndex[blob.ID] = pos
			repositoryData.IndexToBlob = append(repositoryData.IndexToBlob, blob.ID)
			pos++
		}

		// add packID from packfiles to 'blob_to_index' and 'index_to_blob',
		if _, ok := repositoryData.BlobToIndex[blob.PackID]; ! ok {
			repositoryData.BlobToIndex[blob.PackID] = pos
			repositoryData.IndexToBlob = append(repositoryData.IndexToBlob, blob.PackID)
			pos++
		}

		// build our index_handle from 'blob' record
		length := int(blob.Length)
		repositoryData.IndexHandle[blob.ID] = Index_Handle{Type: blob.Type, size: length,
			pack_index:         repositoryData.BlobToIndex[blob.PackID],
			blob_index:         repositoryData.BlobToIndex[blob.ID],
			UncompressedLength: int(blob.UncompressedLength),
		}
	})

	// add the *restic.IDs from the snapshots list to these list / maps
	for _, sn := range repositoryData.Snaps {
		snap := *sn.ID()
		if _, ok := repositoryData.BlobToIndex[snap]; ! ok {
			repositoryData.BlobToIndex[snap] = pos
			repositoryData.IndexToBlob = append(repositoryData.IndexToBlob, snap)
			pos++
		}
	}

	// we need to set EMPTY_NODE_ID_TRANSLATED
	ok := false
	EMPTY_NODE_ID_TRANSLATED, ok = repositoryData.BlobToIndex[EMPTY_NODE_ID]
	if !ok {
		repositoryData.BlobToIndex[EMPTY_NODE_ID] = pos
		repositoryData.IndexToBlob = append(repositoryData.IndexToBlob, EMPTY_NODE_ID)
		EMPTY_NODE_ID_TRANSLATED = pos
	}
}

// FindChildren steps through the directory_map and finds subdirectories.
// The children get attached their parent
func FindChildren(repositoryData *RepositoryData) (children map[IntID]mapset.Set[IntID]) {
	children = CreateAllChildren(repositoryData)
	names  := make(map[IntID]string)
	for _, idd_file_list := range repositoryData.DirectoryMap {
		for _, node := range idd_file_list {
			if node.subtree_ID == EMPTY_NODE_ID_TRANSLATED { continue }
			names[node.subtree_ID] = node.name
		}
	}

	// 2. create tree root names for fullpath
	repositoryData.roots = make([]RootOfTree, 0, len(repositoryData.Snaps))
	initials := mapset.NewSet[IntID]()
	for _, sn := range repositoryData.Snaps {
		tree := repositoryData.BlobToIndex[*sn.Tree]
		repositoryData.FullPath[tree] = "/."
		repositoryData.roots = append(repositoryData.roots,
			RootOfTree{meta_blob: tree, name: "/", multiplicity: 1})
		initials.Add(tree)
	}

	// 3. create full name tree for all meta_blobs (repositoryData.FullPath)
	// dfs sorts the children topologically, so parents appear before their children
	for _, meta_blob := range dfs(children, initials) {
		for child := range children[meta_blob].Iter() {
			repositoryData.FullPath[child] = repositoryData.FullPath[meta_blob] + "/" +
				names[child]
		}
	}

	// rename repository root entries after fullpath has been set
	for _, sn := range repositoryData.Snaps {
		ix := repositoryData.BlobToIndex[*sn.Tree]
		repositoryData.FullPath[ix] = fmt.Sprintf("/./ (root of %s)", sn.ID().Str())
	}
	initials = nil
	return children
}

// build a topology structure for one snapshot
// the function relies on 'children' being initialized properly
func TopologyStructure(sn restic.Snapshot, repositoryData *RepositoryData,
children map[IntID]mapset.Set[IntID]) {

	// for each snapshot, there are always 2 fixed elements:
	// the tree root and the empty directory
	tree_root := repositoryData.BlobToIndex[*sn.Tree]
	seen := mapset.NewSet(tree_root, EMPTY_NODE_ID_TRANSLATED)

	// use 'queue' as a FIFO queue to copy the topologicl tree
	to_be_processed := queue.New()
	to_be_processed.Enqueue(tree_root)
	for to_be_processed.Len() > 0 {
		for child := range children[to_be_processed.Dequeue().(IntID)].Iter() {
			if ! seen.Contains(child) {
				seen.Add(child)
				to_be_processed.Enqueue(child)
			}
		}
	}

	// at the end of the loop, 'seen' contains all directories
	// referenced in the snapshot
	id_ptr := Ptr2ID(*sn.ID(), repositoryData)
	repositoryData.MetaDirMap[id_ptr] = seen
	// reset for GC
	seen = nil
	to_be_processed = nil
}

// this methods runs through all the steps to gather the pertinent repository data
func GatherAllRepoData(gopts GlobalOptions, ctx context.Context,
	repo *repository.Repository, repositoryData *RepositoryData) error {
	// step 1: build a slice of all meta_blob IDs in the repo
	if err := ForAllMyTrees(gopts, ctx, repo, repositoryData); err != nil {
		Printf("ForAllMyTrees returned %v\n", err)
		return err
	}

	// step 2: prepare children and parents from idd_file records
	children := FindChildren(repositoryData)

	// step 3: build topology structure for each snapshot in repository
	for _, sn := range repositoryData.Snaps {
		TopologyStructure(*sn, repositoryData, children)
	}
	return nil
}

// auxiliary function to deliver meta blobs from the index to ForAllMyTrees
// for parallel processing
func DeliverTreeBlobs(ctx context.Context, repo *repository.Repository,
repositoryData *RepositoryData, fn func(id restic.ID) error) error {

	for blob_ID, data := range repositoryData.IndexHandle {
		if data.Type == restic.TreeBlob {
			fn(blob_ID)
		}
	}
	return nil
}

// home built parallel call to restic.LoadTree. All trees are accessed by the
// method 'DeliverTreeBlobs' which accesses 'repositoryData.IndexHandle'
// which has been built beforehand
func ForAllMyTrees(gopts GlobalOptions, ctx context.Context,
	repo *repository.Repository, repositoryData *RepositoryData) error {

	var m sync.Mutex
	//orgCtx := ctx
	wg, ctx := errgroup.WithContext(ctx)
	chan_tree_blob := make(chan restic.ID)

	/*
	repositoryData.rename_children = map[string]string{} // from_name -> to_name
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
	*/

	wg.Go(func() error {
		defer close(chan_tree_blob)

		// this callback function get fed the 'id'
		return DeliverTreeBlobs(ctx, repo, repositoryData, func(id restic.ID) error {
			select {
			case <-ctx.Done():
				return nil
			case chan_tree_blob <- id:
				return nil
			}
			return nil
		})
	})

	// a worker receives a metablob ID from chan_tree_blob, loads the tree
	// and runs fn with id, the snapshot and the error
	//var name string
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
				content := []IntID{}
				subt_ID := EMPTY_NODE_ID_TRANSLATED

				if node.Type == "file" {
					for _, cont := range node.Content {
						// get the index for our restic.ID storage
						ix_data, ok := repositoryData.BlobToIndex[cont]
						if !ok {
							Printf("Fatal: %v not in blob_to_index\n", cont)
							panic("ForAllMyTrees: error during content processing")
						}
						content = append(content, ix_data)
					}
				}

				blob_file := BlobFile2{name: node.Name,
					Type: node.Type, size: node.Size,
					DeviceID: node.DeviceID, inode: node.Inode,
					mtime: node.ModTime, content: content, subtree_ID: subt_ID}
				idd_file_list[offset_in_node_list] = blob_file
			}

			ix := repositoryData.BlobToIndex[id]
			// insert directory_map: this is the critical region, so lock it
			m.Lock()
			repositoryData.DirectoryMap[ix] = idd_file_list
			m.Unlock()
		}
		return nil
	}

	// start all these parallel workers
	max_parallel := int(repo.Connections()) + runtime.GOMAXPROCS(0)
	for i := 0; i < max_parallel; i++ {
		wg.Go(worker)
	}
	res := wg.Wait()
	return res
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
	Printf(format, args...)
	if memory_use {
		PrintMemUsage()
	}
}

func Ptr2ID(id restic.ID, repositoryData *RepositoryData) *restic.ID {
	return Ptr2ID3(id, repositoryData, "2ID")
}

// return the pointer to a given ID
func Ptr2ID3(id restic.ID, repositoryData *RepositoryData, where string) *restic.ID {
	ix, ok := repositoryData.BlobToIndex[id]
	if ok {
		return &(repositoryData.IndexToBlob[ix])
	} else {
		// allocate new slot
		Printf("Ptr2ID.allocate new %s %s\n", where, id.String()[:12])
		repositoryData.BlobToIndex[id] = IntID(len(repositoryData.IndexToBlob))
		repositoryData.IndexToBlob = append(repositoryData.IndexToBlob, id)
		// be aware: length just changed during last 'append'
		return &(repositoryData.IndexToBlob[IntID(
			len(repositoryData.IndexToBlob)-1)])
	}
}

// collect the usual stuff from a repository:
// snapshots, Index records, node records
func gather_base_data_repo(repo *repository.Repository, gopts GlobalOptions,
	ctx context.Context, repositoryData *RepositoryData, timing bool) error {

	var err error
	start := time.Now()
	if timing {
		timeMessage(true, "%-30s %10.1f seconds\n", "repository is open",
			time.Now().Sub(start).Seconds())
	}

	// step 2: gather all snapshots
	repositoryData.Snaps, repositoryData.SnapMap, err = GatherAllSnapshots(gopts, ctx, repo)
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

// depth first search using 'childrenMap' as the map, 'initials' as starting
// points and 'repositoryData' is used for debugging
func dfs(childrenMap map[IntID]mapset.Set[IntID], initials mapset.Set[IntID]) (inverse_result []IntID) {

	// define a stack entry
	type StackEntry struct {
		parent IntID
		ChildrenIter *mapset.Iterator[IntID]
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
			stak.Push(StackEntry{parent: root, ChildrenIter: childrenMap[root].Iterator()})

			for true {
				// this is the only exit from the loop
				if stak.Len() == 0 { break }
LOOP:
				entry  := stak.Peek().(StackEntry)
				parent := entry.parent
				new_data := false
				for child := range entry.ChildrenIter.C {
					if ! visited.Contains(child) {
						visited.Add(child)
						stak.Push(StackEntry{parent: child, ChildrenIter: childrenMap[child].Iterator()})
						new_data = true
					}
				}
				if new_data { goto LOOP }

				stak.Pop()
				if stak.Len() > 0 { results = append(results, parent) }
			} // end LOOP
			results = append(results, root)
		}
	}

	// reverse 'results' so that the roots are at the relative beginning of the slice
	l_result := len(results) - 1
	inverse_result = make([]IntID, 0, l_result + 1)
	for ix := range results {
		inverse_result = append(inverse_result, results[l_result - ix])
	}

	// reset for GC and return
	visited = nil
	results = nil
	return inverse_result
}
