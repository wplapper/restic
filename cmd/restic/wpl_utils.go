package main

import (
	// system
	"fmt"
	"golang.org/x/sync/errgroup"
	"runtime"
	"sort"
	"sync"

	// restic library
	"github.com/wplapper/restic/library/restic"

	//deque
	"github.com/gammazero/deque"

	// sets
	"github.com/deckarep/golang-set/v2"
)

// system wide variables and containers
var (
	gOptions GlobalOptions
	db_aggregate DBAggregate
	dbOptions DBOptions
	newComers *Newcomers
	repositoryData *RepositoryData
	EMPTY_NODE_ID restic.ID
	EMPTY_NODE_ID_TRANSLATED restic.IntID
)

func init_repositoryData() *RepositoryData {
	var repositoryData RepositoryData
	repositoryData.snaps = []*restic.Snapshot{}
	repositoryData.directory_map = make(map[restic.IntID][]BlobFile2)
	repositoryData.fullpath = make(map[restic.IntID]string)
	repositoryData.names = make(map[restic.IntID]string)
	repositoryData.children = make(map[restic.IntID]restic.IntSet)

	repositoryData.meta_dir_map = make(map[*restic.ID]restic.IntSet)
	repositoryData.index_handle = make(map[restic.ID]Index_Handle)
	repositoryData.blob_to_index = make(map[restic.ID]restic.IntID)
	repositoryData.index_to_blob = []restic.ID{}
	return &repositoryData
}

func GatherAllSnapshots(gopts GlobalOptions, repo restic.Repository) ([]*restic.Snapshot, error) {
	// collect all snap records
	snaps := make([]*restic.Snapshot, 0, 10)
	repo.List(gopts.ctx, restic.SnapshotFile, func(id restic.ID, size int64) error {
		sn, err := restic.LoadSnapshot(gopts.ctx, repo, id)
		if err != nil {
			Printf("Skip snap record %s!\n", id)
			return err
		}
		snaps = append(snaps, sn)
		return nil
	})

	// now we can sort 'snaps' by sn.Time
	// sort is in-place!
	sort.SliceStable(snaps, func(i, j int) bool {
		return snaps[i].Time.Before(snaps[j].Time)
	})
	return snaps, nil
}

func HandleIndexRecords(gopts GlobalOptions, repo restic.Repository,
	repositoryData *RepositoryData) error {
	//Printf("HandleIndexRecords start\n")
	// load index files and their contents
	// 'LoadIndex' is in library/repository/repository.go, needs to happen first
	//start := time.Now()
	if err := repo.LoadIndex(gopts.ctx); err != nil {
		return err
	}
	// about 0.9 seconds
	//timeMessage("  %-30s %10.1f seconds\n", "LoadIndex",
	//	time.Now().Sub(start).Seconds())

	//start = time.Now()
	Convert_to_IntSet(gopts, repo, repositoryData)
	//timeMessage("  %-30s %10.1f seconds\n", "Conv2IntSet",
	//	time.Now().Sub(start).Seconds())
	return nil
}

// this function converts restic.ID to IntID, forth and back
// It also correlates blobs and pack IDs
func Convert_to_IntSet(gopts GlobalOptions, repo restic.Repository,
	repositoryData *RepositoryData) {

	// blob is a 'restic.PackedBlob' which contains
	// ID,  PackID, Type, Length and Offset
	// build 'blob_to_index' and 'blob_to_index' for all known restic.ID(s)
	// sources are the index files, packfiles and snapshot records
	pos := restic.IntID(0)
	for blob := range repo.Index().Each(gopts.ctx) {
		_, ok := repositoryData.blob_to_index[blob.ID]
		if !ok {
			repositoryData.blob_to_index[blob.ID] = pos
			repositoryData.index_to_blob = append(repositoryData.index_to_blob, blob.ID)
			pos++
		}

		// add packID from packflies to 'blob_to_index' and 'index_to_blob'
		_, ok = repositoryData.blob_to_index[blob.PackID]
		if !ok { // new PackID found
			repositoryData.blob_to_index[blob.PackID] = pos
			repositoryData.index_to_blob = append(repositoryData.index_to_blob, blob.PackID)
			pos++
		}

		// build index_handle
		repositoryData.index_handle[blob.ID] = Index_Handle{Type: blob.Type, size: int(blob.Length),
			pack_index: repositoryData.blob_to_index[blob.PackID],
			blob_index: repositoryData.blob_to_index[blob.ID]}
	}

	// add the *restic.IDs from the snapshots list to this list
	for _, sn := range repositoryData.snaps {
		snap := *sn.ID()
		if _, ok := repositoryData.blob_to_index[snap]; !ok {
			repositoryData.blob_to_index[snap] = pos
			repositoryData.index_to_blob = append(repositoryData.index_to_blob, snap)
			pos++
		}
	}

	// we need to set EMPTY_NODE_ID_TRANSLATED
	ok := false
	EMPTY_NODE_ID_TRANSLATED, ok = repositoryData.blob_to_index[EMPTY_NODE_ID]
	if !ok {
		panic("No EMPTY_NODE_ID in repositoryData.blob_to_index!")
	}
}

// FindChildren steps through the directory_map and finds subdirectories.
// The children get attached their parent
func FindChildren(repositoryData *RepositoryData) {
	//Printf("FindChildren start\n")
	// 1. collect the children
	for parent, idd_file_list := range repositoryData.directory_map {
		for _, node := range idd_file_list {
			if node.Type != "dir" {
				continue
			}

			if node.subtree_ID == EMPTY_NODE_ID_TRANSLATED {
				continue
			}
			// initialize
			if old_name, ok := repositoryData.names[node.subtree_ID]; ok {
				if old_name != node.name {
					//Printf("Clobbering %6d with '%s', before '%s'\n", node.subtree_ID,
					//node.name, old_name)
				}
			} else {
				repositoryData.names[node.subtree_ID] = node.name
			}

			// new subtree
			if _, ok := repositoryData.children[parent]; !ok {
				repositoryData.children[parent] = restic.NewIntSet()
			}
			// the children data will be used in topology step
			repositoryData.children[parent].Insert(node.subtree_ID)
		}
	}

	// 2. create tree roots for fullpath
	for _, sn := range repositoryData.snaps {
		repositoryData.fullpath[repositoryData.blob_to_index[*sn.Tree]] = "/."
	}

	// 3. create name tree for all meta_blobs
	//Printf("*** create name tree ***\n")
	fail := true
	//count := 0
	for fail {
		//count++
		//Printf("*Iteration %2d\n", count)
		fail = false
		for parent, children := range repositoryData.children {
			// if parent is not set yet, ignore and try again later
			if _, ok := repositoryData.fullpath[parent]; !ok {
				fail = true
				continue
			}

			for child := range children {
				if child == EMPTY_NODE_ID_TRANSLATED {
					continue
				}
				// if already done, don't repeat
				if _, ok := repositoryData.fullpath[child]; ok {
					continue
				}

				// construct name of child directory
				repositoryData.fullpath[child] = repositoryData.fullpath[parent] +
						"/" + repositoryData.names[child]
			}
		}
	}
	//Print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
}

// build a topology structure for one snapshot
// the function relies on 'children' being initialized properly
func topology_structure(sn restic.Snapshot, repositoryData *RepositoryData) {

	// for each snapshot, there are always 2 fixed elements:
	// the tree root and the empty directory
	tree_root := repositoryData.blob_to_index[*sn.Tree]
	seen := restic.NewIntSet(tree_root, EMPTY_NODE_ID_TRANSLATED)

	// 'to_be_processed' is the list of new meta_blobs, not yet handled
	// use deque as a FIFO queue
	to_be_processed := deque.New[restic.IntID](100, 100)
	// prime the process loop
	to_be_processed.PushBack(tree_root)

	for to_be_processed.Len() > 0 {
		//take off the oldest one, the one at the front of the queuen and
		// process te children of it.
		for child := range repositoryData.children[to_be_processed.PopFront()] {
			if seen.Has(child) {
				continue
			} else {
				seen.Insert(child)
				to_be_processed.PushBack(child)
			}
		}
	}

	// at the end of the loop, 'seen' contains all directories
	// referenced in the snapshot
	id_ptr := Ptr2ID(*sn.ID(), repositoryData)
	repositoryData.meta_dir_map[id_ptr] = seen
}

// this methods runs through all the steps to gather the pertinent repository data
func GatherAllRepoData(gopts GlobalOptions, repo restic.Repository,
	repositoryData *RepositoryData) error {
	// step 1: gather snapshots
	// build a slice of all meta_blob IDs in the repo
	if err := ForAllMyTrees(gopts, repo, repositoryData); err != nil {
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
// for paralel processing
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
func ForAllMyTrees(gopts GlobalOptions, repo restic.Repository,
repositoryData *RepositoryData) error {

	var m sync.Mutex

	wg, ctx := errgroup.WithContext(gopts.ctx)
	chan_tree_blob := make(chan restic.ID)
	//Printf("%-26s ForAllMyTrees start\n", time.Now().Format("2006-01-02 15:04:05.999999"))
	wg.Go(func() error {
		defer close(chan_tree_blob)

		// this callback function needs to return an 'id'
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
	worker := func() error {
		for id := range chan_tree_blob {
			tree, err := restic.LoadTree(gopts.ctx, repo, id)
			if err != nil {
				Printf("LoadTree returned %v\n", err)
				return err
			}

			idd_file_list := make([]BlobFile2, len(tree.Nodes))
			// do the work on the tree just received
			offset_in_node_list := 0
			for _, node := range tree.Nodes {
				// setup these two place holders
				content := make([]restic.IntID, 0)
				subt_ID := EMPTY_NODE_ID_TRANSLATED

				switch node.Type {
				case "file":
					for _, cont := range node.Content {
						// get the index for our restic.ID storage
						ix_data, ok := repositoryData.blob_to_index[cont]
						if !ok {
							Printf("Fatal: %v not in blob_to_index\n", cont)
							panic("error during content processing")
						}
						content = append(content, ix_data)
					}
				case "dir":
					// get the index for our restic.ID storage
					subt_ID = repositoryData.blob_to_index[*node.Subtree]
					/*if !ok {
						Printf("Fatal: %v not in blob_to_index\n", *node.Subtree)
						panic("error during sub directory processing")
					}*/
				}
				idd_file_list[offset_in_node_list] = BlobFile2{name: node.Name,
					Type: node.Type, size: node.Size, inode: node.Inode,
					mtime: node.ModTime, content: content, subtree_ID: subt_ID}
				offset_in_node_list++
			}

			// get the index for our restic.ID storage
			position, ok := repositoryData.blob_to_index[id]
			if !ok {
				Printf("request for missing ID %v\n", id)
				panic("error in GetAllNodes - storing idd_file_list")
			}

			// insert directory_map, this is the critical region, so lock it
			m.Lock()
			repositoryData.directory_map[position] = idd_file_list
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

	// and wait for them all to finish
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
	fmt.Printf("Alloc = %4d MiB", bToMb(m2.Alloc))
	fmt.Printf("\tTotalAlloc = %4d MiB", bToMb(m2.TotalAlloc))
	fmt.Printf("\tSys = %4d MiB", bToMb(m2.Sys))
	fmt.Printf("\tHeap = %4d MiB", bToMb(m2.HeapInuse))
	fmt.Printf("\tNumGC = %4d\n", m2.NumGC)
}

func bToMb(b uint64) uint64 {
	// full megabytes
	return b / 1024 / 1024
}

func timeMessage(format string, args ...interface{}) {
	if gOptions.verbosity > 0 {
		Printf(format, args...)
	}
}

func Ptr2ID(id restic.ID, repositoryData *RepositoryData) *restic.ID {
	return Ptr2ID3(id, repositoryData, "??")
}

// return the pointer to a given ID
func Ptr2ID3(id restic.ID, repositoryData *RepositoryData, where string) *restic.ID {
	ix, ok := repositoryData.blob_to_index[id]
	if ok {
		return &(repositoryData.index_to_blob[ix])
	} else {
		// allocate new slot
		//Printf("Ptr2ID.allocate new %s %s\n", where, id.String()[:12])
		repositoryData.blob_to_index[id] = restic.IntID(len(repositoryData.index_to_blob))
		repositoryData.index_to_blob = append(repositoryData.index_to_blob, id)
		// be aware: length just changed during last 'append'
		return &(repositoryData.index_to_blob[restic.IntID(
			len(repositoryData.index_to_blob) - 1)])
	}
}

// stop at terminal
func ConfirmStdin() {
	fmt.Print("Confirm ")
	var input string
	fmt.Scanln(&input)
	return
}

// compare the keys of the equivalent memory and database table,
// return the difference between database and memory
func OldDBKeys[K comparable, V1 any, V2 any](db map[K]V1, mem map[K]V2) mapset.Set[K] {
	// define sets for the keys
	// these two sets are very short lived
	set_db_keys  := mapset.NewSet[K]()
	set_mem_keys := mapset.NewSet[K]()
	for key := range db {
		set_db_keys.Add(key)
	}
	for key := range mem {
		set_mem_keys.Add(key)
	}
	diff := set_db_keys.Difference(set_mem_keys)
	set_db_keys  = nil
	set_mem_keys = nil
	return diff
}
