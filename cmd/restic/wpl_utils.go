package main

import (
	// system
	"sort"
	"runtime"
	"fmt"
	"time"
	_ "math"
	"golang.org/x/sync/errgroup"
	"sync"

	// restic library
	"github.com/wplapper/restic/library/restic"
	"github.com/wplapper/restic/library/repository"
	//"github.com/wplapper/restic/library/debug"

	//deque
	"github.com/gammazero/deque"
)

type SortByPack struct {
	pack_ID restic.ID
	offset uint
	blob_ID restic.ID
}

// static variable
var gOptions GlobalOptions
//var sort_by_pack []SortByPack
//var old_pack_ID restic.ID

type BlobFile2 struct {
	// name, size, type_, mtime, content and subtree ID
	size 				uint64
	inode      	uint64
	content 		restic.IntSet
	subtree_ID 	restic.IntID
	name 				string
	Type 				string
	mtime 			time.Time
}

type Index_Handle struct {
		blob_index restic.IntID
		pack_index restic.IntID
		size uint
		Type restic.BlobType
}

type RepositoryData struct {
	// all snapshots
	snaps         []*restic.Snapshot
	// map contents of tree blob record via JSON to memory
	directory_map map[restic.IntID][]BlobFile2
	// map directory blob to a path name
	fullpath      map[restic.IntID]string
	// all directory names, basename ony
	names         map[restic.IntID]string
	// all directory children of a directory
	children      map[restic.IntID]restic.IntSet

	// need to be restic.ID
	// all tree blobs of a snapshot
	meta_dir_map  map[restic.ID]restic.IntSet
	// all tree and data blobs from the index
	index_handle	map[restic.ID]Index_Handle
	// map blob to an IntID number
	blob_to_index map[restic.ID]restic.IntID
	// address this slice via an IntID index to get back to the restic.ID
	index_to_blob		[]restic.ID
}

var EMPTY_NODE_ID restic.ID
var EMPTY_NODE_ID_TRANSLATED restic.IntID

func init_repositoryData() *RepositoryData {
	var repositoryData RepositoryData
	repositoryData.snaps				 = []*restic.Snapshot{}
	repositoryData.directory_map = make(map[restic.IntID][]BlobFile2)
	repositoryData.fullpath      = make(map[restic.IntID]string)
	repositoryData.names         = make(map[restic.IntID]string)
	repositoryData.children			 = make(map[restic.IntID]restic.IntSet)

	repositoryData.meta_dir_map  = make(map[restic.ID]restic.IntSet)
	repositoryData.index_handle	 = make(map[restic.ID]Index_Handle)
	repositoryData.blob_to_index = make(map[restic.ID]restic.IntID)
	repositoryData.index_to_blob = []restic.ID{}
	return &repositoryData
 }

func GatherAllSnapshots(gopts GlobalOptions, repo restic.Repository)  ([]*restic.Snapshot, error) {
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

	// now we can sort 'snaps': sort by sn.Time
	// sort is in-place!
	sort.SliceStable(snaps, func (i, j int) bool {
		return snaps[i].Time.Before(snaps[j].Time)
	})
	return snaps, nil
}

func HandleIndexRecords(gopts GlobalOptions, repo restic.Repository,
repositoryData *RepositoryData) error {
	// load index files and their contents
	// 'LoadIndex' is in library/repository/repository.go, needs to happen first
	if err := repo.LoadIndex(gopts.ctx); err != nil {
		return err
	}

	// 'ForAllIndexes' is in library/repository/index_parallel.go
	// ForAllIndexes loads a Index information and saves it efficiently
	// can be accessed by repo.Index().Each(), is in library/repository/index.go
	repository.ForAllIndexes(gopts.ctx, repo, func(id restic.ID,
	idx *repository.Index, oldFormat bool, err error) error {
		if err != nil {
			Printf("Ignoring error %v for ID %v\n", err, id)
			return err
		}
		return nil
	})

	Convert_to_IntSet(gopts, repo, repositoryData)
	return nil
}

// this function converts restic.ID to IntID, forth and back
// It also correlates bobs and pack IDs
func Convert_to_IntSet(gopts GlobalOptions, repo restic.Repository,
repositoryData *RepositoryData) {
	pos := restic.IntID(0)

	//start := time.Now()
	// blob is a 'restic.PackedBlob' which contains
	// ID,  PackID, Type, Length and Offset
	for blob := range repo.Index().Each(gopts.ctx) {
		repositoryData.blob_to_index[blob.ID] = pos
		repositoryData.index_to_blob = append(repositoryData.index_to_blob, blob.ID)
		pos++

		// add packID to 'blob_to_index' and 'index_to_blob'
		_, ok := repositoryData.blob_to_index[blob.PackID]
		if !ok { // new PackID found
			repositoryData.blob_to_index[blob.PackID] = pos
			repositoryData.index_to_blob = append(repositoryData.index_to_blob, blob.PackID)
			pos++
		}

		// build index_handle
		repositoryData.index_handle[blob.ID] = Index_Handle{Type: blob.Type, size: blob.Length,
			pack_index: repositoryData.blob_to_index[blob.PackID],
			blob_index: repositoryData.blob_to_index[blob.ID]}
	}
	//timeMessage("  %-30s %10.1f seconds\n", "building repositoryData.index_handle, blob_to_index, index_to_blob",
	//	time.Now().Sub(start).Seconds())
}

// FindChildren steps through the directory_map and finds subdirectories
// these get attached their (current) parent
func FindChildren (repositoryData *RepositoryData) {
	for parent, idd_file_list := range repositoryData.directory_map {
		for _,node := range idd_file_list {
			if node.Type == "dir" {
				// initialize
				repositoryData.names[node.subtree_ID] = node.name
				if len(repositoryData.children[parent]) == 0 {
					repositoryData.children[parent] = restic.NewIntSet()
				}
				// the children data will be used in topology step
				repositoryData.children[parent].Insert(node.subtree_ID)
			}
		}
	}
}

// build a topology structure for one snapshot
// the function relies on 'children' being initialized properly
func topology_structure(sn restic.Snapshot, repositoryData *RepositoryData) {
	// for each snapshot, there are always 2 fixed elements:
	// the tree root and the empty directory

	tree_root := repositoryData.blob_to_index[*sn.Tree]
	seen := restic.NewIntSet(tree_root, EMPTY_NODE_ID_TRANSLATED)
	repositoryData.fullpath[tree_root] = "/"

	// to_be_processed is the list of new meta_blobs, not yet handled
	// use deque as a FIFO queue
	to_be_processed := deque.New[restic.IntID](100, 100)
	// prime the process loop
	to_be_processed.PushBack(tree_root)

	for to_be_processed.Len() > 0 {
		//take off the oldest one
		meta_blob := to_be_processed.PopFront()
		for child_dir := range repositoryData.children[meta_blob].Sub(seen) {
			seen.Insert(child_dir)
			to_be_processed.PushBack(child_dir)

			// manage fullpath
			_, ok := repositoryData.fullpath[child_dir]
			if ok {
				continue
			}

			// construct name of child directory
			if repositoryData.fullpath[meta_blob] != "/" {
				repositoryData.fullpath[child_dir] = (repositoryData.fullpath[meta_blob] +
					"/" + repositoryData.names[child_dir])
			} else {
				repositoryData.fullpath[child_dir] = "/" + repositoryData.names[child_dir]
			}
		}
	}
	// remove reference to the empty directory node
	seen.Delete(EMPTY_NODE_ID_TRANSLATED)
	// at the end of the loop, 'seen' contains all directories
	// referenced in the snapshot
	repositoryData.meta_dir_map[*sn.ID()] = seen
}

// this methods runs through all the steps to gather the pertinent repository data
func GatherAllRepoData(gopts GlobalOptions, repo restic.Repository,
repositoryData *RepositoryData) error {
	// step 1: gather snapshots
	start := time.Now()
	snaps, err := GatherAllSnapshots(gopts, repo);
	if err != nil {
			return err
	}
	repositoryData.snaps = snaps
	//timeMessage("  %-30s %10.1f seconds\n", "gather snapshots", time.Now().Sub(start).Seconds())

	start = time.Now()
	// build a sice of all meta_blob ID in the repo
	meta_blobs := make([]*restic.ID, 0)
	for blob_ID, data := range repositoryData.index_handle {
			if data.Type == restic.TreeBlob {
				ix := repositoryData.blob_to_index[blob_ID]
				meta_blobs = append(meta_blobs, &repositoryData.index_to_blob[ix])
			}
	}
	//timeMessage("%-30s %10.1f seconds\n", "Index().Each()", time.Now().Sub(start).Seconds())

	start = time.Now()
	// ForAllMyTrees issues a lot of requests in parallel for all 'meta_blobs'
	err = ForAllMyTrees(gopts, repo, repositoryData, meta_blobs)
	if err != nil {
		Printf("ForAllMyTrees returned %v\n", err)
		return err
	}
	timeMessage("  %-28s %10.1f seconds\n", "ForAllMyTrees", time.Now().Sub(start).Seconds())

	// step 3: prepare children and parents from idd_file records
	//start = time.Now()
	FindChildren(repositoryData)
	//timeMessage("%-30s %10.1f seconds\n", "find children", time.Now().Sub(start).Seconds())

	// step 4: build topology for each snapshot in repository
	//start = time.Now()
	for _, sn := range repositoryData.snaps {
		topology_structure(*sn, repositoryData)
	}
	//timeMessage("%-30s %10.1f seconds\n", "build topology", time.Now().Sub(start).Seconds())
	return nil
}

// auxiliary function to deliver meta blobs from the index to ForAllMyTrees
// for paralel processing
func DeliverTreeBlobs(repositoryData *RepositoryData, meta_blobs []*restic.ID, fn func(id restic.ID) error) error {
	for _, blob_ID := range meta_blobs {
		fn(*blob_ID)
	}
	return nil
}

// home built parallel call to restic.LoadTree. All trees are accessed by the
// method 'DeliverTreeBlobs' which accesses 'meta_blobs'
// which has been built beforehand
func ForAllMyTrees(gopts GlobalOptions, repo restic.Repository,
repositoryData *RepositoryData, meta_blobs []*restic.ID) error {

	var m sync.Mutex
	// track spawned goroutines using wg, create a new context
	wg, ctx := errgroup.WithContext(gopts.ctx)
	ch := make(chan restic.ID)
	wg.Go(func() error {
		defer close(ch)

		// this callback function needs to return an 'id'
		return DeliverTreeBlobs(repositoryData, meta_blobs, func(id restic.ID) error {
			select {
			case <-ctx.Done():
				return nil
			case ch <- id:
			}
			return nil
		})
	})

	// a worker receives an snapshot ID from ch, loads the snapshot
	// and runs fn with id, the snapshot and the error
	worker := func() error {
		for id := range ch {
			tree, err := restic.LoadTree(gopts.ctx, repo, id)
			if err != nil {
				Printf("LoadTree returned %v\n", err)
				return err
			}
			idd_file_list := make([]BlobFile2, len(tree.Nodes))

			// do the work on the tree ust received
			for offset_in_node_list, node := range tree.Nodes {
				// setup these two place holders
				content    := restic.NewIntSet()
				subtree_ID := EMPTY_NODE_ID_TRANSLATED

				ok := false
				switch node.Type {
				case "file":
					for _, cont := range node.Content {
						// get the index for our restic.ID storage
						ix_data, ok := repositoryData.blob_to_index[cont]
						if !ok {
							Printf("Fatal: %v not in blob_to_index\n", cont)
							panic("error during content processing")
						}
						content.Insert(ix_data)
					}
				case "dir":
					// get the index for our restic.ID storage
					subtree_ID, ok = repositoryData.blob_to_index[*node.Subtree]
					if !ok {
						Printf("Fatal: %v not in blob_to_index\n", *node.Subtree)
						panic("error during sub directory processing")
					}
					// if *node.Subtree is nil: this is harmless since the entry
					// is replaced with EMPTY_NODE_ID_TRANSLATED
				}
				idd_file_list[offset_in_node_list] = BlobFile2{name: node.Name,
						Type: node.Type, size: node.Size, inode: node.Inode,
						mtime: node.ModTime, content: content, subtree_ID: subtree_ID}
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
	for i := 0; i < int(repo.Connections()) + runtime.GOMAXPROCS(0); i++ {
		wg.Go(worker)
	}
	// and wait for them all to finish
	return wg.Wait()
}

// PrintMemUsage outputs the current, total and OS memory being used.
// As well as the number of garbage collection cycles completed.
func PrintMemUsage() {

	/*
	count := 0
	var (
			m = runtime.MemStats{}
			prevInUse uint64
			prevNumGC uint32
	)

  for {

		runtime.ReadMemStats(&m)

		// Considering heap stable if recent cycle collected less than 10KB.
		if prevNumGC != 0 && m.NumGC > prevNumGC && math.Abs(float64(m.HeapInuse-prevInUse)) < 10*1024 {
				break
		}

		prevInUse = m.HeapInuse
		prevNumGC = m.NumGC

		// Sleeping to allow GC to run a few times and collect all temporary data.
		time.Sleep(50 * time.Millisecond)
		runtime.GC()
		count++
		if count > 2 {
			break
		}
	}
	*/

	var m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m2)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc        = %4d MiB", bToMb(m2.Alloc))
	fmt.Printf("\tTotalAlloc = %4d MiB", bToMb(m2.TotalAlloc))
	fmt.Printf("\tSys        = %4d MiB", bToMb(m2.Sys))
	fmt.Printf("\tHeap       = %4d MiB", bToMb(m2.HeapInuse))
	fmt.Printf("\tNumGC      = %4d\n",   m2.NumGC)
}

func bToMb(b uint64) uint64 {
	// full megabytes
	return b / 1024 / 1024
}

func timeMessage(format string, args... interface{}) {
  if gOptions.verbosity > 0 {
    Printf(format, args...)
  }
}
