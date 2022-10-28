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
		// could possibly be paralllized
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
	//start := time.Now()
	if err := repo.LoadIndex(gopts.ctx); err != nil {
		return err
	}
	// about 0.9 seconds
	//timeMessage("  %-30s %10.1f seconds\n", "LoadIndex",
	//	time.Now().Sub(start).Seconds())

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
	// about .5 seconds
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
	_ = start
	snaps, err := GatherAllSnapshots(gopts, repo);
	if err != nil {
			return err
	}
	repositoryData.snaps = snaps
	//timeMessage("  %-30s %10.1f seconds\n", "gather snapshots", time.Now().Sub(start).Seconds())

	// build a slice of all meta_blob IDs in the repo
	start = time.Now()
	err = ForAllMyTrees(gopts, repo, repositoryData)
	if err != nil {
		Printf("ForAllMyTrees returned %v\n", err)
		return err
	}
	//timeMessage("  %-28s %10.1f seconds\n", "ForAllMyTrees", time.Now().Sub(start).Seconds())

	// step 3: prepare children and parents from idd_file records
	FindChildren(repositoryData)

	// step 4: build topology for each snapshot in repository
	//start = time.Now()
	for _, sn := range repositoryData.snaps {
		topology_structure(*sn, repositoryData)
	}
	//end := time.Now() ())) // about 65-70 msec later
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
// method 'DeliverTreeBlobs' which accesses 'meta_blobs'
// which has been built beforehand
func ForAllMyTrees(gopts GlobalOptions, repo restic.Repository, repositoryData *RepositoryData) error {

	var m sync.Mutex
	type PerfRecord struct {
		id restic.ID
		count_file int
		count_dirs int
		time_diff time.Duration // int64 nanosecond count
	}
	//p_start := time.Now()
	perf_records := make([]PerfRecord, 0)
	wg, ctx := errgroup.WithContext(gopts.ctx)
	ch := make(chan restic.ID)
	//Printf("%-26s ForAllMyTrees start\n", time.Now().Format("2006-01-02 15:04:05.999999"))
	wg.Go(func() error {
		defer close(ch)

		// this callback function needs to return an 'id'
		return DeliverTreeBlobs(repositoryData, func(id restic.ID) error {
			select {
			case <-ctx.Done():
				return nil
			case ch <- id:
			}
			return nil
		})
	})
	//Printf("%-26s ForAllMyTrees filled chan\n", time.Now().Format("2006-01-02 15:04:05.999999"))

	// a worker receives an snapshot ID from ch, loads the snapshot
	// and runs fn with id, the snapshot and the error
	worker := func() error {
		count_file := 0
		count_dirs := 0
		for id := range ch {
			start := time.Now()
			//Printf("%-26s START %s\n", start.Format("2006-01-02 15:04:05.999999"), id.Str())
			tree, err := restic.LoadTree(gopts.ctx, repo, id)
			if err != nil {
				Printf("LoadTree returned %v\n", err)
				return err
			}
			idd_file_list := make([]BlobFile2, len(tree.Nodes))
			count_file = 0
			count_dirs = 0

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
						count_file++
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
					count_dirs++
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
			ende := time.Now()
			m.Lock()
			repositoryData.directory_map[position] = idd_file_list
			// perormance record
			perf_records = append(perf_records, PerfRecord{id: id,
				count_file: count_file, count_dirs: count_dirs, time_diff: ende.Sub(start)})
			m.Unlock()
			//Printf("%-26s FINIS %s\n", ende.Format("2006-01-02 15:04:05.999999"), id.Str())
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
	//Printf("%-26s Waited\n", time.Now().Format("2006-01-02 15:04:05.999999"))
	//p_end := time.Now()

	max_file := -1
	max_dirs := -1
	max_time := int64(-1)
	sum_time := int64(0)
	for _, perf_record := range perf_records {
		time_diff := int64(perf_record.time_diff) / 1000
		//Printf("pf %s %5d %5d %8d\n", perf_record.id.Str(),
		//  perf_record.count_file, perf_record.count_dirs, time_diff)

		if perf_record.count_file > max_file {
			max_file = perf_record.count_file
		}
		if perf_record.count_dirs > max_dirs {
			max_dirs = perf_record.count_dirs
		}
		if time_diff > max_time {
			max_time = time_diff
		}
		sum_time += time_diff
	}

	/*
	Printf("max_file = %5d max_dirs = %5d max_time %8d μs\n", max_file, max_dirs, max_time)
	Printf("# records %5d cpu_time %6.2f elapsed time %6.2f seconds accelerator %4.1f\n",
		len(perf_records),
		float64(sum_time) / 1.0e6, p_end.Sub(p_start).Seconds(),
		float64(sum_time) / 1.0e6 / p_end.Sub(p_start).Seconds())
	*/
	return res
}

// PrintMemUsage outputs the current, total and OS memory being used.
// As well as the number of garbage collection cycles completed.
func PrintMemUsage() {

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

