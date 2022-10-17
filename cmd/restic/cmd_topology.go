package main
// compile with "go run build.go -tags debug""
// run with DEBUG_LOG=/home/wplapper/restic/debug.log fully-qualified-name/restic -r <repo> overview

// ref to onedrive: rclone:onedrive:restic_backups

import (
	// system
	"time"
	"golang.org/x/sync/errgroup"
	"sync"
	"sort"

	//argparse
	"github.com/spf13/cobra"

	// restic library
	"github.com/wplapper/restic/library/restic"
	//"github.com/wplapper/restic/library/debug"

	//deque
	"github.com/gammazero/deque"
)

type TopologyOptions struct {
		cutoff int
		snap string
}

type BlobFile2 struct {
	// name, size, type_, mtime, content and subtree ID
	name string
	Type string
	size uint64
	mtime time.Time
	content restic.IntSet
	subtree_ID restic.IntID
}

type IndexHandle struct {
		Type restic.BlobType
		size uint
}

type RepositoryData struct {
	snaps         []*restic.Snapshot
	directory_map map[restic.IntID][]BlobFile2
	fullpath      map[restic.IntID]string
	names         map[restic.IntID]string
	children      map[restic.IntID]restic.IntSet
	parents  			map[restic.IntID]restic.IntID

	// need to be (*)restic.ID
	meta_dir_map  map[*restic.ID]restic.IntSet
	index_handle	map[restic.ID]IndexHandle
	blob_to_index map[restic.ID]restic.IntID
	all_lin_ID		[]restic.ID
}

var EMPTY_NODE_ID restic.ID
var EMPTY_NODE_ID_TRANSLATED restic.IntID
type IntID int

func init_repositoryData() *RepositoryData {
	var repositoryData RepositoryData
	repositoryData.snaps         = []*restic.Snapshot{}
	repositoryData.directory_map = make(map[restic.IntID][]BlobFile2)
	repositoryData.fullpath      = make(map[restic.IntID]string)
	repositoryData.names         = make(map[restic.IntID]string)
	repositoryData.children			 = make(map[restic.IntID]restic.IntSet)
	repositoryData.parents			 = make(map[restic.IntID]restic.IntID)

	repositoryData.meta_dir_map  = make(map[*restic.ID]restic.IntSet)
	repositoryData.index_handle	 = make(map[restic.ID]IndexHandle)
	repositoryData.blob_to_index = make(map[restic.ID]restic.IntID)
	repositoryData.all_lin_ID    = []restic.ID{}
	return &repositoryData
 }

var cmdTopology = &cobra.Command{
	Use:   "topology [flags]",
	Short: "generate topology of one snapshot",
	Long: `generate topology for all snapshots.

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
	DisableAutoGenTag: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runTopology(globalOptions, args)
	},
}

func init() {
	cmdRoot.AddCommand(cmdTopology)
}

func runTopology(gopts GlobalOptions, args []string) error {

	// step 0: setup global stuff
	start := time.Now()
	repositoryData := init_repositoryData()
	EMPTY_NODE_ID = restic.Hash([]byte("{\"nodes\":[]}\n"))
	gOptions = gopts

	// step 1: open repository
	repo, err := OpenRepository(gopts)
	if err != nil {
		return err
	}
	timeMessage("%-30s %10.1f seconds\n", "open repository", time.Now().Sub(start).Seconds())

	// step 4.1: manage Index Records
	start = time.Now()
	if err = HandleIndexRecords(gopts, repo); err != nil {
		return err
	}

	// need the meta_blobs from the master index,
	//setup index_handle, blob_to_index and all_lin_ID
	blobs_from_ix := restic.NewIntSet()
	pos := restic.IntID(0)
	for blob := range repo.Index().Each(gopts.ctx) { // very fast 0.2s for ca. 230000 blobHandles
		repositoryData.index_handle[blob.ID] = IndexHandle{Type: blob.Type, size: blob.Length}
		repositoryData.blob_to_index[blob.ID] = pos
		repositoryData.all_lin_ID = append(repositoryData.all_lin_ID, blob.ID)
		if blob.Type == restic.TreeBlob {
			blobs_from_ix.Insert(pos)
		}
		pos += 1
	}
	EMPTY_NODE_ID_TRANSLATED = repositoryData.blob_to_index[EMPTY_NODE_ID]

	if blobs_from_ix.Has(EMPTY_NODE_ID_TRANSLATED) {
		blobs_from_ix.Delete(EMPTY_NODE_ID_TRANSLATED)
	}
	timeMessage("%-30s %10.1f seconds\n", "repo.Index().Each()", time.Now().Sub(start).Seconds())
	//timeMessage("Index has %d entries\n", countx)

	start = time.Now()
	GatherAllRepoData(gopts, repo, repositoryData)
	timeMessage("%-30s %10.1f seconds\n", "GatherAllRepoData", time.Now().Sub(start).Seconds())

	// convert the keys of 'directory_map' to a set
	idd_file_keys := restic.NewIntSet()
	for key := range repositoryData.directory_map {
			idd_file_keys.Insert(key)
	}

	//compare the sets
	if !idd_file_keys.Equals(blobs_from_ix)  {
		rest := blobs_from_ix.Sub(idd_file_keys)
		Printf("len blobs_from_ix      %7d\n", len(blobs_from_ix))
		Printf("len keys directory_map %7d\n", len(idd_file_keys))
		Printf("len rest               %7d\n", len(rest))
		//Printf("rest %v\n", rest.String()[:12])
	}

	count := 0
	for _, lists := range repositoryData.directory_map {
		count += len(lists)
	}
	countxx := 0
	for _, blob_list := range repositoryData.meta_dir_map {
		countxx += len(blob_list)
	}
	Printf("number of nodes in idd_file    %7d\n", count)
	Printf("number of elements in meta_dir %7d\n", countxx)


	//service1(repositoryData)
	service2(repositoryData)
	return nil
}

// GetAllNodes traverses the tree ID and adds all seen blobs (trees and data
// blobs) to the set blobs. Already seen tree blobs will not be visited again.
// function extended to deliver the key elements of each record in a directory.
func GetAllNodes(gopts GlobalOptions, repo restic.Repository, treeIDs []restic.ID,
// input & returns
repositoryData *RepositoryData) error {
	var lock sync.Mutex
	wg, ctx := errgroup.WithContext(gopts.ctx)

	// start processing
	treeStream := restic.StreamTrees(ctx, wg, repo, treeIDs, func(treeID restic.ID) bool {
		// locking is essential, if omitted, the channels will break
		lock.Lock()
		if treeID == EMPTY_NODE_ID {
			lock.Unlock()
			return true
		}
		// we dont need to reprocess data which we have aready seen
		position, ok := repositoryData.blob_to_index[treeID]
		if !ok {
				Printf("requesting ID %v\n", treeID)
				panic("error in GetAllNodes")
		}
		_, ok = repositoryData.directory_map[position]
		lock.Unlock()
		return ok
	}, nil)

	wg.Go(func() error {
		for tree := range treeStream {
			if tree.Error != nil {
				return tree.Error
			}

			idd_file_list := make([]BlobFile2, 0, len(tree.Nodes))
			lock.Lock()
			for _, node := range tree.Nodes {
				content := restic.NewIntSet()
				subtree_ID := EMPTY_NODE_ID_TRANSLATED
				ok := false
				switch node.Type {
				case "file":
					for _,cont := range node.Content {
						position, ok := repositoryData.blob_to_index[cont]
						if !ok {
							Printf("Fatal: %v not in blob_to_index\n", cont)
							panic("error during content processing")
						}
						content.Insert(position)
					}
				case "dir":
					subtree_ID, ok = repositoryData.blob_to_index[*node.Subtree]
					if !ok {
						Printf("Fatal: %v not in blob_to_index\n", *node.Subtree)
						panic("error during content processing")
					}
				}
				bf := BlobFile2{name: node.Name, Type: node.Type, size: node.Size,
						mtime: node.ModTime, content: content, subtree_ID: subtree_ID}
				idd_file_list = append(idd_file_list, bf)
			}
			// we need a permanent ID pointer
			position, ok := repositoryData.blob_to_index[tree.ID]
			if !ok {
					Printf("requesting ID %v\n", tree.ID)
					panic("error in GetAllNodes")
			}
			// update directory_map
			repositoryData.directory_map[position] = idd_file_list
			lock.Unlock()
		}
		return nil
	})
	err := wg.Wait()
	return err
}

// build a topology structure for one snapshot
// the function relies on 'children' being initialized properly
func topology_structure(sn restic.Snapshot, repositoryData *RepositoryData) {
	// for each snapshot, there are 2 fixed elements:
	// the tree root and the empty directory

	tree_root := repositoryData.blob_to_index[*sn.Tree]
	seen := restic.NewIntSet(tree_root, EMPTY_NODE_ID_TRANSLATED)
	repositoryData.fullpath[tree_root] = "/"
	// to_be_processed is the list of new meta_blobs, not yet handled
	// use deque as a FIFO queue
	to_be_processed := deque.New[restic.IntID](100)

	// prime the process loop
	to_be_processed.PushBack(tree_root)
	for to_be_processed.Len() > 0 {
		//take out oldest one
		blob 				:= to_be_processed.PopFront()
		for bl := range repositoryData.children[blob].Sub(seen) {
			seen.Insert(bl)
			to_be_processed.PushBack(bl)

			// manage fullpath
			if repositoryData.fullpath[blob] != "/" {
				repositoryData.fullpath[bl] = repositoryData.fullpath[blob] + "/" + repositoryData.names[bl]
			} else {
				repositoryData.fullpath[bl] = "/" + repositoryData.names[bl]
			}
		}
	}
	// at the end of the loop, 'seen' contains all directories
	// referenced in the snapshot
	repositoryData.meta_dir_map[sn.ID()] = seen
}

func GatherAllRepoData(gopts GlobalOptions, repo restic.Repository,
repositoryData *RepositoryData) error {
	// step 1: gather snapshots
	snaps, err := GatherAllSnapshots(gopts, repo)
	if err != nil {
			return err
	}
	repositoryData.snaps = snaps

	// step 2: collect all data for idd_file
	tree_list := make([]restic.ID, 1, 1)
	for _, sn := range repositoryData.snaps {
		tree_list[0] = *sn.Tree
		// a lot of stuff gets updated inside GetAllNodes
		if err = GetAllNodes(gopts, repo, tree_list, repositoryData); err != nil {
			return err
		}
	}

	// step 3: prepare children and parents from idd_file records
	for meta_blob, idd_file_list := range repositoryData.directory_map {
		for _,node := range idd_file_list {
			if node.Type == "dir" {
				// initialize
				repositoryData.names[node.subtree_ID] = node.name
				if len(repositoryData.children[meta_blob]) == 0 {
					repositoryData.children[meta_blob] = restic.NewIntSet()
				}
				// the children data will be used in topology step
				repositoryData.children[meta_blob].Insert(node.subtree_ID)

				// get pointer to parent - currently not used
				if node.subtree_ID != EMPTY_NODE_ID_TRANSLATED {
					repositoryData.parents[node.subtree_ID] = meta_blob
				}
			}
		}
	}

	// step 4: build topology for each snapshot in repository
	for _, sn := range repositoryData.snaps {
		topology_structure(*sn, repositoryData)
	}
	return nil
}

// example: list the directory names of the first snapshot
func service1 (repositoryData *RepositoryData) {
	sn := repositoryData.snaps[0]
	directory_names := make([]string, 0, len(repositoryData.fullpath))
	for meta_blob := range repositoryData.meta_dir_map[sn.ID()] {
		//position := repositoryData.blob_to_index[meta_blob]
		//index := &(repositoryData.all_lin_ID[position])
		directory_names = append(directory_names, repositoryData.fullpath[meta_blob])
	}
	// sort alphabetically
	sort.SliceStable(directory_names, func(i, j int) bool {
		return directory_names[i] < directory_names[j] })

	for _, name := range directory_names {
		Printf("%s\n", name)
	}
}

// example2: calculate wich blobs are unique to first (or second) snapshot
func service2 (repositoryData *RepositoryData) {
	//ln := len(repositoryData.snaps)
	sn1 := repositoryData.snaps[0]
	Printf("snap_ID %s = %s:%s at %s\n", sn1.ID().Str(),
		sn1.Hostname, sn1.Paths[0], sn1.Time.String()[:19])
	start := time.Now()

	// reference to all meta_blobs
	collect_blobs := repositoryData.meta_dir_map[sn1.ID()]
	for meta_blob := range repositoryData.meta_dir_map[sn1.ID()] {
		for _, meta := range repositoryData.directory_map[meta_blob] {
			if meta.Type == "file" {
				collect_blobs.Merge(meta.content)
			}
		}
	}

	// find the snaps for all other snapshots
	all_other_snaps := make([]*restic.Snapshot, 0, len(repositoryData.snaps))
	for _, sn  := range repositoryData.snaps {
		if sn == sn1 {
			continue
		}
		all_other_snaps = append(all_other_snaps, sn)
	}


	// loop over all_other_snaps
	all_other_blobs := restic.NewIntSet()
	for _, sn := range all_other_snaps {
		all_other_blobs.Merge(repositoryData.meta_dir_map[sn.ID()])
		for meta_blob := range repositoryData.meta_dir_map[sn.ID()] {
			for _, meta := range repositoryData.directory_map[meta_blob] {
				if meta.Type == "file" {
					all_other_blobs.Merge(meta.content)
				}
			}
		}
	}
	timeMessage("%-30s %10.1f seconds\n", "collect unique blobs", time.Now().Sub(start).Seconds())

	// the difference is unique to sn1
	difference := collect_blobs.Sub(all_other_blobs)

	// find their sizes
	size_meta  := uint64(0)
	size_data  := uint64(0)
	count_meta := 0
	count_data := 0
	for blob := range difference {
		data  := repositoryData.index_handle[repositoryData.all_lin_ID[blob]]
		size  := uint64(data.size)
		if data.Type == restic.TreeBlob {
				count_meta++
				size_meta += size
		} else if data.Type == restic.DataBlob {
				count_data++
				size_data += size
		}
	}
	Printf("meta  count is %7d sz %7.1f MiB\n", count_meta, float64(size_meta) / ONE_MEG)
	Printf("data  count is %7d sz %7.1f MiB\n", count_data, float64(size_data) / ONE_MEG)
	Printf("total count is %7d sz %7.1f MiB\n", count_meta + count_data, float64(size_meta + size_data) / ONE_MEG)
}
