package main
// compile with "go run build.go -tags debug""
// run with DEBUG_LOG=/home/wplapper/restic/debug.log fully-qualified-name/restic -r <repo> overview

// ref to onedrive: rclone:onedrive:restic_backups

import (
	// system
	"time"
	"golang.org/x/sync/errgroup"
	"sync"
	//"sort"

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

type BlobFile struct {
	// name, size, type_, mtime, translated_data
	name string
	Type string
	mtime time.Time
	content []restic.ID
	size uint64
	subtree_ID restic.ID
}

var topologyOptions TopologyOptions

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
	// step 1: open repository
	//debug.Log("Start tremove")
	EMPTY_NODE_ID := restic.Hash([]byte("{\"nodes\":[]}\n"))
	start := time.Now()
	repo, err := OpenRepository(gopts)
	if err != nil {
		return err
	}
	if gopts.verbosity > 0 {
		Printf("%-30s %10.1f seconds\n", "open repository",
				time.Now().Sub(start).Seconds())
	}

	// step 2: gather all snapshots
	start = time.Now()
	snaps := make([]*restic.Snapshot, 0, 10)
	snap_dict := make(map[restic.ID]restic.Snapshot)
	// snaps is []*restic.Snapshot
	snaps, err = GatherAllSnapshots(gopts, repo, snaps)
	if err != nil {
			return err
	}
	for _, sn := range snaps {
			snap_dict[*sn.ID()] = *sn
	}

	// step 4.1: manage Index Records
	start = time.Now()
	err = HandleIndexRecords(gopts, repo)
	if err != nil {
		return err
	}

	// need the meta_blobs from the master index
	blobs_from_ix := restic.NewIDSet()
	for blob := range repo.Index().Each(gopts.ctx) {
		if blob.Type == restic.TreeBlob {
			blobs_from_ix.Insert(blob.ID)
		}
	}
	if gopts.verbosity > 0 {
		Printf("%-30s %10.1f seconds\n", "read all index records",
			time.Now().Sub(start).Seconds())
	}

	// we need a slice of length one
	tree_list     := make([]restic.ID, 1, 1)
	// directory_map contains al relevant information about a file or directory
	directory_map := make(map[restic.ID][]BlobFile)

	start = time.Now()
	for _, sn := range snaps {
		tree_list[0] = *sn.Tree
		// directory_map gets updated inside GetAllNodes
		err = GetAllNodes(gopts, repo, tree_list, directory_map)
		if err != nil {
			return err
		}
	}
	Printf("\rprocessing finished\n")
	if gopts.verbosity > 0 {
		Printf("%-30s %10.1f seconds\n", "generate all idd_file entries",
				time.Now().Sub(start).Seconds())
	}

	// convert the keys of 'directory_map' to a set
	idd_file_keys := restic.NewIDSet()
	for key := range directory_map {
			idd_file_keys.Insert(key)
	}
	if blobs_from_ix.Has(EMPTY_NODE_ID) {
		blobs_from_ix.Delete(EMPTY_NODE_ID)
	}

	count := 0
	for _, lists := range directory_map {
		count += len(lists)
	}
	Printf("number of nodes in idd_file %d\n", count)

	//compare the sets
	if !idd_file_keys.Equals(blobs_from_ix)  {
		rest := blobs_from_ix.Sub(idd_file_keys)
		Printf("len blobs_from_ix      %7d\n", len(blobs_from_ix))
		Printf("len keys directory_map %7d\n", len(idd_file_keys))
		Printf("len rest               %7d\n", len(rest))
		Printf("rest %v\n", rest.String()[:12])
	}

	// construct children and parents: the set of all children of a node
	// must be created before calling 'topology_structure'
	start = time.Now()
	children := make(map[restic.ID]restic.IDSet)
	parents  := make(map[restic.ID]restic.ID)
	names    := make(map[restic.ID]string)
	for meta_blob, idd_file_list := range directory_map {
		for _,node := range idd_file_list {
			if node.Type == "dir" {
				// initialize
				names[node.subtree_ID] = node.name
				if len(children[meta_blob]) == 0 {
					children[meta_blob] = restic.NewIDSet()
				}
				children[meta_blob].Insert(node.subtree_ID)

				if node.subtree_ID != EMPTY_NODE_ID {
					parents[node.subtree_ID] = meta_blob
				}
			}
		}
	}
	Printf("%-30s %10.1f seconds\n", "children from idd_file",
			time.Now().Sub(start).Seconds())

	// construct meta_dir by building the topology
	start = time.Now()
	meta_dir := make(map[restic.ID]restic.IDSet)
	fullpath := make(map[restic.ID]string)
	for _, sn := range snaps {
		seen := topology_structure(*sn, directory_map, children, names, fullpath)
		meta_dir[*sn.ID()] = seen
	}
	Printf("%-30s %10.1f seconds\n", "create topology",
			time.Now().Sub(start).Seconds())

	// exampe: list the directory names of the first snapshot
	/*
	sn := snaps[0]
	directory_names := make([]string, 0, len(fullpath))
	for meta_blob := range meta_dir[*sn.ID()] {
		directory_names = append(directory_names, fullpath[meta_blob])
	}
	// sort alphabetically
	sort.SliceStable(directory_names, func(i, j int) bool {
		return directory_names[i] < directory_names[j] })
	for _, name := range directory_names {
		Printf("%s\n", name)
	}
	*/

	countx := 0
	for _, blob_list := range meta_dir {
		countx += len(blob_list)
	}
	Printf("number of elements in meta_dir %d\n", countx)
	PrintMemUsage()
	return nil
}

// GetAllNodes traverses the tree ID and adds all seen blobs (trees and data
// blobs) to the set blobs. Already seen tree blobs will not be visited again.
// function extended to deliver the key elements of each record in a directory.
func GetAllNodes(gopts GlobalOptions, repo restic.Repository, treeIDs []restic.ID,
// returns
directory_map map[restic.ID][]BlobFile) error {
	EMPTY_NODE_ID := restic.Hash([]byte("{\"nodes\":[]}\n"))
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
		_, ok := directory_map[treeID]
		lock.Unlock()
		return ok
	}, nil)

	wg.Go(func() error {
		for tree := range treeStream {
			if tree.Error != nil {
				return tree.Error
			}

			idd_file_list := make([]BlobFile, 0, len(tree.Nodes))
			lock.Lock()
			for _, node := range tree.Nodes {
				content := []restic.ID{}
				subtree_ID := EMPTY_NODE_ID
				switch node.Type {
				case "file":
					content = node.Content
				case "dir":
					subtree_ID = *node.Subtree
				}
				bf := BlobFile{name: node.Name, Type: node.Type, size: node.Size,
						mtime: node.ModTime, content: content, subtree_ID: subtree_ID}
				idd_file_list = append(idd_file_list, bf)
			}
			directory_map[tree.ID] = idd_file_list
			lock.Unlock()
		}
		return nil
	})
	err := wg.Wait()
	return err
}

// build a topology structure for one snapshot
// the function relies on 'children' being initialized properly
func topology_structure(sn restic.Snapshot,
directory_map map[restic.ID][]BlobFile,
children map[restic.ID]restic.IDSet,
names map[restic.ID]string,
// fullpath gts updated
fullpath map[restic.ID]string) restic.IDSet {
		EMPTY_NODE_ID := restic.Hash([]byte("{\"nodes\":[]}\n"))

		// for each snapshot, there are 2 fixed elements:
		// the tree root and the empty directory
		seen := restic.NewIDSet(*sn.Tree, EMPTY_NODE_ID)
		fullpath[*sn.Tree] = "/"

		// to_be_processed is the list of new meta_blobs, not yet handled
		// use deque as a FIFO queue
		to_be_processed := deque.New[restic.ID](100)

		// prime the process loop
		to_be_processed.PushBack(*sn.Tree)
		for to_be_processed.Len() > 0 {
			//take out oldest one
			blob := to_be_processed.PopFront()
			for bl := range children[blob].Sub(seen) {
				seen.Insert(bl)
				to_be_processed.PushBack(bl)

				// manage fullpath
				if fullpath[blob] != "/" {
					fullpath[bl] = fullpath[blob] + "/" + names[bl]
				} else {
					fullpath[bl] = "/" + names[bl]
				}
			}
		}
		// at the end of the loop, 'seen' contains all directories
		// referenced in the snapshot
		return seen
}

/*
// my first async function, copied from library/restic/find.go and adapted
func getMetaBlobs(gopts GlobalOptions, repo restic.Repository,
treeIDs []restic.ID, blobs restic.IDSet) error  {
	EMPTY_NODE_ID := restic.Hash([]byte("{\"nodes\":[]}\n"))
	var lock sync.Mutex
	wg, ctx := errgroup.WithContext(gopts.ctx)

	treeStream := restic.StreamTrees(ctx, wg, repo, treeIDs, func(treeID restic.ID) bool {
		// locking is necessary
		if treeID == EMPTY_NODE_ID {
				return true
		}
		lock.Lock()
		//blobReferenced := blobs.Has(h)
		// noop if already referenced
		blobs.Insert(treeID)
		lock.Unlock()
		return false
	}, nil)

	//part2
	wg.Go(func() error {
		for tree := range treeStream {
			if tree.Error != nil {
				return tree.Error
			}
		}
		return nil
	})
	return wg.Wait()
}
*/
