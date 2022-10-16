package main

import (
	"sort"
	"runtime"
	"fmt"
	"time"
	"context"
	"sync"
	"golang.org/x/sync/errgroup"

	// restic library
	"github.com/wplapper/restic/library/restic"
	"github.com/wplapper/restic/library/repository"
	"github.com/wplapper/restic/library/debug"
	"github.com/wplapper/restic/library/walker"
)

func GatherAllSnapshots(gopts GlobalOptions, repo restic.Repository,
snaps []*restic.Snapshot) ([]*restic.Snapshot, error) {
	// collect all snap records
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

func HandleIndexRecords(gopts GlobalOptions,
repo restic.Repository) error {
	// load index files and their contents
	// we need to load the Index first
	if err := repo.LoadIndex(gopts.ctx); err != nil {
		return err
	}

	repository.ForAllIndexes(gopts.ctx, repo, func(id restic.ID,
	idx *repository.Index, oldFormat bool, err error) error {
		if err != nil {
			Printf("Ignoring error %v for ID %v\n", err, id)
			return err
		}
		return nil
	})
	return nil
}

func CalculatePruneSize(gopts GlobalOptions, repo restic.Repository,
	selected []*restic.Snapshot, snaps []*restic.Snapshot,
	blobs_from_ix map[restic.ID]Pack_and_size,
	blobs_per_packID map[restic.ID]restic.IDSet) error {
	start := time.Now()
	//debug.Log("CalculatePruneSize start %v", selected)
	// step 5: find blobs for the selected snaps
	//start := time.Now()
	usedBlobs_delete := restic.NewBlobSet()
	tree_list := make([]restic.ID, 0, len(selected))
	for _, sn := range selected {
		tree_list = append(tree_list, *sn.Tree)
	}
	err := restic.FindUsedBlobs(gopts.ctx, repo, tree_list, usedBlobs_delete, nil)
	if err != nil {
		return err
	}
	debug.Log("FindUsedBlobs - single done")

	// step 6: build the tree list for the rest of the snapshots
	rest_tree_list := make([]restic.ID, 0, len(snaps) - len(selected))
	for _, sn := range snaps {
		found := false
		for _, sn2 := range selected {
			if sn == sn2 {
				found = true
				break
			}
		}
		if found {
			continue
		}
		rest_tree_list = append(rest_tree_list, *sn.Tree)
	}

	usedBlobs_keep := restic.NewBlobSet()
	err = restic.FindUsedBlobs(gopts.ctx, repo, rest_tree_list, usedBlobs_keep, nil)
	if err != nil {
		return err
	}
	debug.Log("FindUsedBlobs -all other done")

	// step 7: get the pack information
	unique_blobs := usedBlobs_delete.Sub(usedBlobs_keep)
	delete_packs := make(map[restic.ID]restic.IDSet, len(unique_blobs))
	// map these blobs back to pack IDs
	for blob := range unique_blobs {
		packID := blobs_from_ix[blob.ID].PackID
		//initialize
		if len(delete_packs[packID]) == 0 {
			delete_packs[packID] = restic.NewIDSet()
		}
		delete_packs[packID].Insert(blob.ID)
	}

	// step 8: summarize
	count_delete_packs  := 0
	count_repack_blobs  := 0
	count_delete_blobs  := 0
	count_partial_blobs := 0
	size_delete_blobs   := uint64(0)
	size_repack_blobs   := uint64(0)
	size_partial_blobs  := uint64(0)
	for PackID := range delete_packs {
		if len(delete_packs[PackID]) == len(blobs_per_packID[PackID]) {
			// straight delete!
			count_delete_packs++
			for blob := range delete_packs[PackID] {
				size_delete_blobs += uint64(blobs_from_ix[blob].Size)
				count_delete_blobs++
			}
		} else {
			// needs repacking
			for blob := range blobs_per_packID[PackID] {
				size_repack_blobs += uint64(blobs_from_ix[blob].Size)
				count_repack_blobs++
			}
			for blob := range delete_packs[PackID] {
				size_partial_blobs += uint64(blobs_from_ix[blob].Size)
				count_partial_blobs++
			}
		}
	}

	Printf("straight delete %10d blobs %7d packs %10.3f Mib\n",
		count_delete_blobs, count_delete_packs, float64(size_delete_blobs) / ONE_MEG)
	Printf("this removes    %10d blobs %7s       %10.3f Mib\n",
		count_partial_blobs, " ", float64(size_partial_blobs) /ONE_MEG)
	Printf("repack          %10d blobs %7d packs %10.3f Mib\n",
		count_repack_blobs, len(delete_packs) - count_delete_packs,
		float64(size_repack_blobs) /ONE_MEG)
	Printf("total prune     %10d blobs %7s       %10.3f Mib\n",
		count_partial_blobs + count_delete_blobs, " ",
		float64(size_partial_blobs + size_delete_blobs) /ONE_MEG)
	debug.Log("CalculatePruneSize Done!")

	if gopts.verbosity > 0 {
		Printf("%-30s %10.1f seconds\n", "selected all blobs",
			time.Now().Sub(start).Seconds())
		PrintMemUsage()
	}
	return nil
}

// PrintMemUsage outputs the current, total and OS memory being used.
// As well as the number of garbage collection cycles completed.
func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc        = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys        = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC      = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	// full megabytes
	return b / 1024 / 1024
}

func BuildTopology(sn *restic.Snapshot, gopts GlobalOptions,
repo restic.Repository) ([]restic.ID, error) {

	Printf("snap_ID %s %s:%s at %s\n", sn.ID().Str(),
		sn.Hostname, sn.Paths[0], sn.Time.String()[:19])
	seen := restic.NewIDSet()
	tree_blobs := make([]restic.ID, 0)
	err := walker.Walk(gopts.ctx, repo, *sn.Tree, nil,
			func(id restic.ID, nodepath string, node *restic.Node, err error) (bool, error) {
		if err != nil {
			return false, err
		}
		if node == nil {
			return false, nil
		}

		if node.Type == "dir" && !seen.Has(id) {
			tree_blobs = append(tree_blobs, id)
			seen.Insert(id)
		}
		return false, nil
	})

	if err != nil {
		return nil, err
	}
	return tree_blobs, nil
}

// FindUsedBlobs traverses the tree ID and adds all seen blobs (trees ony(!)
// blobs) to the set blobs. Already seen tree blobs will not be visited again.
func FindUsedMetaBlobs(ctx context.Context, repo restic.Loader, treeIDs restic.IDs, blobs restic.BlobSet) error {
	var lock sync.Mutex

	wg, ctx := errgroup.WithContext(ctx)
	treeStream := restic.StreamTrees(ctx, wg, repo, treeIDs, func(treeID restic.ID) bool {
		// locking is necessary
		lock.Lock()
		h := restic.BlobHandle{ID: treeID, Type: restic.TreeBlob}
		blobReferenced := blobs.Has(h)
		// noop if already referenced
		blobs.Insert(h)
		lock.Unlock()
		return blobReferenced
	}, nil)

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

