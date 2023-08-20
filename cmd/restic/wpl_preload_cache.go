


package main

import (
	// system
	"context"
	"io/fs"
	"os"
	"strings"
	"path/filepath"

	//argparse
	"github.com/spf13/cobra"

	// restic library
	"github.com/wplapper/restic/library/cache"
	"github.com/wplapper/restic/library/repository"
	"github.com/wplapper/restic/library/restic"

	// sets
	"github.com/deckarep/golang-set/v2"
)

var cmdPreloadCache = &cobra.Command{
	Use:   "preload-cache [flags]",
	Short: "wpl make sure that the cache is current",
	Long: `wpl make sure that the cache is current.

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
	DisableAutoGenTag: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runPreloadCache(cmd.Context())
	},
}

func init() {
	cmdRoot.AddCommand(cmdPreloadCache)
}

// make sure that all index and snapshot files have a copy in the cache
// as a consequence, all metablob packs are loaded as well
func runPreloadCache(ctx context.Context) error {

	var err error

	// step 1: open repository
	repo, err := OpenRepository(ctx, globalOptions)
	if err != nil {
		Printf("repository %s is busted!\n", globalOptions.Repo)
		return err
	}
	Printf("repo is %s\n", globalOptions.Repo)

	var cache_dir string
	config := repo.Config()
	cache_dir, err = cache.DefaultDir()
	subdir_name := cache_dir + "/" + config.ID

	// step 2: load the index file(s)
	Verbosef("LoadIndex\n")
	if err := repo.LoadIndex(ctx); err != nil {
		Printf("repo.LoadIndex failed. Error is '%v'\n", err)
		return err
	}

	// step 3: loop over MasterIndex - select only tree blobs
	packfiles := make(map[restic.ID]restic.ID)
	repo.Index().Each(ctx, func(blob restic.PackedBlob) {
		if blob.Type == restic.TreeBlob {
			packfiles[blob.PackID] = blob.ID
		}
	})

	// step 4: load one tree for each of the packfiles
	Verbosef("LoadTree\n")
	for _, ID := range packfiles {
		Verboseff("Loading tree for blob %s\n", ID.String()[:12])
		_, err := restic.LoadTree(ctx, repo, ID)
		if err != nil {
			Printf("LoadTree returned '%v' - ignored!\n", err)
		}
	}

	// step 5: load snapshots
	Verbosef("Snapshot Fileload\n")
	snap_set := mapset.NewSet[string]()
	repo.List(ctx, restic.SnapshotFile, func(ID restic.ID, size int64) error {
		sn, err := restic.LoadSnapshot(ctx, repo, ID)
		if err != nil {
			Printf("Skipped loading snap record %s! - reason: '%v' - ignored!\n",
				ID.String()[:12], err)
		}
		snap_set.Add(sn.ID().String())
		return nil
	})
	snaps_in_cache := walk_cache_dir(subdir_name + "/snapshots")

	// step 6: match front end and backend files for the following types
	// index, meta_blobs
	Printf("\nCheck index ...")
	err1 := walk_cache(ctx, repo, subdir_name + "/index", restic.IndexFile)
	if err1 == nil {
		Printf("OK\n")
	}
	Printf("Check meta data ... ")
	err2 := walk_cache(ctx, repo, subdir_name + "/data", restic.PackFile)
	if err2 == nil {
		Printf("OK\n")
	}

	Printf("Check snaps ... ")
	if snap_set.Equal(snaps_in_cache) {
		Printf("OK\n")
	} else {
		Print("not OK!\n")
		diff_set := snap_set.Difference(snaps_in_cache)
		Printf("\ndiff_set %+v\n\n", diff_set)
		remove_old_snaps(diff_set, subdir_name + "/snapshots")
		Printf("There are %d differences in the snapshots!\n", diff_set.Cardinality())
		for snap := range diff_set.Iter() {
			if snap_set.Contains(snap) {
				Printf("%s in snap_set\n", snap[:12])
			} else if snaps_in_cache.Contains(snap) {
				Printf("%s in snaps_in_cache\n", snap[:12])
			}
		}
	}
	return nil
}

// walk down a subtree and find the files,
// not interested in the directory structure
func walk_cache(ctx context.Context, repo *repository.Repository, root string,
	file_type restic.FileType) error {
	err := filepath.Walk(root, func(path string, info fs.FileInfo, err error) error {
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

		// check against backend
		basename := filepath.Base(path)
		handle := restic.Handle{Type: file_type, Name: basename}
		back_info, err := repo.Backend().Stat(ctx, handle)
		if err != nil || back_info.Size != info.Size() {
			// backend file does not exist, cache file has to go
			Printf("remove old cache file %s\n", path)
			err = os.Remove(path)
			if err != nil {
				Printf("Could not remove cache file %s - reason '%v' - ignored!\n",
					path, err)
			}
		}
		return nil
	})

	if err != nil {
		Printf("Could not walk cache -reason '%v'\n", err)
		return err
	}
	return nil
}

// function to check loaded snapshots vs cached snapshots
func walk_cache_dir(root string) mapset.Set[string] {
	result := mapset.NewSet[string]()
	error_set := mapset.NewSet[string]()
	err := filepath.Walk(root, func(path string, info fs.FileInfo, err error) error {
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

		// check for major inconsistency
		components := strings.Split(path, "/")
		lcomp := len(components)
		if lcomp >= 2 {
			before := components[lcomp - 2]
			lastst := components[lcomp - 1]
			prefix := lastst[0:2]
			if prefix != before {
				Printf("before is '%s', last[0:2] is '%s' last is %s\n", before, prefix,
					lastst)
				panic("some logic inconsistency")
			}
		}
		result.Add(filepath.Base(path))
		return nil
	})

	if err != nil {
		Printf("Could not walk cache -reason '%v'\n", err)
		return error_set
	}
	return result
}

func remove_old_snaps(snaps mapset.Set[string], base_path string) {
	// remove entries in 'snaps' from cache

	Printf("remove_old_snaps.start\n")
	for elem := range snaps.Iter() {
		subdir := elem[0:2]
		filename := base_path + "/" + subdir + "/" + elem
		Printf("Statting %s\n", filename)
		if _, err := os.Stat(filename); err == nil {
			Printf("remove %s\n", filename)
			err2 := os.Remove(filename)
			if err2 != nil {
				Printf("Can't remove file %s - reason %v\n", filename, err2)
				continue
			}
		} else {
			Printf("File %s not found in cache!\n", filename)
			continue
		}
		snaps.Remove(elem)
		Printf("dropped %s\n", elem)
	}
	Printf("remove_old_snaps.ended\n")
}
