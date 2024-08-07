package main

import (
  // system
  "context"
  "io/fs"
  "os"
  "os/exec"
  "path/filepath"

  //argparse
  "github.com/spf13/cobra"

  // restic library
  //"github.com/wplapper/restic/library/cache"
  "github.com/wplapper/restic/library/backend/cache"
  "github.com/wplapper/restic/library/repository"
  "github.com/wplapper/restic/library/restic"
  "github.com/wplapper/restic/library/backend"

  // sets
  "github.com/deckarep/golang-set/v2"
)

type PreloadOptions struct {
  DryRun bool
  All    bool
}

var preloadOptions PreloadOptions

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
    return runPreloadCache(cmd.Context(), globalOptions)
  },
}

func init() {
  cmdRoot.AddCommand(cmdPreloadCache)
  f := cmdPreloadCache.Flags()
  f.BoolVarP(&preloadOptions.DryRun, "dry-run", "N", false, "dry-run the command")
  f.BoolVarP(&preloadOptions.All, "all", "A", false, "run the command for all well known repos")

}

func runPreloadCache(ctx context.Context, gopts GlobalOptions) error {
  if preloadOptions.All {
    for _, repoName := range WplRepositoryList {
      Printf("repo %s\n", repoName)
      if repoName[0:6] == "/media" {
        // execute usr/bin/ncat -z new-PC 22 -w 1s
        pingCmd := exec.Command("/usr/bin/ncat", "-z", "new-PC", "22", "-w", "1s")
        err := pingCmd.Run()
        if err != nil {
          Printf("Could not ping host new-PC on port 22\n")
          continue
        }
      }
      gopts.Repo = repoName
      err := runPreloadOne(ctx, gopts)
      if err != nil {
        continue
      }
    }
    return nil
  }
  return runPreloadOne(ctx, gopts)
}

func runPreloadOne(ctx context.Context, gopts GlobalOptions) error {
  // index files are always recreated with 'restic repair index' or 'restic prune'
  // only snapshat files in the local cache can be identified as outdated
  to_be_deleted := mapset.NewThreadUnsafeSet[string]()
  err := runPreloadCache1(ctx, gopts, to_be_deleted)
  if err != nil {
    return err
  }

  // delete the files which can only be found in the local cache.
  for filename := range to_be_deleted.Iter() {
    if !preloadOptions.DryRun {
      err := os.Remove(filename)
      if err == nil {
        Printf("file %s deleted\n", filepath.Base(filename))
      } else {
        Printf("%s failed with %v\n", filename, err)
      }
    } else {
      Printf("rm %s\n", filename)
    }
  }
  return nil
}

func runPreloadCache1(ctx context.Context, gopts GlobalOptions,
to_be_deleted mapset.Set[string]) error {
  repo, err := OpenRepository(ctx, gopts)
  if err != nil {
    Printf("repository %s is busted!\n", globalOptions.Repo)
    return err
  }
  Verboseff("repo is %s\n", globalOptions.Repo)

  var cache_dir string
  config := repo.Config()
  cache_dir, err = cache.DefaultDir()
  subdir_name := cache_dir + "/" + config.ID

  // step 2: load the index file(s)
  Verbosef("LoadIndex\n")
  if err := repo.LoadIndex(ctx, nil); err != nil {
    Printf("repo.LoadIndex failed. Error is '%v'\n", err)
    return err
  }

  // step 5: load snapshots
  Verbosef("Snapshot Fileload\n")
  snap_set := mapset.NewThreadUnsafeSet[string]()
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
  //Verbosef("%7d snapshot files in cache\n", snaps_in_cache.Cardinality())
  //Verbosef("%7d snapshot files in repo \n", snap_set.Cardinality())

  // step 6: match front end and backend files for the following types
  // index, meta_blobs
  Verbosef("\nCheck index  ...")
  err1 := walk_cache(ctx, repo, subdir_name+"/index", restic.IndexFile)
  if err1 == nil {
    Verbosef("OK\n")
  }
  Verbosef("Check meta   ...")
  err2 := walk_cache(ctx, repo, subdir_name+"/data", restic.PackFile)
  if err2 == nil {
    Verbosef("OK\n")
  }

  processed, err := restic.RemoveAllLocks(ctx, repo)
  if err != nil {
    Printf("Could not unlock - reason %v\n", err)
    return err
  }

  if processed > 0 {
    Verbosef("successfully removed %d locks\n", processed)
  }
  repo.Close()

  Verbosef("Check snaps  ...")
  if snap_set.Equal(snaps_in_cache) {
    Verbosef("OK\n")
  } else {
    Print("not OK!\n")
    diff_set2 := snaps_in_cache.Difference(snap_set)
    for snapshotLong := range diff_set2.Iter() {
      filename := subdir_name + "/snapshots/" + snapshotLong[0:2] + "/" + snapshotLong
      to_be_deleted.Add(filename)
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
    handle := backend.Handle{Type: file_type, Name: basename}
    back_info, err := repo.Backend().Stat(ctx, handle)
    if err != nil || back_info.Size != info.Size() {
      // backend file does not exist, cache file has to go
      Verbosef("remove old cache file %s\n", path)
      err = os.Remove(path)
      if err != nil {
        Printf("Could not remove cache file %s - reason '%v' - ignored!\n",
          path, err)
      }
    }
    return nil
  })

  if err != nil {
    Printf("Could not walk cache - reason '%v'\n", err)
    return err
  }
  return nil
}

// function to check loaded snapshots vs cached snapshots
func walk_cache_dir(root string) (result mapset.Set[string]) {
  result = mapset.NewThreadUnsafeSet[string]()
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

    result.Add(filepath.Base(path))
    return nil
  })

  if err != nil {
    Printf("Could not walk cache - reason '%v'\n", err)
    return mapset.NewThreadUnsafeSet[string]()
  }
  return result
}
