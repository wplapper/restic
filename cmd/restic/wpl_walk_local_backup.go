package main

// collect some globals stats about this repository

import (
	// system
	"context"
	//"crypto/sha256"
	//"sort"
	"strings"
	//"time"
	"io/fs"
	//"os"
	"path/filepath"

	//argparse
	"github.com/spf13/cobra"

	// restic library
	//"github.com/wplapper/restic/library/restic"

	// sets
	"github.com/deckarep/golang-set/v2"
)
var cmdWalkBackup = &cobra.Command{
	Use:   "wpl-walk-repo",
	Short: "walk down local repository and find outdated files",
	Long:  `walk down local repository and find outdated files.

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
	DisableAutoGenTag: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runWalkBackup(cmd.Context(), cmd, globalOptions)
	},
}

func init() {
	cmdRoot.AddCommand(cmdWalkBackup)
}

func runWalkBackup(ctx context.Context, cmd *cobra.Command, gopts GlobalOptions) error {

	var repositoryData RepositoryData

	repo, err := OpenRepository(ctx, gopts)
	if err != nil {
		return err
	}
	Printf("Repository is %s\n", globalOptions.Repo)
	init_repositoryData(&repositoryData)

	err = gather_base_data_repo(repo, gopts, ctx, &repositoryData,
		repo_details_options.Timing)
	if err != nil {
		return err
	}
	err = walk_local_backup(globalOptions.Repo, &repositoryData)
	if err != nil {
		return err
	}
	return nil
}

// walk down a subtree and find the files,
// not interested in the directory structure
func walk_local_backup(root string, repositoryData *RepositoryData) error {

	// to be found under the root of a restic backup:
	// config	data/  index/  keys/  locks/  snapshots/  wpl/packfiles.stat
	// make a list of all packfiles
	skipSet := mapset.NewThreadUnsafeSet("config", "packfiles.stat")
	skipDir := mapset.NewThreadUnsafeSet("keys", "locks", "index")
	if root[0:1] != "/" {
		comp := strings.Split(root, ":")
		root = "/root/onedrive/" + comp[len(comp) - 1]
		Printf("onedrive root: %s\n", root)
	}

	packfileSet := mapset.NewThreadUnsafeSet[string]()
	for _, ih := range repositoryData.IndexHandle {
		packfileSet.Add(repositoryData.IndexToBlob[ih.pack_index].String())
	}
	Printf("done packfileSet\n")

	// make a list of all snapshots
	//snapshots := repositoryData.SnapMap // key is string
	var countData, countSnap, countDataFail, countSnapFail int
	err := filepath.Walk(root, func(path string, info fs.FileInfo, err error) error {
		// skip on error
		if err != nil {
			Printf("prevent panic by handling failure accessing path %q: '%v'\n",
				path, err)
			return err
		}

		// skip directories
		if info.IsDir() {
			//Verboseff("skip directory %s\n", path)
			return nil
		}

		// split into components, match 'root' vs 'path'path[lenRoot:]
		lenRoot := len(root)
		shortPath := path[lenRoot+1:]
		components := strings.Split(shortPath, "/")
		basename := filepath.Base(shortPath)
		if skipSet.Contains(basename) {
			return nil
		}
		subPath := components[0]
		if skipDir.Contains(subPath) {
			return nil
		}
		switch subPath {
			case "data":
				if packfileSet.Contains(basename) {
					countData++
				} else {
					Printf("suspicous %s\n", basename)
					countDataFail++
				}
				return nil
			case "snapshots":
				if _, ok := repositoryData.SnapMap[basename]; ok {
					countSnap++
				} else {
					Printf("suspicous %s\n", basename)
					countSnapFail++
				}
				return nil
		}
		Printf("??comp %+v\n", components)
		return nil
	})

	Printf("countData     %7d\n", countData)
	Printf("packfiles     %7d\n", packfileSet.Cardinality())
	if countDataFail > 0 {
		Printf("countDataFail %7d\n", countDataFail)
	}
	Printf("countSnap     %7d\n", countSnap)
	Printf("in snapshots  %7d\n", len(repositoryData.SnapMap))
	if countSnapFail > 0 {
		Printf("countSnapFail %7d\n", countSnapFail)
	}

	if err != nil {
		Printf("Could not walk cache - reason '%v'\n", err)
		return err
	}
	return nil
}
