package main

import (
	// system
	"context"
	"os"
	"encoding/json"
	"errors"
	"fmt"
	"bufio"
	"sort"

	//argparse
	"github.com/spf13/cobra"

	// restic library
	"github.com/wplapper/restic/library/restic"
	"github.com/wplapper/restic/library/repository"
)

var cmdExportMeta = &cobra.Command{
	Use:   "wpl-export",
	Short: "export metadata of repo to plaintext directory structure",
	Long: `export metadata of repo to plaintext directory structure.

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
	DisableAutoGenTag: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runExportMeta(cmd.Context(), cmd, globalOptions, args)
	},
}

func init() {
	cmdRoot.AddCommand(cmdExportMeta)
}

// convert all the metadata into plain files with a directory strcuture like
// a repository
func runExportMeta(ctx context.Context, cmd *cobra.Command, gopts GlobalOptions, args []string) error {
	var (
		err              error
		repositoryData   RepositoryData
		repoUnpackedName string
	)

	gOptions = gopts
	init_repositoryData(&repositoryData)

	// step: open repository
	repo, err := OpenRepository(ctx, gopts)
	if err != nil {
		return err
	}
	defer repo.Close()
	Verboseff("Repository is %s\n", globalOptions.Repo)

	// check for given repo name
	repoLongName := map_repo_names(gopts.Repo)
	if len(args) > 0  {
		dirname := args[0]
		fileInfo, err := os.Stat(dirname)
		if err == nil && ! fileInfo.IsDir() {
			Printf("export name exists, but not a directory %s\n", dirname)
			return errors.New("export name exists, but not a directory. Aborting!")
		} else if err != nil {
			errx := os.MkdirAll(dirname, 0o755)
			if errx != nil {
				Printf("Cant create directory %s - reason %v\n", dirname, errx)
				return errx
			}
		}
		repoUnpackedName = dirname
	} else if repoLongName != gopts.Repo {
		repoUnpackedName = fmt.Sprintf("/home/wplapper/restic/.repositoryExports/%s-new", gopts.Repo)
	} else {
		Printf("no name given for export directory. Aborting!\n")
		return errors.New("no name for export directory. Aborting!")
	}

	CleanDirectory(repoUnpackedName)

	// step: build the directrory structure
	err = MakeDirectoryStructure(repoUnpackedName, true)
	if err != nil {
		return err
	}

	type procFunc func(context.Context, *repository.Repository, string) error
	var processList = []procFunc{
		ProcessSnapshots, //ProcessIndexFiles, ProcessMetaData, CreatePackList,
		//ConfigFile,
	}

	// run through the file lists: snapshots, index, metablobs, packlists
	for _, proc := range processList {
		err = proc(ctx, repo, repoUnpackedName)
		if err != nil {
			return err
		}
	}
	return nil
}

// build the directory structure for the export
func MakeDirectoryStructure(dirname string, toplevel bool) error {
	if fileInfo, err := os.Stat(dirname); err != nil {
		errx := os.MkdirAll(dirname, 0o755)
		if errx != nil {
			Printf("Cant create directory %s - reason %v\n", dirname, errx)
			return errx
		}
	} else if ! fileInfo.IsDir() {
		Printf("%s is NOT a directory, giving up!\n", dirname)
		return errors.New("NOT a directory, giving up!")
	}

	if ! toplevel {
		return nil
	}

	// toplelvel here
	err := MakeDirectoryStructure(dirname + "/metadata", false)
	if err != nil {
		Printf("error making sub directory metadata\n")
		return err
	}
	return nil
}

func ProcessSnapshots(ctx context.Context, repo *repository.Repository, repoUnpackedName string) error {
	type RawSnap struct{
		id restic.ID
		data []byte
	}

	snaps := []RawSnap{}
	// get snapshots list
	repo.List(ctx, restic.SnapshotFile, func(id restic.ID, size int64) error {
		buf, err := repo.LoadUnpacked(ctx, restic.SnapshotFile, id)
		if err != nil {
			Printf("LoadUnpacked.skip loading snap record %s! - reason: %v\n", id, err)
			return err
		}
		snaps = append(snaps, RawSnap{id, buf})
		return nil
	})

	// loop over all snapshots, write one file
	target := fmt.Sprintf("%s/all_snapshots", repoUnpackedName)
	Verboseff("target is %s\n", target)
	fd, _ := os.Create(target)
	handle := bufio.NewWriter(fd)
	for _, raw_snap := range snaps {
		handle.WriteString(fmt.Sprintf("%s,%s\n", raw_snap.id.String(),
			raw_snap.data))
	}

	handle.Flush()
	fd.Close()
	snaps = nil
	return nil
}

func ProcessIndexFiles(ctx context.Context, repo *repository.Repository, repoUnpackedName string) error {

	if err := repo.LoadIndex(ctx); err != nil {
		Printf("repo.LoadIndex - failed. Error is %v\n", err)
		return err
	}

	target := fmt.Sprintf("%s/all_index_info", repoUnpackedName)
	fd, _ := os.Create(target)
	handle := bufio.NewWriter(fd)
	for _, e := range CreateIndexInfo(ctx, repo) {
		handle.WriteString(fmt.Sprintf("%s,\"%s\",%d,%d,%d,%s\n", e.ID, e.Type,
			e.Offset, e.Length, e.UncompressedLength, e.PackID))
	}

	handle.Flush()
	fd.Close()
	return nil
}

type IndexHandleExpo struct {
	ID                  string
	Type                string
	Offset              uint
	Length              uint
	UncompressedLength  uint
	PackID              string
}

func CreateIndexInfo(ctx context.Context, repo *repository.Repository) (MetaBlobInfo []IndexHandleExpo) {
	MetaBlobInfo = []IndexHandleExpo{}
	repo.Index().Each(ctx, func(blob restic.PackedBlob) {
		MetaBlobInfo = append(MetaBlobInfo, IndexHandleExpo{
			blob.ID.String(),        blob.Type.String(),
			blob.Offset,             blob.Length,
			blob.UncompressedLength, blob.PackID.String(),
		})
	})

	sort.SliceStable(MetaBlobInfo, func(i,j int) bool {
		if MetaBlobInfo[i].PackID < MetaBlobInfo[j].PackID {
			return true
		} else if MetaBlobInfo[i].PackID > MetaBlobInfo[j].PackID {
			return false
		} else {
			return MetaBlobInfo[i].Offset < MetaBlobInfo[j].Offset
		}
	})
	return MetaBlobInfo
}

func ProcessMetaData(ctx context.Context, repo *repository.Repository, repoUnpackedName string) error {

	// load index
	if err := repo.LoadIndex(ctx); err != nil {
		Printf("repo.LoadIndex - failed. Error is %v\n", err)
		return err
	}

	// gather all metablobs
	metaBlobList := []restic.ID{}
	repo.Index().Each(ctx, func(blob restic.PackedBlob) {
		if blob.Type == restic.TreeBlob {
			metaBlobList = append(metaBlobList, blob.ID)
		}
	})

	// get all trees and write them to the metadata directory
	type MyTree struct {
		MetaBlob restic.ID `json:"metablob"`
		*restic.Tree
	}

	for _, meta_blob := range metaBlobList {
		tree, err := restic.LoadTree(ctx, repo, meta_blob)
		if err != nil {
			Printf("Can't load tree %s - reason %v\n", meta_blob.String()[:12])
			return err
		}

		filename := meta_blob.String()
		target := fmt.Sprintf("%s/metadata/%s/%s", repoUnpackedName, filename[:2], filename)
		subdir := fmt.Sprintf("%s/metadata/%s",    repoUnpackedName, filename[:2])
		_, err = os.Stat(subdir)
		if err != nil {
			err2 := os.Mkdir(subdir, 0o755)
			if err2 != nil {
				Printf("Can't create subdirectory %s - reason %v\n", subdir, err2)
				return err2
			}
		}
		Verboseff("target is %s\n", target)

		val, err := json.MarshalIndent(MyTree{MetaBlob: meta_blob, Tree: tree}, "", " ")
		if err != nil {
			Printf("json.MarshalIndent failed with %v\n", err)
			return err
		}
		val = append(val, '\n')
		err = os.WriteFile(target, val, 0o644)
		if err != nil {
			Printf("os.WriteFile failed with %v\n", err)
			return err
		}
	}
	metaBlobList = nil
	return nil
}

func CreatePackList(ctx context.Context, repo *repository.Repository, repoUnpackedName string) error {
	// packlists and sets
	packIDs  := map[restic.ID]string{}
	packList := []restic.ID{}

	repo.Index().Each(ctx, func(blob restic.PackedBlob) {
		if _, ok := packIDs[blob.PackID];  ! ok {
			packIDs[blob.PackID] = blob.Type.String()
			packList = append(packList, blob.PackID)
		}
	})

	// write one large packfile list
	target := fmt.Sprintf("%s/all_packfiles", repoUnpackedName)
	fd, _ := os.Create(target)
	handle := bufio.NewWriter(fd)
	for _, packID := range packList {
		handle.WriteString(fmt.Sprintf("%s,\"%s\"\n", packID.String(), packIDs[packID]))
	}
	handle.Flush()
	fd.Close()
	packIDs = nil
	packList = nil
	return nil
}

// gather config .ID from repo for verification purposes
func ConfigFile(ctx context.Context, repo *repository.Repository, repoUnpackedName string) error {

	// don't need ctx here

	config := repo.Config()
	target := fmt.Sprintf("%s/config", repoUnpackedName)
	fd, _ := os.Create(target)
	handle := bufio.NewWriter(fd)
	handle.WriteString(fmt.Sprintf("%s\n", config.ID))
	handle.Flush()
	fd.Close()
	return nil
}

func CleanDirectory(repoUnpackedName string) error {
	err := os.RemoveAll(repoUnpackedName)
	if err != nil {
		Printf("os.RemoveAll(%s) failed - reason '%v'\n", repoUnpackedName, err)
	}
	return err
}
