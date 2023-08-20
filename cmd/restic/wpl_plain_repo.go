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

var cmdPlainRepo = &cobra.Command{
	Use:   "wpl-export [flags]",
	Short: "export metadata of repo to plaintext directory structure",
	Long: `export metadata of repo to plaintext directory structure.

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
	DisableAutoGenTag: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runPlainRepo(cmd.Context(), cmd, globalOptions)
	},
}

type PlainRepoOptions struct {
	Repo string
}
var plainRepoOptions PlainRepoOptions

func init() {
	cmdRoot.AddCommand(cmdPlainRepo)
	f := cmdPlainRepo.Flags()
	f.StringVarP(&plainRepoOptions.Repo, "alt-repo", "A", "", "repository name")
}

// convert all the metadata into plain files with a directory strcuture like
// a repository
func runPlainRepo(ctx context.Context, cmd *cobra.Command, gopts GlobalOptions) error {
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
	Verboseff("Repository is %s\n", globalOptions.Repo)

	if globalOptions.Repo[:1] != "/" && plainRepoOptions.Repo == "" {
		return errors.New("Need a local filesystem directory to map repo!")
	}

	// check for alternative repo name
	if plainRepoOptions.Repo != "" {
		repoUnpackedName = plainRepoOptions.Repo
	} else {
		lenRepoName := len(globalOptions.Repo)
		if globalOptions.Repo[lenRepoName-1 : lenRepoName] == "/" {
			globalOptions.Repo = globalOptions.Repo[:lenRepoName-1]
		}
		repoUnpackedName = globalOptions.Repo + "_unpacked"
	}

	// step: build the directrory structure
	err = MakeDirectoryStructure(repoUnpackedName, true)
	if err != nil {
		return err
	}

	type procFunc func(GlobalOptions, context.Context, *repository.Repository, string) error
	var processList = []procFunc{
		ProcessSnapshots, ProcessIndexFiles, ProcessMetaData, CreatePackList,
	}

	// run through the file lists: snapshots, index, metablobs, packlists
	for _, proc := range processList {
		err = proc(gopts, ctx, repo, repoUnpackedName)
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

func ProcessSnapshots(gopts GlobalOptions, ctx context.Context,
repo *repository.Repository, repoUnpackedName string) error {
	// get snapshots list
	snaps, _, err := GatherAllSnapshots(gopts, ctx, repo)
	if err != nil {
		return err
	}

	// loop over all snapshots, write one file
	target := fmt.Sprintf("%s/all_snapshots", repoUnpackedName)
	Verboseff("target is %s\n", target)
	fd, _ := os.Create(target)
	handle := bufio.NewWriter(fd)
	for _, sn := range snaps {
		handle.WriteString(fmt.Sprintf("%s,%s,%s,%s,%s\n", sn.ID().String(),
			sn.Tree.String(), sn.Hostname, sn.Paths[0], sn.Time.String()[:19]))
	}

	handle.Flush()
	fd.Close()
	return nil
}

func ProcessIndexFiles(gopts GlobalOptions, ctx context.Context,
repo *repository.Repository, repoUnpackedName string) error {

	if err := repo.LoadIndex(ctx); err != nil {
		Printf("repo.LoadIndex - failed. Error is %v\n", err)
		return err
	}

	target := fmt.Sprintf("%s/all_index_info", repoUnpackedName)
	fd, _ := os.Create(target)
	handle := bufio.NewWriter(fd)
	for _, e := range CreateIndexInfo(gopts, ctx, repo) {
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

func CreateIndexInfo(gopts GlobalOptions, ctx context.Context,
repo *repository.Repository) ([]IndexHandleExpo) {
	MetaBlobInfo := []IndexHandleExpo{}
	repo.Index().Each(ctx, func(blob restic.PackedBlob) {
		MetaBlobInfo = append(MetaBlobInfo, IndexHandleExpo{
			ID: blob.ID.String(),
			Type: blob.Type.String(),
			Offset: blob.Offset,
			Length: blob.Length,
			UncompressedLength: blob.UncompressedLength,
			PackID: blob.PackID.String(),
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

func ProcessMetaData(gopts GlobalOptions, ctx context.Context,
repo *repository.Repository, repoUnpackedName string) error {

	// load index
	if err := repo.LoadIndex(ctx); err != nil {
		Printf("repo.LoadIndex - failed. Error is %v\n", err)
		return err
	}

	// gather all metablobs
	metaBlobList  := []restic.ID{}
	repo.Index().Each(ctx, func(blob restic.PackedBlob) {
		if blob.Type == restic.TreeBlob {
			metaBlobList = append(metaBlobList, blob.ID)
		}
	})

	// get all trees and write them to the metadat directory
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
		subdir := fmt.Sprintf("%s/metadata/%s", repoUnpackedName, filename[:2])
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
	return nil
}

func CreatePackList(gopts GlobalOptions, ctx context.Context,
repo *repository.Repository, repoUnpackedName string) error {
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
	//handle.WriteString("PackID\n")
	for _, packID := range packList {
		handle.WriteString(fmt.Sprintf("%s,\"%s\"\n", packID.String(), packIDs[packID]))
	}
	handle.Flush()
	fd.Close()
	return nil
}
