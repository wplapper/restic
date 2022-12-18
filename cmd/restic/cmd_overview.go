package main

// compile with "go run build.go -tags debug""
// run with DEBUG_LOG=/home/wplapper/restic/debug.log fully-qualified-name/restic -r <repo> overview

// ref to onedrive: rclone:onedrive:restic_backups

import (
	// system
	"sort"
	"strings"
	"time"

	//argparse
	"github.com/spf13/cobra"

	// restic library
	//"github.com/deckarep/golang-set"
	//"github.com/wplapper/restic/library/mapset"
	"github.com/deckarep/golang-set/v2"
	"github.com/wplapper/restic/library/restic"
)

var cmdOverview = &cobra.Command{
	Use:   "overview [flags]",
	Short: "show summary of snapshots by host and filesystems",
	Long: `
show summary of snapshots by host and filesystems.

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
	DisableAutoGenTag: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runOverview(globalOptions)
	},
}

func init() {
	cmdRoot.AddCommand(cmdOverview)
}

func runOverview(gopts GlobalOptions) error {

	// step 1: open repository
	repositoryData := init_repositoryData()
	EMPTY_NODE_ID = restic.Hash([]byte("{\"nodes\":[]}\n"))
	gOptions = gopts

	start := time.Now()
	repo, err := OpenRepository(gopts)
	if err != nil {
		return err
	}
	if gopts.verbosity > 0 {
		Printf("%-30s in %10.1f seconds.\n", "open repository",
			time.Now().Sub(start).Seconds())
	}

	repositoryData.snaps, err = GatherAllSnapshots(gopts, repo)
	if err != nil {
		return err
	}

	// step 2.1: manage Index Records
	start = time.Now()
	if err = HandleIndexRecords(gopts, repo, repositoryData); err != nil {
		return err
	}

	start = time.Now()
	GatherAllRepoData(gopts, repo, repositoryData)
	timeMessage("%-30s %10.1f seconds\n", "GatherAllRepoData (sum)", time.Now().Sub(start).Seconds())

	// step 2.2 gather size info for each blob
	blobs_from_ix := make(map[restic.ID]uint)
	for blob := range repo.Index().Each(gopts.ctx) {
		blobs_from_ix[blob.ID] = blob.Length
	}
	timeMessage("%-30s in %10.1f seconds\n", "read all index records", time.Now().Sub(start).Seconds())

	// step 4: build snap groups by host and filesystem
	type snapGroup struct {
		host string
		fsys string
	}

	start = time.Now()
	groups := make(map[snapGroup][]*restic.Snapshot)
	for _, sn := range repositoryData.snaps {
		host := sn.Hostname
		fsys := sn.Paths[0]
		group := snapGroup{host: host, fsys: fsys}
		groups[group] = append(groups[group], sn)
	}

	// step 5: sort the groups according to host and filesystem
	groups_sorted := make([]snapGroup, len(groups))
	index := 0
	for group := range groups {
		groups_sorted[index] = group
		index++
	}

	sort.Slice(groups_sorted, func(i, j int) bool {
		if groups_sorted[i].host < groups_sorted[j].host {
			return true
		} else if groups_sorted[i].host > groups_sorted[j].host {
			return false
		}
		return groups_sorted[i].fsys < groups_sorted[j].fsys
	})

	// step 6: extract size information for these groups
	Printf("%-22s %-50s %-5s %11s %7s %7s %10s\n",
		"hostname", "filesystem_path", "snaps", "directories", "files", "dblobs",
		"size[MiB]")
	Printf("%s\n", strings.Repeat("=", 118))
	for _, group := range groups_sorted {
		host := group.host
		fsys := group.fsys

		// step 8: gather blobs for the constructed tree lists
		// get the data from our repository_data structures
		usedIntBlobs := restic.NewIntSet()
		count_file_sets := mapset.NewSet[uint64]()
		for _, sn := range groups[group] {
			// step trough the list of meta_blobs and collect data
			id_ptr := Ptr2ID(*sn.ID(), repositoryData)
			usedIntBlobs.Merge(repositoryData.meta_dir_map[id_ptr])
			for meta_blob := range repositoryData.meta_dir_map[id_ptr] {
				for _, meta := range repositoryData.directory_map[meta_blob] {
					if meta.Type == "file" {
						for _, cont := range meta.content {
							usedIntBlobs.Insert(cont)
						}
						count_file_sets.Add(meta.inode)
					}
				}
			}
		}

		// step 9: access size information on used blobs
		count_data_blobs := 0
		count_meta_blobs := 0
		group_size := uint64(0)
		for int_blob := range usedIntBlobs {
			blobID := repositoryData.index_to_blob[int_blob]
			ih := repositoryData.index_handle[blobID]
			if ih.Type == restic.DataBlob {
				group_size += uint64(blobs_from_ix[blobID])
				count_data_blobs++
			} else if ih.Type == restic.TreeBlob {
				count_meta_blobs++
			}
		}
		Printf("%-22s %-50s %5d %11d %7d %7d %10.1f\n",
			host, fsys, len(groups[group]), count_meta_blobs, count_file_sets.Cardinality(),
			count_data_blobs, float64(group_size)/1024.0/1024.0)
	}

	// *** ALL ***
	usedIntBlobs := restic.NewIntSet()
	count_file_sets := mapset.NewSet[uint64]()
	for _, sn := range repositoryData.snaps {
		// step trough the list of meta_blobs and collect data
		id_ptr := Ptr2ID(*sn.ID(), repositoryData)
		usedIntBlobs.Merge(repositoryData.meta_dir_map[id_ptr])
		for meta_blob := range repositoryData.meta_dir_map[id_ptr] {
			for _, meta := range repositoryData.directory_map[meta_blob] {
				if meta.Type == "file" {
					for _, cont := range meta.content {
						usedIntBlobs.Insert(cont)
					}
					count_file_sets.Add(meta.inode)
				}
			}
		}
	}

	// step 9: access size information on ALL blobs
	count_data_blobs := 0
	count_meta_blobs := 0
	size_repo := uint64(0)
	for int_blob := range usedIntBlobs {
		blobID := repositoryData.index_to_blob[int_blob]
		ih := repositoryData.index_handle[blobID]
		if ih.Type == restic.DataBlob {
			size_repo += uint64(blobs_from_ix[blobID])
			count_data_blobs++
		} else if ih.Type == restic.TreeBlob {
			count_meta_blobs++
		}
	}
	Printf("%s\n", strings.Repeat("=", 118))
	Printf("%-22s %-50s %5d %11d %7d %7d %10.1f\n", "summary", "", len(repositoryData.snaps),
		count_meta_blobs, count_file_sets.Cardinality(), count_data_blobs,
		float64(size_repo)/1024.0/1024.0)

	timeMessage("%-30s in %10.1f seconds\n", "gather data for groups", time.Now().Sub(start).Seconds())
	return nil
}
