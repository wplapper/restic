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
	"github.com/wplapper/restic/library/restic"

	// sets
	"github.com/deckarep/golang-set/v2"
)

type snapGroup struct {
	host string
	fsys string
}

var cmdOverview = &cobra.Command{
	Use:   "overview [flags]",
	Short: "show summary of snapshots by host and filesystems",
	Long: `show summary of snapshots by host and filesystems.

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
	flags := cmdOverview.Flags()
	flags.BoolVarP(&dbOptions.timing, "timing", "T", false, "produce timings")
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
	if dbOptions.timing {
		Printf("%-30s %10.1f seconds.\n", "open repository",
			time.Now().Sub(start).Seconds())
	}

	repositoryData.snaps, err = GatherAllSnapshots(gopts, repo)
	if err != nil {
		return err
	}
	if dbOptions.timing {
		timeMessage("%-30s %10.1f seconds\n", "gather snapshots",
			time.Now().Sub(start).Seconds())
	}

	// step 2.1: manage Index Records
	//start = time.Now()
	if err = HandleIndexRecords(gopts, repo, repositoryData); err != nil {
		return err
	}
	if dbOptions.timing {
		timeMessage("%-30s %10.1f seconds\n", "read index records",
			time.Now().Sub(start).Seconds())
	}

	GatherAllRepoData(gopts, repo, repositoryData)
	if dbOptions.timing {
		timeMessage("%-30s %10.1f seconds\n", "GatherAllRepoData",
			time.Now().Sub(start).Seconds())
	}

	// step 4: build snap groups by host and filesystem
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
		data_blobs_in_group := restic.NewIntSet()
		inodes_in_group := mapset.NewSet[uint64]()
		for _, sn := range groups[group] {
			// step trough the list of meta_blobs and collect data
			id_ptr := Ptr2ID(*sn.ID(), repositoryData)
			data_blobs_in_group.Merge(repositoryData.meta_dir_map[id_ptr])
			for meta_blob := range repositoryData.meta_dir_map[id_ptr] {
				for _, meta := range repositoryData.directory_map[meta_blob] {
					if meta.Type == "file" {
						inodes_in_group.Add(meta.inode)
						for _, cont := range meta.content {
							data_blobs_in_group.Insert(cont)
						}
					}
				}
			}
		}

		// step 9: access size information on used blobs
		count_data_blobs := 0
		count_meta_blobs := 0
		group_size := 0
		for int_blob := range data_blobs_in_group {
			ih := repositoryData.index_handle[repositoryData.index_to_blob[int_blob]]
			if ih.Type == restic.DataBlob {
				group_size += ih.size
				count_data_blobs++
			} else if ih.Type == restic.TreeBlob {
				count_meta_blobs++
			}
		}
		Printf("%-22s %-50s %5d %11d %7d %7d %10.1f\n",
			host, fsys, len(groups[group]), count_meta_blobs, inodes_in_group.Cardinality(),
			count_data_blobs, float64(group_size) / ONE_MEG)
		inodes_in_group = nil
		data_blobs_in_group = nil
	}

	// *** ALL ***
	all_blobs := restic.NewIntSet()
	all_inodes_repo := mapset.NewSet[uint64]()
	for _, sn := range repositoryData.snaps {
		id_ptr := Ptr2ID(*sn.ID(), repositoryData)
		all_blobs.Merge(repositoryData.meta_dir_map[id_ptr])
		for meta_blob := range repositoryData.meta_dir_map[id_ptr] {
			for _, meta := range repositoryData.directory_map[meta_blob] {
				if meta.Type == "file" {
					all_inodes_repo.Add(meta.inode)
					for _, cont := range meta.content {
						all_blobs.Insert(cont)
					}
				}
			}
		}
	}

	// step 9: access size information on ALL blobs
	count_data_blobs := 0
	count_meta_blobs := 0
	size_repo := 0
	for int_blob := range all_blobs {
		ih := repositoryData.index_handle[repositoryData.index_to_blob[int_blob]]
		if ih.Type == restic.DataBlob {
			size_repo += ih.size
			count_data_blobs++
		} else if ih.Type == restic.TreeBlob {
			count_meta_blobs++
		}
	}
	Printf("%s\n", strings.Repeat("=", 118))
	Printf("%-22s %-50s %5d %11d %7d %7d %10.1f\n", "summary", "", len(repositoryData.snaps),
		count_meta_blobs, all_inodes_repo.Cardinality(), count_data_blobs,
		float64(size_repo) / ONE_MEG)

	if dbOptions.timing {
		timeMessage("%-30s %10.1f seconds\n", "gather data for groups",
			time.Now().Sub(start).Seconds())
	}
	all_blobs = nil
	all_inodes_repo = nil
	return nil
}
