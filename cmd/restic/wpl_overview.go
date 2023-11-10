package main

// compile with "go run build.go -tags debug""
// run with DEBUG_LOG=/home/wplapper/restic/debug.log fully-qualified-name/restic -r <repo> overview

import (
	// system
	"context"
	"sort"
	"strings"
	"time"
	"errors"
	"os"
	"encoding/json"

	//argparse
	"github.com/spf13/cobra"

	// restic library
	"github.com/wplapper/restic/library/restic"

	// sets
	"github.com/deckarep/golang-set/v2"
)

type OverviewOptions struct {
	timing     bool
	memory_use bool
	Fullpath   bool
	Age        bool
	Hostname   string
	FileSystem string
	Cutoff     int
	MultiRepo  string
	ConfigFile string
}

var overview_options OverviewOptions

var cmdOverview = &cobra.Command{
	Use:   "wpl-overview [flags]",
	Short: "show summary of snapshots by Hostname and filesystems",
	Long: `show summary of snapshots by Hostname and filesystems.

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
	DisableAutoGenTag: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runOverview(cmd.Context(), cmd, globalOptions)
	},
}

func init() {
	cmdRoot.AddCommand(cmdOverview)
	f := cmdOverview.Flags()
	f.BoolVarP(&overview_options.timing, "timing", "T", false, "produce timings")
	f.BoolVarP(&overview_options.memory_use, "memory", "m", false, "produce memory usage")
	f.BoolVarP(&overview_options.Age, "age", "A", false, "show snapshots by age in days")
	f.BoolVarP(&overview_options.Fullpath, "fullpath", "U", false, "show duplicate paths")
	f.StringVarP(&overview_options.Hostname, "hostname", "H", "", "filter for Hostname")
	f.StringVarP(&overview_options.FileSystem, "file-system", "F", "", "filter for filesystem")
	f.IntVarP(&overview_options.Cutoff, "cutoff", "C", 250, "filter for cutoff days")
	f.StringVarP(&overview_options.MultiRepo, "multi-repo", "M", "", "base part of repositories local or onedrive")
	f.StringVarP(&overview_options.ConfigFile, "config-file", "O", "/home/wplapper/restic/backup_config.json", "json config file")
}

func runOverview(ctx context.Context, cmd *cobra.Command, gopts GlobalOptions) error {

	var (
		err            error
		repositoryData RepositoryData
	)

	globalOptions.Quiet = true
	globalOptions.verbosity = 0
	if overview_options.MultiRepo != "" {
		return doRunMultiRepoOverview(ctx, cmd, gopts, overview_options)
	}

	// startup
	gOptions = gopts
	init_repositoryData(&repositoryData)

	// step 1: open repository
	start := time.Now()
	repo, err := OpenRepository(ctx, gopts)
	if err != nil { return err }

	Verboseff("Repository is %s\n", globalOptions.Repo)
	if overview_options.timing {
		timeMessage(overview_options.memory_use, "%-30s %10.1f seconds\n", "open repo",
			time.Now().Sub(start).Seconds())
	}

	err = gather_base_data_repo(repo, gopts, ctx, &repositoryData, overview_options.timing, false)
	if err != nil { return err }

	if overview_options.Age {
		ShowAge(&repositoryData, overview_options)
		return nil
	}

	// step 3: build snap groups by Hostname and filesystem
	groups_sorted, groups := makeGroups(&repositoryData)
	group_summary := summarizeGroup(groups_sorted, groups, &repositoryData)
	// print group_Summary
	Printf("\n%-22s %-50s %-5s %11s %7s %7s %10s\n",
		"Hostname", "filesystem_path", "snaps", "directories", "files", "dblobs",
		"size[MiB]")
	Printf("%s\n", strings.Repeat("=", 118))

	for _, group := range groups_sorted {
		summary_data := group_summary[group]
		Hostname := group.Hostname
		FileSystem := group.FileSystem
		count_meta_blobs := summary_data.count_meta_blobs
		count_data_blobs := summary_data.count_data_blobs
		group_size := summary_data.sizes_blobs
		inodes := summary_data.count_inodes
		Printf("%-22s %-50s %5d %11d %7d %7d %10.1f\n",
			Hostname, FileSystem, len(groups[group]), count_meta_blobs,
			inodes,
			count_data_blobs, float64(group_size)/ONE_MEG)
	}

	// total counts and sizes for repository
	total_count_meta_blobs := 0
	total_count_data_blobs := 0
	total_count_snaps := len(repositoryData.Snaps)
	total_sizes := 0
	for _, ih := range repositoryData.IndexHandle {
		if ih.Type == restic.TreeBlob {
			total_count_meta_blobs++

		} else if ih.Type == restic.DataBlob {
			total_count_data_blobs++
		}
		total_sizes += ih.size
	}

	// count all inodes, don't cross filesystems
	inodeSet := mapset.NewThreadUnsafeSet[DeviceAndInode]()
	for _, file_list := range repositoryData.DirectoryMap {
		for _, meta := range file_list {
			if meta.Type == "file" {
				inodeSet.Add(DeviceAndInode{meta.DeviceID, meta.inode})
			}
		}
	}
	total_count_inodes := inodeSet.Cardinality()
	inodeSet = nil

	Printf("%s\n", strings.Repeat("=", 118))
	Printf("%-22s %-50s %5d %11d %7d %7d %10.1f\n",
		"repo total", "", total_count_snaps, total_count_meta_blobs,
		total_count_inodes,
		total_count_data_blobs, float64(total_sizes) / ONE_MEG)
	if overview_options.timing {
		timeMessage(overview_options.memory_use, "%-30s %10.1f seconds\n", "finale",
			time.Now().Sub(start).Seconds())
	}
	return nil
}

// print the age of filtered snaps
// The snaps are already sort by descending age.
func ShowAge(repositoryData *RepositoryData, options OverviewOptions) {
	filt_hostname := options.Hostname != ""
	filt_fsys     := options.FileSystem != ""
	hostname      := options.Hostname
	FileSystem    := options.FileSystem
	lfsys         := len(FileSystem)
	older         := options.Cutoff

	// remove trailing "/", if any
	if lfsys > 0 && FileSystem[lfsys-1:lfsys] == "/" {
		FileSystem = FileSystem[:lfsys-1]
	}

	now := time.Now()
	Printf("%-8s %-19s %-12s %s:%s\n", "snap_id", "   date and time", "", "hostname",
		"file-system")
	for _, sn := range repositoryData.Snaps {
		if filt_hostname && sn.Hostname != hostname {
			continue
		}
		if filt_fsys && sn.Paths[0] != FileSystem {
			continue
		}

		year, month, day := sn.Time.Date()
		// diff in days
		diff := int(now.Sub(time.Date(
			year, month, day, 0, 0, 0, 0, time.UTC)).Hours() / 24.0)
		if diff < older {
			continue
		}

		Printf("%s %s %3d days old %s:%s\n", sn.ID.Str(), sn.Time.Format(time.DateTime),
			diff, sn.Hostname, sn.Paths[0])
	}
}

func doRunMultiRepoOverview(ctx context.Context, cmd *cobra.Command, gopts GlobalOptions,
options OverviewOptions) error {
	if gopts.Repo != "" {
		return errors.New("repo specified as well!")
	}

	// read json from file
	var (
		err error
		json_config map[string]map[string]StringSlice
		ok bool
	)

	json_string, err := os.ReadFile(overview_options.ConfigFile)
	if err != nil {
		Printf("Cant read json config file '%s' - %v\n", overview_options.ConfigFile, err)
		return err
	}

	err = json.Unmarshal(json_string, &json_config)
	if err != nil {
		Printf("Cant decode config file %v\n", err)
		return err
	}

	// we need the target
	_, ok = json_config["BASE_CONFIG"][options.MultiRepo]
	if ! ok {
		Printf("Can't current deal with other base_parts %s\n", overview_options.MultiRepo)
		return errors.New("Can't currently deal with other multiple repositories")
	}
	target := json_config["BASE_CONFIG"][options.MultiRepo][0]

	// loop over all repositories
	repos := json_config["BASE_CONFIG"]["repos"]
	all_group_summary := map[snapGroup]GroupInfoSummary{}
	all_groups_sorted := []snapGroup{}
	for _, repo_basename := range repos {
		repository := target + repo_basename
		gopts.Repo = repository
		repo, err := OpenRepository(ctx, gopts)
		if err != nil {
			return err
		}

		var repositoryData RepositoryData
		init_repositoryData(&repositoryData)
		err = gather_base_data_repo(repo, gopts, ctx, &repositoryData,
			overview_options.timing, false)
		if err != nil {
			return err
		}
		repo.Close()

		// gather group info per repository
		groups_sorted, groups := makeGroups(&repositoryData)
		for group, group_detail := range summarizeGroup(groups_sorted, groups, &repositoryData) {
			all_group_summary[group] = group_detail
			all_groups_sorted = append(all_groups_sorted, group)
		}
	}

	// step 5: all groups sorted
	sort.Slice(all_groups_sorted, func(i, j int) bool {
		if all_groups_sorted[i].Hostname < all_groups_sorted[j].Hostname {
			return true
		} else if all_groups_sorted[i].Hostname > all_groups_sorted[j].Hostname {
			return false
		}
		return all_groups_sorted[i].FileSystem < all_groups_sorted[j].FileSystem
	})

	Printf("\n%-22s %-50s %-5s %11s %7s %7s %10s\n",
		"Hostname", "filesystem_path", "snaps", "directories", "files", "dblobs",
		"size[MiB]")
	Printf("%s\n", strings.Repeat("=", 118))

	total_count_meta_blobs := 0
	total_count_data_blobs := 0
	total_count_snaps := 0
	total_count_inodes := 0
	total_sizes := 0
	for _, group := range all_groups_sorted {
		summary_data := all_group_summary[group]
		Hostname := group.Hostname
		FileSystem := group.FileSystem
		count_meta_blobs := summary_data.count_meta_blobs
		count_data_blobs := summary_data.count_data_blobs
		group_size := summary_data.sizes_blobs
		inodes := summary_data.count_inodes
		count_snaps := summary_data.count_snapshots
		Printf("%-22s %-50s %5d %11d %7d %7d %10.1f\n",
			Hostname, FileSystem, count_snaps, count_meta_blobs,
			inodes, count_data_blobs, float64(group_size)/ONE_MEG)
		total_count_meta_blobs += count_meta_blobs
		total_count_data_blobs += count_data_blobs
		total_count_snaps      += count_snaps
		total_count_inodes     += inodes
		total_sizes            += group_size
	}

	Printf("%s\n", strings.Repeat("=", 118))
	Printf("%-22s %-50s %5d %11d %7d %7d %10.1f\n",
		"", "", total_count_snaps, total_count_meta_blobs,
		total_count_inodes,
		total_count_data_blobs, float64(total_sizes) / ONE_MEG)
	return nil
}
