package main

import (
  "context"
  //"math"
  "sort"
  //"strconv"
  //"strings"
	//"golang.org/x/sync/errgroup"

  //"github.com/wplapper/restic/library/errors"
  //"github.com/wplapper/restic/library/index"
  //"github.com/wplapper/restic/library/pack"
  "github.com/wplapper/restic/library/repository"
  "github.com/wplapper/restic/library/restic"

  "github.com/spf13/cobra"

	// sets
	"github.com/deckarep/golang-set/v2"
)

type ReorgOptions struct {
  DryRun        bool
  PercentCutoff int
  Detail        int

  UnsafeNoSpaceRecovery string

  unsafeRecovery bool

  MaxUnused      string
  maxUnusedBytes func(used uint64) (unused uint64) // calculates the number of unused bytes after repacking, according to MaxUnused

  MaxRepackSize  string
  MaxRepackBytes uint64

  RepackCachableOnly bool
  RepackSmall        bool
  RepackUncompressed bool
}
var reorgOptions ReorgOptions

var cmdWplPrune = &cobra.Command{
  Use:   "reorg [flags]",
  Short: "reorganise the repository",
  Long: `
The "reorg" command checks the repository and reorganises packflies in a more
usable order.

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
  DisableAutoGenTag: true,
  RunE: func(cmd *cobra.Command, args []string) error {
    return runWplPrune(cmd.Context(), reorgOptions, globalOptions)
  },
}

func init() {
  cmdRoot.AddCommand(cmdWplPrune)
  f := cmdWplPrune.Flags()
  f.StringVar(&reorgOptions.MaxUnused, "max-unused", "5%", "tolerate given `limit` of unused data (absolute value in bytes with suffixes k/K, m/M, g/G, t/T, a value in % or the word 'unlimited')")
  f.BoolVarP(&reorgOptions.DryRun, "dry-run", "n", false, "do not modify the repository, just print what would be done")
  f.StringVar(&reorgOptions.MaxRepackSize, "max-repack-size", "", "maximum `size` to repack (allowed suffixes: k/K, m/M, g/G, t/T)")
  f.BoolVar(&reorgOptions.RepackCachableOnly, "repack-cacheable-only", false, "only repack packs which are cacheable")
  f.BoolVar(&reorgOptions.RepackSmall, "repack-small", false, "repack pack files below 80% of target pack size")
  f.BoolVar(&reorgOptions.RepackUncompressed, "repack-uncompressed", false, "repack all uncompressed data")

  f.CountVarP(&reorgOptions.Detail, "detail", "D", "reorganisation details")
	f.IntVarP(&reorgOptions.PercentCutoff, "pcutoff", "C", 10, "reorg everything with less than <x>% contribution to a pack")
}

func runWplPrune(ctx context.Context, opts ReorgOptions, gopts GlobalOptions) error {

  repo, err := OpenRepository(ctx, gopts)
  if err != nil {
    return err
  }
  Verbosef("Repository opened.\n")

	var repositoryData RepositoryData
  init_repositoryData(&repositoryData)
	err = gather_base_data_repo(repo, gopts, ctx, &repositoryData, false)
	if err != nil {
		return err
	}
  Verbosef("gather_base_data_repo done.\n")

  return planWplPrune(ctx, opts, gopts, repo, &repositoryData)
}

type PackSizeInfo struct {
  sizes struct {
    used       int
  }
  count struct {
    used       int
  }
}



// planPrune selects which files to rewrite and which to delete and which blobs to keep.
// Also some summary statistics are returned.
func planWplPrune(ctx context.Context, opts ReorgOptions, gopts GlobalOptions,
repo *repository.Repository, repositoryData *RepositoryData) error {

  // get group info
  groupInfo := MakeSnapGroups(ctx, repo, repositoryData)

  // get a set of snap_ids per group
  group_sets := make(map[snapGroup]mapset.Set[string])
  for group, snap_slice := range groupInfo.snap_groups {
    group_sets[group] = mapset.NewSet[string]()
    for _, sn := range snap_slice {
      group_sets[group].Add(sn.ID().Str())
    }
  }

	// gather all blobs
  repositoryData.all_blobs = mapset.NewSet[IntID]()
	for _, ih := range repositoryData.index_handle {
		repositoryData.all_blobs.Add(ih.blob_index)
	}

	// collect all blobs which belong to the same pack
	//repositoryData.blobs_per_packID = make_blobs_per_packID(repositoryData)

  // get a full map of all data blobs
  full_contents_map := make_full_contents_map_v3(repositoryData)
  count_entries := 0
  for _, data_sett := range full_contents_map {
    count_entries += data_sett.Cardinality()
  }
  Printf("full_contents_map %8d entries\n", len(full_contents_map))
  Printf("full_contents_map %8d variations\n", count_entries)

  // print group info
  Printf("\n*** groups ***\n")
  for ix, group := range groupInfo.group_keys {
    Printf("group number %2d %v\n", ix, group)
  }

  Printf("\n*** Group info ***\n")
  Printf("%-40s %7s %14s         %7s %14s\n", "group", "count", "size [MiB]", "out-cnt", "out-sz [SMiB]")
  for _, group := range groupInfo.group_keys {
    used_blobs := mapset.NewSet[IntID]()
    group_slice := groupInfo.snap_groups[group]
    for _, sn := range group_slice {
      snap_id := sn.ID().Str()
      id_ptr := Ptr2ID(*(repositoryData.snap_map[snap_id]).ID(), repositoryData)
      for meta_blob := range repositoryData.meta_dir_map[id_ptr].Iter() {
        used_blobs.Add(meta_blob)
        for _, meta := range repositoryData.directory_map[meta_blob] {
          used_blobs.Append(meta.content...)
        }
      }
    }
    ClassifyBlobs(group, used_blobs, repositoryData, full_contents_map, group_sets[group], groupInfo)
  }
  return nil
}

func ClassifyBlobs(group snapGroup, blobs mapset.Set[IntID], repositoryData *RepositoryData,
full_contents_map map[IntID]mapset.Set[FullSet], group_members mapset.Set[string],
groupInfo GroupInfo) {
  count := 0
  sizes := 0
  seen  := mapset.NewSet[IntID]()
  // find data blobs which live inside this group
  for blob := range blobs.Iter() {
    count++
    sizes += repositoryData.index_handle[repositoryData.index_to_blob[blob]].size
  }
  group_name := group.Hostname + ":" + group.FileSystem
  if len(group_name) > 40 {
    group_name = group_name[:40]
  }

  // find data blobs which live outside this group
  outside_count := make(map[int]int)
  outsize_sizes := make(map[int]int)
  for _, ix := range groupInfo.group_numbers_sorted {
    outside_count[ix] = 0
    outsize_sizes[ix] = 0
  }

  outside_count_once := 0
  outside_sizes_once := 0
  for blob := range blobs.Iter() {
    data_sett, ok := full_contents_map[blob]
    if ! ok { continue }
    for cpi := range data_sett.Iter() {
      snap_id := cpi.snap_id
      if group_members.Contains(snap_id) { continue } // inside the group

      snap_ix := groupInfo.map_snap_2_ix[snap_id]
      if ! seen.Contains(blob) {
        // catching the count and size once is an arbitrary choice for the group!
        size := repositoryData.index_handle[repositoryData.index_to_blob[blob]].size
        outside_count[snap_ix]++
        outsize_sizes[snap_ix] += size
        outside_sizes_once += size
        outside_count_once++
        seen.Add(blob)
      }
    }
  }

  Printf("%-40s %7d %10.1f MiB outside %7d %10.1f MiB\n", group_name,
    count, float64(sizes) / ONE_MEG, outside_count_once, float64(outside_sizes_once) / ONE_MEG)
  for _, ix := range groupInfo.group_numbers_sorted {
    if _, ok := outside_count[ix]; ! ok { continue }
    count = outside_count[ix]
    if count == 0 { continue }
    sizes := outsize_sizes[ix]
    group := groupInfo.group_keys[ix]
    name := group.Hostname + ":" + group.FileSystem
    if len(name) > 36 {
      name = name[:36]
    }
    Printf("    %-36s %7d %10.1f MiB\n", name, count, float64(sizes) / ONE_MEG)
  }
}

type GroupInfo struct {
  snap_groups          map[snapGroup][]*restic.Snapshot
  group_numbers_sorted []int
  group_keys           []snapGroup
  group_numbers        map[snapGroup]int
  map_snap_2_ix        map[string]int
}

func MakeSnapGroups(ctx context.Context, repo *repository.Repository,
repositoryData *RepositoryData) (groupInfo GroupInfo) {
  // generate groups based on hostname and filesystems
  groupInfo.snap_groups = make(map[snapGroup][]*restic.Snapshot)
	repo.List(ctx, restic.SnapshotFile, func(id restic.ID, size int64) error {
		sn, err := restic.LoadSnapshot(ctx, repo, id)
		if err != nil {
			Printf("Skip loading snap record %s! - reason: %v\n", id, err)
			return err
		}

		hostname := sn.Hostname
    for _, path := range sn.Paths {
      group := snapGroup{Hostname: hostname, FileSystem: path}
      groupInfo.snap_groups[group] = append(groupInfo.snap_groups[group], sn)
    }
		return nil
	})

  // transform groups in such a way that a group index can be used
  // sort snap_group keys according to Hostname and FileSystem
  groupInfo.group_keys = make([]snapGroup, 0, len(groupInfo.snap_groups))
  for key := range groupInfo.snap_groups {
    groupInfo.group_keys = append(groupInfo.group_keys, key)
  }
  sort.Slice(groupInfo.group_keys, func (i, j int) bool {
    if groupInfo.group_keys[i].Hostname < groupInfo.group_keys[j].Hostname {
      return true
    } else if groupInfo.group_keys[i].Hostname > groupInfo.group_keys[j].Hostname {
      return false
    } else {
      return groupInfo.group_keys[i].FileSystem < groupInfo.group_keys[j].FileSystem
    }
  })

  // enumerate group_keys for future reference as group number
  groupInfo.group_numbers = make(map[snapGroup]int)
  for ix, key := range groupInfo.group_keys {
    groupInfo.group_numbers[key] = ix
  }

  groupInfo.group_numbers_sorted = make([]int, 0, len(groupInfo.group_keys))
  for _, ix := range groupInfo.group_numbers {
    groupInfo.group_numbers_sorted = append(groupInfo.group_numbers_sorted, ix)
  }
  sort.Ints(groupInfo.group_numbers_sorted)

  // map the group members == snaps back to the group-ID
  groupInfo.map_snap_2_ix = make(map[string]int)
  for group, group_slice := range groupInfo.snap_groups {
    group_index := groupInfo.group_numbers[group]
    for _, sn := range group_slice {
      groupInfo.map_snap_2_ix[sn.ID().Str()] = group_index
    }
  }
  return groupInfo
}

type FullSet struct {
	// the following triple maps a data blob
	data_blob_int IntID
	meta_blob_int IntID     // unique, part1
	//position      int       // unique, part2
	//offset        int       // unique, part3
 	//name          string
  snap_id       string
}

// go through all contents and generate unique triples &
// additional string info for sorting
func make_full_contents_map_v3(repositoryData *RepositoryData) (data_map map[IntID]mapset.Set[FullSet]) {
  // is a map[IntID]mapset.Set[string]
  //meta_dir_map_reverse := make_meta_dir_map_reverse(repositoryData)
	data_map = make(map[IntID]mapset.Set[FullSet])
  for ID, meta_blob_sett := range repositoryData.meta_dir_map {
    snap_id := ID.String()[:8]
    for meta_blob_int := range meta_blob_sett.Iter() {
      for _, meta := range repositoryData.directory_map[meta_blob_int] {
        for _, data_blob_int := range meta.content {
          // this data_blob can appear multiple times in different meta_blobs
          cmp_ix := FullSet{
            meta_blob_int: meta_blob_int,
            data_blob_int: data_blob_int,
            snap_id: snap_id,
          }
          if _, ok := data_map[data_blob_int]; ! ok {
            data_map[data_blob_int] = mapset.NewSet[FullSet]()
          }
          data_map[data_blob_int].Add(cmp_ix)
        }
      }
    }
	}
	return data_map
}
