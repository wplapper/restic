package main


import (
	"sort"
	"time"
	"context"

	// restic library
	"github.com/wplapper/restic/library/restic"
	"github.com/wplapper/restic/library/repository"

	// sets
	"github.com/deckarep/golang-set/v2"
)

type snapGroup struct {
	Hostname   string
	FileSystem string
}

type GroupInfoSummary struct {
	count_snapshots  int
	count_meta_blobs int
	count_data_blobs int
	sizes_blobs      int
	count_inodes     int
}

func makeGroups(repositoryData *RepositoryData) (groups_sorted []snapGroup,
groups map[snapGroup][]SnapshotWpl) {
	// step: build snap groups by Hostname and filesystem
	groups = make(map[snapGroup][]SnapshotWpl)
	for _, sn := range repositoryData.Snaps {
		hostname := sn.Hostname
		fileSystem := sn.Paths[0]
		group := snapGroup{Hostname: hostname, FileSystem: fileSystem}
		groups[group] = append(groups[group], sn)
	}

	// step 4: sort the groups according to Hostname and filesystem
	groups_sorted = make([]snapGroup, len(groups))
	index := 0
	for group := range groups {
		groups_sorted[index] = group
		index++
	}

	// step 5: custom sort for 'groups_sorted' by Hostname and filesystem
	sort.Slice(groups_sorted, func(i, j int) bool {
		if groups_sorted[i].Hostname < groups_sorted[j].Hostname {
			return true
		} else if groups_sorted[i].Hostname > groups_sorted[j].Hostname {
			return false
		}
		return groups_sorted[i].FileSystem < groups_sorted[j].FileSystem
	})
	return groups_sorted, groups
}

func summarizeGroup(groups_sorted []snapGroup, groups map[snapGroup][]SnapshotWpl,
repositoryData *RepositoryData) (groupResults map[snapGroup]GroupInfoSummary) {
	// extract size / count information frogroups m these groups
	timings := make(map[snapGroup]float64)
	groupResults = make(map[snapGroup]GroupInfoSummary)
	for _, group := range groups_sorted {
		start_g := time.Now()
		data_blobs_in_group := mapset.NewThreadUnsafeSet[IntID]()
		inodes_in_group := mapset.NewThreadUnsafeSet[uint64]()
		for _, sn := range groups[group] {
			// step trough the list of meta_blobs and collect data
			data_blobs_in_group = data_blobs_in_group.Union(repositoryData.MetaDirMap[sn.ID])
			for meta_blob := range repositoryData.MetaDirMap[sn.ID].Iter() {
				for _, meta := range repositoryData.DirectoryMap[meta_blob] {
					if meta.Type == "file" {
						inodes_in_group.Add(meta.inode)
						data_blobs_in_group.Append(meta.content...)
					}
				}
			}
		}

		// access size information on used blobs
		count_data_blobs := 0
		count_meta_blobs := 0
		group_size := 0
		for int_blob := range data_blobs_in_group.Iter() {
			ih := repositoryData.IndexHandle[repositoryData.IndexToBlob[int_blob]]
			group_size += ih.size
			if ih.Type == restic.DataBlob {
				count_data_blobs++
			} else if ih.Type == restic.TreeBlob {
				count_meta_blobs++
			}
		}
		res := GroupInfoSummary{
			count_snapshots: len(groups[group]),
			count_meta_blobs: count_meta_blobs,
			count_data_blobs: count_data_blobs,
			sizes_blobs: group_size,
			count_inodes: inodes_in_group.Cardinality(),
		}
		groupResults[group] = res
		timings[group] = time.Now().Sub(start_g).Seconds()
	}
	return groupResults
}

func MakeSnapGroups(ctx context.Context, repo *repository.Repository,
repositoryData *RepositoryData) (groupInfo GroupInfo) {
  // generate groups based on hostname and filesystems
  groupInfo.snap_groups = map[snapGroup][]SnapshotWpl{}
	repo.List(ctx, restic.SnapshotFile, func(id restic.ID, size int64) error {
		sn, err := restic.LoadSnapshot(ctx, repo, id)
		if err != nil {
			Printf("Skip loading snap record %s! - reason: %v\n", id, err)
			return err
		}

		hostname := sn.Hostname
    for _, path := range sn.Paths {
      group := snapGroup{Hostname: hostname, FileSystem: path}
      groupInfo.snap_groups[group] = append(groupInfo.snap_groups[group], SnapshotWpl{
				ID: *sn.ID(), Hostname: sn.Hostname, Paths: sn.Paths, Time: sn.Time,
				Tree: *sn.Tree,
			})
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
  groupInfo.map_snap_2_ix = map[string]int{}
  for group, group_slice := range groupInfo.snap_groups {
    group_index := groupInfo.group_numbers[group]
    for _, sn := range group_slice {
      groupInfo.map_snap_2_ix[sn.ID.Str()] = group_index
    }
  }
  return groupInfo
}
