package main

import (
	// system
	"fmt"
	"sort"
	"time"

	// restic library
	"github.com/wplapper/restic/library/restic"
)

// example: list the directory names of the first snapshot
func service1 (repositoryData *RepositoryData) {
	sn := repositoryData.snaps[0]
	directory_names := make([]string, 0, len(repositoryData.fullpath))
	for meta_blob := range repositoryData.meta_dir_map[*sn.ID()] {
		directory_names = append(directory_names, repositoryData.fullpath[meta_blob])
	}

	// sort alphabetically
	sort.SliceStable(directory_names, func(i, j int) bool {
		return directory_names[i] < directory_names[j] })

	for _, name := range directory_names {
		Printf("%s\n", name)
	}
}

type dirname_and_blob struct {
	dir_name string
	meta_blob restic.IntID
}
// example: list the directory names and the names of the files
func service3 (repositoryData *RepositoryData) {
	sn := repositoryData.snaps[0]
	Printf("snap_ID %s = %s:%s at %s\n", sn.ID().Str(),
		sn.Hostname, sn.Paths[0], sn.Time.String()[:19])

	directory_names := make([]dirname_and_blob, 0, len(repositoryData.fullpath))
	Printf("fullpath has  %d entries\n", len(repositoryData.fullpath))
	Printf("this snap has %d entries\n", len(repositoryData.meta_dir_map[*sn.ID()]))
	for meta_blob := range repositoryData.meta_dir_map[*sn.ID()] {
		if meta_blob == EMPTY_NODE_ID_TRANSLATED {
			continue
		}
		if repositoryData.fullpath[meta_blob] == "" {
			continue
		}
		directory_names = append(directory_names, dirname_and_blob{
			dir_name: repositoryData.fullpath[meta_blob], meta_blob: meta_blob})
	}

	// sort dir_name alphabetically
	sort.SliceStable(directory_names, func(i, j int) bool {
		return directory_names[i].dir_name < directory_names[j].dir_name})

	Printf("*** list of all files and directories for above snapshot ***\n")
	for _, d_and_b := range directory_names {
		meta_blob := d_and_b.meta_blob
		/*if d_and_b.dir_name == "" {
			continue
		}*/
		Printf("%s\n", d_and_b.dir_name)
		for _, meta := range repositoryData.directory_map[meta_blob] {
			name := meta.name
			size := fmt.Sprintf("%11d", meta.size)
			if meta.Type == "dir" {
				name = name + "/"
				size = ""
			}
			Printf("%11s %s\n", size, name)
		}
	}
}

// example2: calculate wich blobs are unique to first (or second) snapshot
func service2 (repositoryData *RepositoryData) {
	Printf("\n*** service 2 ***\n")
	sn1 := repositoryData.snaps[0]
	Printf("snap_ID %s = %s:%s at %s\n", sn1.ID().Str(),
		sn1.Hostname, sn1.Paths[0], sn1.Time.String()[:19])
	start := time.Now()

	// reference to all meta_blobs, we need a spanking new InstSet for merging!
	collect_blobs := restic.NewIntSet()
	collect_blobs.Merge(repositoryData.meta_dir_map[*sn1.ID()])
	for meta_blob := range repositoryData.meta_dir_map[*sn1.ID()] {
		for _, meta := range repositoryData.directory_map[meta_blob] {
			if meta.Type == "file" {
				collect_blobs.Merge(meta.content)
			}
		}
	}

	// find the snaps for all other snapshots
	all_other_snaps := make([]*restic.Snapshot, 0, len(repositoryData.snaps))
	for _, sn  := range repositoryData.snaps {
		if sn == sn1 {
			continue
		}
		all_other_snaps = append(all_other_snaps, sn)
	}

	// loop over all_other_snaps
	all_other_blobs := restic.NewIntSet()
	for _, sn := range all_other_snaps {
		all_other_blobs.Merge(repositoryData.meta_dir_map[*sn.ID()])
		for meta_blob := range repositoryData.meta_dir_map[*sn.ID()] {
			for _, meta := range repositoryData.directory_map[meta_blob] {
				if meta.Type == "file" {
					all_other_blobs.Merge(meta.content)
				}
			}
		}
	}
	timeMessage("%-30s %10.1f seconds\n", "collect unique blobs", time.Now().Sub(start).Seconds())

	// output section: the difference is unique to sn1
	difference := collect_blobs.Sub(all_other_blobs)

	// find their sizes
	size_meta  := uint64(0)
	size_data  := uint64(0)
	count_meta := 0
	count_data := 0
	for blob := range difference {
		data  := repositoryData.index_handle[repositoryData.index_to_blob[blob]]
		size  := uint64(data.size)
		if data.Type == restic.TreeBlob {
				count_meta++
				size_meta += size
		} else if data.Type == restic.DataBlob {
				count_data++
				size_data += size
		}
	}
	Printf("meta  count is %7d sz %7.1f MiB\n", count_meta, float64(size_meta) / ONE_MEG)
	Printf("data  count is %7d sz %7.1f MiB\n", count_data, float64(size_data) / ONE_MEG)
	Printf("total count is %7d sz %7.1f MiB\n", count_meta + count_data,
		float64(size_meta + size_data) / ONE_MEG)
}

func service4 (repositoryData *RepositoryData) {
	Printf("\n*** service 4 ***\n")
	// count some tables
	// tree blobs and data blobs in index
	count_tree_blobs := 0
	count_data_blobs := 0
	size_tree_blobs  := uint64(0)
	size_data_blobs  := uint64(0)
	for _,bh := range repositoryData.index_handle {
		if bh.Type == restic.TreeBlob {
			count_tree_blobs++
			size_tree_blobs += uint64(bh.size)
		} else if bh.Type == restic.DataBlob {
			count_data_blobs++
			size_data_blobs += uint64(bh.size)
		}
	}
	count := 0
	for _, lists := range repositoryData.directory_map {
		count += len(lists)
	}
	countxx := 0
	for _, blob_list := range repositoryData.meta_dir_map {
		countxx += len(blob_list)
	}
	set_pack_meta := restic.NewIntSet()
	set_pack_data := restic.NewIntSet()
	for _, data := range repositoryData.index_handle {
		if data.Type == restic.TreeBlob {
			set_pack_meta.Insert(data.pack_index)
		} else if data.Type == restic.DataBlob {
			set_pack_data.Insert(data.pack_index)
		}
	}

	Printf("meta packfiles   %7d\n", len(set_pack_meta))
	Printf("data packfiles   %7d\n", len(set_pack_data))
	Printf("all  packfiles   %7d\n", len(set_pack_data) + len(set_pack_meta))
	Printf("nodes idd_file   %7d\n", count)
	Printf("nodes meta_dir   %7d\n", countxx)
	Printf("number of snaps  %7d\n", len(repositoryData.snaps))
	Printf("count tree blobs %7d, sz %10.1f MiB\n", count_tree_blobs, float64(size_tree_blobs) / ONE_MEG)
	Printf("count data blobs %7d, sz %10.1f MiB\n", count_data_blobs, float64(size_data_blobs) / ONE_MEG)
}
