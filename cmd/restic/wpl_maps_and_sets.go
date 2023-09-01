package main

import (
	// restic
	"github.com/wplapper/restic/library/restic"

	// mapset
	"github.com/deckarep/golang-set/v2"
)

// CreateReverseFullpath maps all path to a set of IntID which share the same
// pathname
func CreateReverseFullpath(repositoryData *RepositoryData) (
reverse_fullpath map[string]mapset.Set[IntID]) {

	// setup return map
	reverse_fullpath = map[string]mapset.Set[IntID]{}
	for int_blob, path := range repositoryData.FullPath {
		if _, ok := reverse_fullpath[path]; !ok {
			reverse_fullpath[path] = mapset.NewSet[IntID]()
		}
		reverse_fullpath[path].Add(int_blob)
	}
	return reverse_fullpath
}

// map blob_int to its packfile identifier (int)
func GetPackIDs(repositoryData *RepositoryData) (result map[IntID]IntID) {

	result = map[IntID]IntID{}
	for _, ih := range repositoryData.IndexHandle {
		result[ih.blob_index] = ih.pack_index
	}
	return result
}

// create a map of all children belonging to one parent
func CreateAllChildren(repositoryData *RepositoryData) (children map[IntID]mapset.Set[IntID]){

	children = map[IntID]mapset.Set[IntID]{EMPTY_NODE_ID_TRANSLATED: mapset.NewSet[IntID]()}
	for parent, idd_file_list := range repositoryData.DirectoryMap {
		children[parent] = mapset.NewSet[IntID]()
		for _, node := range idd_file_list {
			children[parent].Add(node.subtree_ID)
		}
	}
	return children
}

// return a map which points to each blob for all packfiles entries
// in addition the type of packfiles is indicated in the 'PackfileType' field
func MakeBlobsPerPackID(repositoryData *RepositoryData) (
blobs_per_packID map[IntID]CompPackfile) {

	blobs_per_packID = map[IntID]CompPackfile{}
	for _, ih := range repositoryData.IndexHandle {
		if _, ok := blobs_per_packID[ih.pack_index]; !ok {
			blobs_per_packID[ih.pack_index] = CompPackfile{mapset.NewSet[IntID](), ih.Type}
		}
		blobs_per_packID[ih.pack_index].PackBlobSet.Add(ih.blob_index)
	}
	return blobs_per_packID
}

// create the inverse of 'meta_dir_map': it can be used to map back a
// directory to a single snap or a set of snaps
func MakeMetaDirMapReverse(repositoryData *RepositoryData) (
meta_dir_map_reverse map[IntID]mapset.Set[string]) {

	// setup return map
	meta_dir_map_reverse = map[IntID]mapset.Set[string]{}
	for snap_ID, meta_sett := range repositoryData.MetaDirMap {
		snap_id := snap_ID.Str()
		for meta_blob_int := range meta_sett.Iter() {
			if _, ok := meta_dir_map_reverse[meta_blob_int]; ! ok {
				meta_dir_map_reverse[meta_blob_int] = mapset.NewSet[string]()
			}
			meta_dir_map_reverse[meta_blob_int].Add(snap_id)
		}
	}
	return meta_dir_map_reverse
}

// go through all contents and generate unique triples &
// additional string info for sorting
func MakeFullContentsMap2(repositoryData *RepositoryData) (
data_map map[restic.ID]mapset.Set[CompIndexOffet]) {

	// setup return map
	data_map = map[restic.ID]mapset.Set[CompIndexOffet]{}
	for meta_blob_int, file_list := range repositoryData.DirectoryMap {
		meta_blob := repositoryData.IndexToBlob[meta_blob_int]
		meta_blob_str := meta_blob.String()[:12]
		for position, meta := range file_list {
			// generate composite index
			for ix, data_blob_int := range meta.content {
				data_blob := repositoryData.IndexToBlob[data_blob_int]
				data_blob_str := data_blob.String()[:12]

				// this data_blob can appear multiple times in different meta_blobs
				cmp_ix := CompIndexOffet{
					meta_blob_str: meta_blob_str,
					data_blob_str: data_blob_str,
					meta_blob: meta_blob,
					position: position,
					offset: ix, name:
					meta.name,
					data_blob: data_blob,
					meta_blob_int: meta_blob_int,
				}
				if _, ok := data_map[data_blob]; ! ok {
					data_map[data_blob] = mapset.NewSet[CompIndexOffet]()
				}
				data_map[data_blob].Add(cmp_ix)
			}
		}
	}
	return data_map
}

// This function creates a data map which is global for the repository. It
// contains a mapping from a data blob to the containing meta blob a the
// index into the file list, used for gathering the file name to which this data
// blob belongs. Data blob can belong to multiple files.
func map_data_blob_file(repositoryData *RepositoryData) (data_map map[IntID]mapset.Set[CompIddFile]) {
	// map data blobs back to meta_blob, position in directory_map
	data_map = map[IntID]mapset.Set[CompIddFile]{}
	for meta_blob, file_list := range repositoryData.DirectoryMap {
		for position, meta := range file_list {
			// generate composite index
			cmp_ix := CompIddFile{meta_blob: meta_blob, position: position}
			if meta.Type == "file" {
				for _, data_blob := range meta.content {
					if _, ok := data_map[data_blob]; !ok {
						data_map[data_blob] = mapset.NewSet[CompIddFile]()
					}
					data_map[data_blob].Add(cmp_ix)
				}
			}
		}
	}
	return data_map
}

