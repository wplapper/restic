package main

import (
	// restic library
	"github.com/wplapper/restic/library/restic"
	//"github.com/wplapper/restic/library/sqlite"

  // sets
  "github.com/deckarep/golang-set"
)

func CreateMemSnapshots(db_aggregate *DBAggregate, repositoryData *RepositoryData) map[string]SnapshotRecordMem {
	snaps := repositoryData.snaps
	mem_snapshots := make(map[string]SnapshotRecordMem, len(snaps))
	for _, sn := range snaps {
		mem_snapshots[sn.ID().Str()] = SnapshotRecordMem{SnapshotRecordDB :
			SnapshotRecordDB{Snap_time: sn.Time.String()[:19],
			Snap_host: sn.Hostname, Snap_fsys: sn.Paths[0]}, root: sn.Tree}
	}
	return mem_snapshots
}

func CreateMemIndexRepo(db_aggregate *DBAggregate,
repositoryData *RepositoryData) map[*restic.ID]IndexRepoRecordMem {
	mem_repo_index_map := make(map[*restic.ID]IndexRepoRecordMem,
		(*db_aggregate.table_counts)["index_repo"])
	for id, data := range repositoryData.index_handle {
		var index_type string
		if data.Type == restic.TreeBlob {
			index_type = "tree"
		} else {
			index_type = "data"
		}
		id_ptr := Ptr2ID(id, repositoryData)
		mem_repo_index_map[id_ptr] = IndexRepoRecordMem{IndexRepoRecordDB:
			IndexRepoRecordDB{Idd_size: int(data.size),
			Index_type: index_type}, idd: id_ptr}
	}
	return mem_repo_index_map
}

func CreateMemNames(db_aggregate *DBAggregate,
repositoryData *RepositoryData) map[string]struct{} {
	mem_names_map := make(map[string]struct{})
  for _, file_list := range repositoryData.directory_map {
		for _, meta := range file_list {
			switch meta.Type {
			case "file", "dir":
				mem_names_map[meta.name] = struct{}{}
			}
		}
	}
	return mem_names_map
}

func CreateMemPackfiles(db_aggregate *DBAggregate,
repositoryData *RepositoryData) map[*restic.ID]struct{} {
	pack_intIDs := mapset.NewSet()
	for _, handle := range repositoryData.index_handle {
		pack_intIDs.Add(handle.pack_index)
	}

	// convert the set to a set of mem_packfiles_map
	mem_packfiles_map := make(map[*restic.ID]struct{}, pack_intIDs.Cardinality())
	for pack_intID := range pack_intIDs.Iter() {
		mem_packfiles_map[&(repositoryData.index_to_blob[pack_intID.(restic.IntID)])] = struct{}{}
	}
	return mem_packfiles_map
}

func CreateMemContents(db_aggregate *DBAggregate,
repositoryData *RepositoryData) map[CompContents]*restic.ID {
	mem_contents_map := make(map[CompContents]*restic.ID)
  for meta_blob_int, file_list := range repositoryData.directory_map {
		meta_blob := &(repositoryData.index_to_blob[meta_blob_int])
		for position, meta := range file_list {
			for offset, data_blob := range meta.content {
				ix := CompContents{meta_blob: meta_blob, position: position, offset: offset}
				mem_contents_map[ix] = &(repositoryData.index_to_blob[int(data_blob)])
			}
		}
	}
	return mem_contents_map
}

func CreateMemMetaDir(db_aggregate *DBAggregate,
repositoryData *RepositoryData) map[CompMetaDir]struct{} {
	mem_meta_dir_map := make(map[CompMetaDir]struct{})
	for snap_id, blob_set := range repositoryData.meta_dir_map {
		for meta_blob := range blob_set {
			ix := CompMetaDir{snap_id: snap_id.Str(),
				meta_blob: &(repositoryData.index_to_blob[meta_blob])}
			mem_meta_dir_map[ix] = struct{}{}
		}
	}
	return mem_meta_dir_map
}

func CreateMemIddFile(db_aggregate *DBAggregate,
repositoryData *RepositoryData) map[CompIddFile]IddFileRecordMem {
	mem_idd_file_map := make(map[CompIddFile]IddFileRecordMem)
	// directory_map is map[restic.IntID][]BlobFile2
	// CompIddFile is meta_blob restic.ID, position  int
	/*
	type IddFileRecordMem struct {
		 id_name	int
		 size			int
		 inode		int64
		 mtime		string
		 Type			string
	}*/
	for meta_blob_int, file_list := range repositoryData.directory_map {
		meta_blob := &(repositoryData.index_to_blob[meta_blob_int])
		for position, meta := range file_list {
			switch meta.Type {
			case "file", "dir":
				mtime := meta.mtime.String()[:19]
				ix := CompIddFile{meta_blob: meta_blob, position: position}
				mem_idd_file_map[ix] = IddFileRecordMem{IddFileRecordDB: IddFileRecordDB{Size: int(meta.size),
					Inode: int64(meta.inode), Mtime: mtime, Type: meta.Type[0:1]}, name: meta.name}
			}
		}
	}
	return mem_idd_file_map
}





