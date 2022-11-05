package main

import (
	// restic library
	"github.com/wplapper/restic/library/restic"

	// sets
	"github.com/wplapper/restic/library/mapset"
)

func CreateMemSnapshots(db_aggregate *DBAggregate, repositoryData *RepositoryData,
newComers *Newcomers) map[string]SnapshotRecordMem {
	snaps := repositoryData.snaps
	mem_snapshots := make(map[string]SnapshotRecordMem, len(snaps))
	for _, sn := range snaps {
		key := sn.ID().Str()
		data, ok := (*db_aggregate.table_snapshots)[key]
		if !ok {
			mem_snapshots[key] = SnapshotRecordMem{SnapshotRecordDB: SnapshotRecordDB{Snap_time: sn.Time.String()[:19],
				Snap_host: sn.Hostname, Snap_fsys: sn.Paths[0], Snap_id: key},
				ID_mem: sn.ID(), root: sn.Tree}
		} else {
			data.Status = "db"
			mem_snapshots[key] = data
		}
	}
	newComers.mem_snapshots = mem_snapshots
	return mem_snapshots
}

func CreateMemIndexRepo(db_aggregate *DBAggregate,
	repositoryData *RepositoryData, newComers *Newcomers) map[restic.ID]IndexRepoRecordMem {
	// make a new map for all entries stored in memory
	mem_repo_index_map := make(map[restic.ID]IndexRepoRecordMem,
		db_aggregate.table_counts["index_repo"])

	var index_type string
	for id, data := range repositoryData.index_handle {
		if data.Type == restic.TreeBlob {
			index_type = "tree"
		} else {
			index_type = "data"
		}

		// convert id to a *restic.ID pointer
		data2, ok := (*db_aggregate.table_index_repo)[id]
		ptr_packfile := &repositoryData.index_to_blob[repositoryData.blob_to_index[id]]
		if !ok {
			mem_repo_index_map[id] = IndexRepoRecordMem{IndexRepoRecordDB:
				IndexRepoRecordDB{Idd_size: int(data.size), Index_type: index_type}, packfile: ptr_packfile}
		} else {
			data2.Status = "db"
			mem_repo_index_map[id] = data2
		}
	}
	return mem_repo_index_map
}

func CreateMemNames(db_aggregate *DBAggregate,
	repositoryData *RepositoryData, newComers *Newcomers) map[string]NamesRecordMem {
	mem_names_map := make(map[string]NamesRecordMem)
	for _, file_list := range repositoryData.directory_map {
		for _, meta := range file_list {
			switch meta.Type {
			case "file", "dir":
				data, ok := (*db_aggregate.table_names)[meta.name]
				if !ok {
					mem_names_map[meta.name] = NamesRecordMem{}
				} else {
					data.Status = "db"
					mem_names_map[meta.name] = data
				}
			}
		}
	}
	return mem_names_map
}

func CreateMemPackfiles(db_aggregate *DBAggregate,
	repositoryData *RepositoryData, newComers *Newcomers) map[*restic.ID]PackfilesRecordMem {

	// collect all packfiles from the index_handle
	pack_intIDs := mapset.NewSet[restic.IntID]()
	for _, handle := range repositoryData.index_handle {
		pack_intIDs.Add(handle.pack_index)
	}

	// convert the set to a map of mem_packfiles_map
	mem_packfiles_map := make(map[*restic.ID]PackfilesRecordMem, pack_intIDs.Cardinality())
	for pack_intID := range pack_intIDs.Iter() {
		ix := &(repositoryData.index_to_blob[pack_intID]) //.(restic.IntID)])
		data, ok := (*db_aggregate.table_packfiles)[ix]
		if !ok {
			mem_packfiles_map[ix] = PackfilesRecordMem{}
		} else {
			data.Status = "db"
			mem_packfiles_map[ix] = data
		}
	}
	return mem_packfiles_map
}

func CreateMemContents(db_aggregate *DBAggregate,
	repositoryData *RepositoryData, newComers *Newcomers) map[CompContents]ContentsRecordMem {

	// contents data in memory
	mem_contents_map := make(map[CompContents]ContentsRecordMem)
	for meta_blob_int, file_list := range repositoryData.directory_map {
		meta_blob := repositoryData.index_to_blob[meta_blob_int]
		for position, meta := range file_list {
			for offset, data_blob := range meta.content {
				ix := CompContents{meta_blob: meta_blob, position: position, offset: offset}
				data, ok := (*db_aggregate.table_contents)[ix]
				if !ok {
					mem_contents_map[ix] = ContentsRecordMem{ContentsRecordDB: ContentsRecordDB{Position: position,
						Offset: offset, Id_fullpath: 0},
						id_data_idd: &(repositoryData.index_to_blob[int(data_blob)])}
				} else {
					data.Status = "db"
					mem_contents_map[ix] = data
				}
			}
		}
	}
	return mem_contents_map
}

func CreateMemMetaDir(db_aggregate *DBAggregate,
	repositoryData *RepositoryData, newComers *Newcomers) map[CompMetaDir]MetaDirRecordMem {

	// meta_dir from memory
	mem_meta_dir_map := make(map[CompMetaDir]MetaDirRecordMem)
	for snap_id, blob_set := range repositoryData.meta_dir_map {
		for meta_blob := range blob_set {
			ix := CompMetaDir{snap_id: snap_id.Str(),
				meta_blob: repositoryData.index_to_blob[meta_blob]}
			data, ok := (*db_aggregate.table_meta_dir)[ix]
			if !ok {
				mem_meta_dir_map[ix] = MetaDirRecordMem{}
			} else {
				data.Status = "db"
				mem_meta_dir_map[ix] = data
			}
		}
	}
	return mem_meta_dir_map
}

func CreateMemIddFile(db_aggregate *DBAggregate,
	repositoryData *RepositoryData, newComers *Newcomers) map[CompIddFile]IddFileRecordMem {
	mem_idd_file_map := make(map[CompIddFile]IddFileRecordMem)
	/*type IddFileRecordMem struct {
			 id_name	int
			 size			int
			 inode		int64
			 mtime		string
			 Type			string
	}*/
	for meta_blob_int, file_list := range repositoryData.directory_map {
		meta_blob := repositoryData.index_to_blob[meta_blob_int]
		for position, meta := range file_list {
			switch meta.Type {
			case "file", "dir":
				mtime := meta.mtime.String()[:19]
				ix := CompIddFile{meta_blob: meta_blob, position: position}
				data, ok := (*db_aggregate.table_idd_file)[ix]
				if !ok {
					mem_idd_file_map[ix] = IddFileRecordMem{IddFileRecordDB: IddFileRecordDB{Size: int(meta.size),
						Inode: int64(meta.inode), Mtime: mtime, Type: meta.Type[0:1]}, name: meta.name}
				} else {
					data.Status = "db"
					mem_idd_file_map[ix] = data
				}
			}
		}
	}
	return mem_idd_file_map
}

func CreateMemNamesV2(db_aggregate *DBAggregate, repositoryData *RepositoryData, newComers *Newcomers) {
	mem_names := make(map[string]NamesRecordMem)
	for _, file_list := range repositoryData.directory_map {
		for _, meta := range file_list {
			switch meta.Type {
			case "file", "dir":
				data, ok := (*db_aggregate.table_names)[meta.name]
				if !ok {
					mem_names[meta.name] = NamesRecordMem{}
				} else {
					data.Status = "db"
					mem_names[meta.name] = data
				}
			}
		}
	}
}

