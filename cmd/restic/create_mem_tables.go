package main


import (
	// restic library
	"github.com/wplapper/restic/library/restic"

	// sets
	"github.com/wplapper/restic/library/mapset"
)

func CreateMemSnapshots(db_aggregate *DBAggregate, repositoryData *RepositoryData,
newComers *Newcomers) map[string]*SnapshotRecordMem {

	mem_snapshots := make(map[string]*SnapshotRecordMem, len(repositoryData.snaps))
	for _, sn := range repositoryData.snaps {
		key := sn.ID().Str()
		data, ok := db_aggregate.Table_snapshots[key]
		if !ok {
			p := SnapshotRecordMem{Snap_time: sn.Time.String()[:19],
				Id_snap_root: sn.Tree.String(), Snap_host: sn.Hostname,
				Snap_fsys: sn.Paths[0], Snap_id: key, Status: "memory"}
			mem_snapshots[key] = &p
		} else {
			data.Status = "db"
			mem_snapshots[key] = data
		}
	}
	//newComers.mem_snapshots = mem_snapshots
	return mem_snapshots
}

func CreateMemIndexRepo(db_aggregate *DBAggregate,
	repositoryData *RepositoryData, newComers *Newcomers) map[restic.ID]*IndexRepoRecordMem {

	// make a new map for all entries stored in memory
	mem_repo_index_map := make(map[restic.ID]*IndexRepoRecordMem,
		len(repositoryData.index_handle))

	var index_type string
	for id, data := range repositoryData.index_handle {
		data2, ok := db_aggregate.Table_index_repo[id]
		if ok {
			data2.Status = "db"
			mem_repo_index_map[id] = data2
		} else {
			pack_index := data.pack_index
			if data.Type == restic.TreeBlob {
				index_type = "tree"
			} else {
				index_type = "data"
			}

			// pack pointer
			ptr_packID := &(repositoryData.index_to_blob[pack_index])
			data3, ok3 := newComers.mem_packfiles[ptr_packID]
			if !ok3 {
				return nil
			}
			id_pack_id_mem := data3.Id
			p := IndexRepoRecordMem{Idd_size: int(data.size),
				Index_type: index_type,	Id_pack_id: id_pack_id_mem, Idd: id.String(),
				Status: "memory"}
			mem_repo_index_map[id] = &p
		}
	}
	return mem_repo_index_map
}

func CreateMemNames(db_aggregate *DBAggregate,
	repositoryData *RepositoryData, newComers *Newcomers) map[string]*NamesRecordMem {

	mem_names_map := make(map[string]*NamesRecordMem, len(repositoryData.directory_map))
	for _, file_list := range repositoryData.directory_map {
		for _, meta := range file_list {
			switch meta.Type {
			case "file", "dir":
				data, ok := db_aggregate.Table_names[meta.name]
				if !ok {
					p := NamesRecordMem{Name_type: "b", Name: meta.name, Status: "memory"}
					mem_names_map[meta.name] = &p
				} else {
					data.Status = "db"
					mem_names_map[meta.name] = data
				}
			}
		}
	}
	return mem_names_map
}

func CreateMemPackfiles(db_aggregate *DBAggregate, repositoryData *RepositoryData,
newComers *Newcomers) map[*restic.ID]*PackfilesRecordMem {

	// collect all packfiles from the index_handle
	pack_intIDs := mapset.NewSet[restic.IntID]()
	for _, handle := range repositoryData.index_handle {
		pack_intIDs.Add(handle.pack_index)
	}

	// convert the set to a map of mem_packfiles_map
	mem_packfiles_map := make(map[*restic.ID]*PackfilesRecordMem, pack_intIDs.Cardinality())
	for pack_intID := range pack_intIDs.Iter() {
		ix := &(repositoryData.index_to_blob)[pack_intID]
		data, ok := (db_aggregate.Table_packfiles)[ix]
		if !ok {
			p := PackfilesRecordMem{Packfile_id: (*ix).String(), Status: "memory"}
			mem_packfiles_map[ix] = &p
		} else {
			data.Status = "db"
			mem_packfiles_map[ix] = data
		}
	}
	return mem_packfiles_map
}

func CreateMemMetaDir(db_aggregate *DBAggregate,
	repositoryData *RepositoryData, newComers *Newcomers) map[CompMetaDir]*MetaDirRecordMem {

	// meta_dir from memory
	mem_meta_dir_map := make(map[CompMetaDir]*MetaDirRecordMem,
		len(repositoryData.meta_dir_map))
	for snap_id, blob_set := range repositoryData.meta_dir_map {
		for meta_blob := range blob_set {
			ix := CompMetaDir{snap_id: snap_id.Str(), meta_blob: meta_blob}
			data, ok := (db_aggregate.Table_meta_dir)[ix]
			if !ok {
				the_meta_blob := repositoryData.index_to_blob[meta_blob]
				id_idd, ok := newComers.mem_index_repo[the_meta_blob]
				if !ok {
					Printf("CreateMemMetaDir No restic.ID for %s\n", the_meta_blob.String()[:12])
					return nil
				}
				if id_idd.Id == 0 {
					Printf("CreateMemMetaDir: underlying meta_blob=%s row=%+v\n", the_meta_blob.String()[:12], id_idd)
					panic("CreateMemMetaDir no Id_idd assigned!")
				}

				the_snap_id := (*snap_id).Str()
				id_snap_id, ok := newComers.mem_snapshots[the_snap_id]
				if !ok {
					Printf("snap_id should be %s\n", the_snap_id)
					return nil
				}
				p := MetaDirRecordMem{Id_snap_id: id_snap_id.Id, Id_idd: id_idd.Id,
					Status: "memory"}
				mem_meta_dir_map[ix] = &p
			} else {
				data.Status = "db"
				mem_meta_dir_map[ix] = data
			}
		}
	}
	return mem_meta_dir_map
}

func CreateMemIddFile(db_aggregate *DBAggregate,
	repositoryData *RepositoryData, newComers *Newcomers) map[CompIddFile]*IddFileRecordMem {

	mem_idd_file_map := make(map[CompIddFile]*IddFileRecordMem, len(repositoryData.directory_map))
	for meta_blob_int, file_list := range repositoryData.directory_map {
		//meta_blob := repositoryData.index_to_blob[meta_blob_int]
		for position, meta := range file_list {
			switch meta.Type {
			case "file", "dir":
				mtime := meta.mtime.String()[:19]
				ix := CompIddFile{meta_blob: meta_blob_int, position: position}
				data, ok := (db_aggregate.Table_idd_file)[ix]
				if !ok {
					// compute Id_name
					row_name, ok := newComers.mem_names[meta.name]
					if !ok {
						Printf("CreateMemIddFile .id_name name=%s\n", meta.name)
						return nil
					}
					// this row is lacking 'Id_blob' which will be inserted later
					p := IddFileRecordMem{Size: int(meta.size),
							Inode: int64(meta.inode), Mtime: mtime, Type: meta.Type[0:1],
							Id_name: row_name.Id, Position: position, Status: "memory"}
					mem_idd_file_map[ix] = &p
				} else {
					data.Status = "db"
					mem_idd_file_map[ix] = data
				}
			}
		}
	}
	return mem_idd_file_map
}

func CreateMemContents(db_aggregate *DBAggregate,
	repositoryData *RepositoryData, newComers *Newcomers) map[CompContents]*ContentsRecordMem {

	// contents data in memory
	mem_contents_map := make(map[CompContents]*ContentsRecordMem, len(repositoryData.directory_map))
	for meta_blob_int, file_list := range repositoryData.directory_map {
		meta_blob := repositoryData.index_to_blob[meta_blob_int]
		for position, meta := range file_list {
			for offset, data_blob := range meta.content {
				ix := CompContents{meta_blob: meta_blob_int, position: position, offset: offset}
				data, ok := db_aggregate.Table_contents[ix]
				if !ok {
					// get id_blob and id_data
					data2, ok2 := newComers.mem_index_repo[meta_blob]
					if !ok2 {
						Printf("CreateMemContents meta_blob %s\n", meta_blob.String()[:12])
						return nil
					}
					id_blob := data2.Id

					data2, ok2 = newComers.mem_index_repo[repositoryData.index_to_blob[data_blob]]
					if !ok2 {
						Printf("CreateMemContents data_blob %s\n",
							repositoryData.index_to_blob[data_blob].String()[:12])
						return nil
					}
					id_data_idd := data2.Id
					p := ContentsRecordMem{Position: position, Offset: offset,
						Id_blob: id_blob, Id_data_idd: id_data_idd, Status: "memory"}
					mem_contents_map[ix] = &p
				} else {
					data.Status = "db"
					mem_contents_map[ix] = data
				}
			}
		}
	}
	return mem_contents_map
}
