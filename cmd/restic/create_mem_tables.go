package main


import (
	// restic library
	"github.com/wplapper/restic/library/restic"

	// sets
	"github.com/wplapper/restic/library/mapset"
)

func CreateMemSnapshots(db_aggregate *DBAggregate, repositoryData *RepositoryData,
newComers *Newcomers) map[string]SnapshotRecordMem {

	Mem_snapshots := make(map[string]SnapshotRecordMem, len(repositoryData.snaps))
	for _, sn := range repositoryData.snaps {
		key := sn.ID().Str()
		data, ok := db_aggregate.Table_snapshots[key]
		if !ok {
			row := SnapshotRecordMem{Snap_time: sn.Time.String()[:19],
				Id_snap_root: sn.Tree.String(), Snap_host: sn.Hostname,
				Snap_fsys: sn.Paths[0], Snap_id: key, Status: "memory"}
			Mem_snapshots[key] = row
		} else {
			data.Status = "db"
			Mem_snapshots[key] = data
		}
	}
	return Mem_snapshots
}

func CreateMemIndexRepo(db_aggregate *DBAggregate,
	repositoryData *RepositoryData, newComers *Newcomers) map[restic.IntID]*IndexRepoRecordMem {

	// make a new map for all entries stored in memory
	mem_repo_index_map := make(map[restic.IntID]*IndexRepoRecordMem,
		len(repositoryData.index_handle))

	var index_type string
	for id, data := range repositoryData.index_handle {
		id_int := repositoryData.blob_to_index[id]
		data2, ok := db_aggregate.Table_index_repo[id_int]
		if ok {
			data2.Status = "db"
			mem_repo_index_map[id_int] = data2
		} else {
			pack_index := data.pack_index
			if data.Type == restic.TreeBlob {
				index_type = "tree"
			} else {
				index_type = "data"
			}

			// pack pointer
			ptr_packID := &(repositoryData.index_to_blob[pack_index])
			data3, ok3 := newComers.Mem_packfiles[ptr_packID]
			if !ok3 {
				return nil
			}
			id_pack_id_mem := data3.Id
			row := IndexRepoRecordMem{Idd_size: int(data.size),
				Index_type: index_type,	Id_pack_id: id_pack_id_mem, Idd: id.String(),
				Status: "memory"}
			mem_repo_index_map[id_int] = &row
		}
	}
	return mem_repo_index_map
}

func CreateMemNames(db_aggregate *DBAggregate,
	repositoryData *RepositoryData, newComers *Newcomers) map[string]*NamesRecordMem {

	Mem_names_map := make(map[string]*NamesRecordMem, len(repositoryData.directory_map))
	for _, file_list := range repositoryData.directory_map {
		for _, meta := range file_list {
			switch meta.Type {
			case "file", "dir":
				data, ok := db_aggregate.Table_names[meta.name]
				if !ok {
					row := NamesRecordMem{Name: meta.name, Status: "memory"}
					Mem_names_map[meta.name] = &row
				} else {
					data.Status = "db"
					Mem_names_map[meta.name] = data
				}
			}
		}
	}
	return Mem_names_map
}

func CreateMemPackfiles(db_aggregate *DBAggregate, repositoryData *RepositoryData,
newComers *Newcomers) map[*restic.ID]*PackfilesRecordMem {

	// collect all packfiles from the index_handle
	pack_intIDs := mapset.NewSet[restic.IntID]()
	for _, handle := range repositoryData.index_handle {
		pack_intIDs.Add(handle.pack_index)
	}

	// convert the set to a map of Mem_packfiles_map
	Mem_packfiles_map := make(map[*restic.ID]*PackfilesRecordMem, pack_intIDs.Cardinality())
	for pack_intID := range pack_intIDs.Iter() {
		ix := &(repositoryData.index_to_blob)[pack_intID]
		data, ok := (db_aggregate.Table_packfiles)[ix]
		if !ok {
			row := PackfilesRecordMem{Packfile_id: (*ix).String(), Status: "memory"}
			Mem_packfiles_map[ix] = &row
		} else {
			data.Status = "db"
			Mem_packfiles_map[ix] = data
		}
	}
	pack_intIDs = nil
	return Mem_packfiles_map
}
