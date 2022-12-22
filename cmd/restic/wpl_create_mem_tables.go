package main


import (
	// restic library
	"github.com/wplapper/restic/library/restic"

	// sets
	"github.com/deckarep/golang-set/v2"
)

/*
 * We need these two functions, since names and packfiles are embedded in other
 * repository structures. To be able to handle them easily, they need to be
 * made visible and extractable.
 */

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
newComers *Newcomers) map[restic.IntID]*PackfilesRecordMem {

	// collect all packfiles from the index_handle
	pack_intIDs := mapset.NewSet[restic.IntID]()
	for _, handle := range repositoryData.index_handle {
		pack_intIDs.Add(handle.pack_index)
	}

	// convert the set to a map of Mem_packfiles_map
	Mem_packfiles_map := make(map[restic.IntID]*PackfilesRecordMem, pack_intIDs.Cardinality())
	for pack_intID := range pack_intIDs.Iter() {
		data, ok := db_aggregate.Table_packfiles[pack_intID]
		if !ok {
			packID := repositoryData.index_to_blob[pack_intID]
			row := PackfilesRecordMem{Packfile_id: packID.String(), Status: "memory"}
			Mem_packfiles_map[pack_intID] = &row
		} else {
			data.Status = "db"
			Mem_packfiles_map[pack_intID] = data
		}
	}
	pack_intIDs = nil
	return Mem_packfiles_map
}
