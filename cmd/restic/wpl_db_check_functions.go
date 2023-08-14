package main

import (
	// system
	"strings"

	// sets
	"github.com/deckarep/golang-set/v2"

	"github.com/wplapper/restic/library/restic"
)

/*
 * All check functions are used in command db_verify to compare Database tables
 * with the equivalent memory tables built by reading from repository data
 */
func check_db_snapshots_row(snap_id string, repositoryData *RepositoryData) SnapshotRecordMem {
	// compare snapshots from repo with snapshots stored in the database
	// step 1: build memory table to allow the comparison
	if sn, ok := repositoryData.snap_map[snap_id]; ok {
		return SnapshotRecordMem{Snap_time: sn.Time.String()[:19],
			Id_snap_root: sn.Tree.String(), Snap_host: sn.Hostname,
			Snap_fsys: sn.Paths[0], Snap_id: snap_id, Id: 1, Status: ""}
	} else {
		return SnapshotRecordMem{}
	}
}

func check_db_snapshots_v2(db_aggregate *DBAggregate, repositoryData *RepositoryData) bool {
	// compare snapshots from repo with snapshots stored in the database
	// compare snapshot values

	compare_equals := true
	count_print := 0
	empty_snapshot := SnapshotRecordMem{}
	// first check - are database snapshots still in repository?
	for db_key, db_value := range db_aggregate.Table_snapshots {
		mem_value := check_db_snapshots_row(db_key, repositoryData)
		if mem_value == empty_snapshot {
			compare_equals = false
			if count_print < 10 {
				count_print++
				Printf("snapshot %s only in database\n", db_key)
				continue
			}
		}

		db_value.Id = 1
		db_value.Status = ""
		if mem_value != db_value {
			compare_equals = false
			if count_print < 10 {
				count_print++
				Printf("snapshot mismatch for %s\n", db_key)
				Printf("db  %+v\n", db_value)
				Printf("mem %+v\n", mem_value)
			}
		}
	}

	// second check if there are more snapshots in the repository
	// compared to the database!!
	count_print = 0
	for _, sn := range repositoryData.snaps {
		snap_id := sn.ID().Str()
		_, ok := db_aggregate.Table_snapshots[snap_id]
		if !ok {
			compare_equals = false
			if count_print < 10 {
				count_print++
				Printf("snapshot %s missing from database\n", snap_id)
			}
		}
	}
	return compare_equals
}

func check_db_names_v2(db_aggregate *DBAggregate, repositoryData *RepositoryData) bool {
	// since all names are stored inside the directory_map, we have to extract them
	// from there
	all_names := mapset.NewSet[string]()
	for _, file_list := range repositoryData.directory_map {
		for _, meta := range file_list {
			switch meta.Type {
			case "file", "dir":
				all_names.Add(meta.name)
			}
		}
	}

	equal := true
	print_count := 0
	for _, row := range db_aggregate.Table_names {
		if all_names.Contains(row.Name) {
			continue
		}
		equal = false
		if print_count < 10 {
			print_count++
			Printf("name %s missing\n", row.Name)
		}
	}
	all_names = nil
	return equal
}

func check_db_idd_file_row(db_key CompIddFile, repositoryData *RepositoryData,
	db_aggregate *DBAggregate) IddFileRecordMem {

	// create new memory record from directory_map, given the input from 'db_key'
	meta_blob := db_key.meta_blob
	position := db_key.position
	meta := repositoryData.directory_map[meta_blob][position]
	switch meta.Type {
	case "file", "dir":
		mtime := meta.mtime.String()[:19]
		// compute Id_name, we need the back pointer to Table_names
		row_name, ok := db_aggregate.Table_names[meta.name]
		if !ok {
			Printf("check_db_idd_file_row.id_name missing. Name=%s\n", meta.name)
			// error return
			return IddFileRecordMem{}
		} else {
			// this row is lacking 'Id_blob' which will be inserted later
			row := IddFileRecordMem{Size: int(meta.size),
				Inode: int64(meta.inode), Mtime: mtime, Type: meta.Type[0:1],
				Id_name: row_name.Id, Position: position, Status: "", Id: 1}
			return row
		}
	}
	// error return
	return IddFileRecordMem{}
}

// this function reads the table idd_file by itself and generates an
// incremental check for each memory row as it goes along. We therefore miss
// a lot of large memory allocations
func check_db_idd_file_v2(db_aggregate *DBAggregate, repositoryData *RepositoryData) bool {

	// read table idd_file
	ptr_index_repo := db_aggregate.pk_index_repo
	rows, err := db_aggregate.tx.Queryx("SELECT * FROM idd_file")
	defer rows.Close()

	equal := true
	print_count := 0
	for rows.Next() {
		var row IddFileRecordMem
		err = rows.StructScan(&row)
		if err != nil {
			Printf("check_db_idd_file_v2.StructScan failed %v\n", err)
			return false
		}

		row.Mtime = strings.Replace(row.Mtime, "T", " ", 1) // replace T with " "
		row.Status = ""
		row.Type = row.Type[0:1] // shorten type to one rune
		row.Id = 1

		// need the back mapping repo_index
		meta_blob := ptr_index_repo[row.Id_blob]
		db_key := CompIddFile{meta_blob: meta_blob, position: row.Position}
		mem_value := check_db_idd_file_row(db_key, repositoryData, db_aggregate)
		mem_value.Id_blob = row.Id_blob

		if mem_value != row {
			equal = false
			Printf("idd_file.key %6d.%3d\n", meta_blob, row.Position)
			Printf("  db   %+v\n", row)
			Printf("  mem  %+v\n", mem_value)
			print_count++
			if print_count > 10 {
				break
			}
		}
	}
	rows.Close()
	return equal
}

// check meta_dir table and
func check_db_meta_dir_v2(db_aggregate *DBAggregate, repositoryData *RepositoryData) bool {
	ptr_snapshot := db_aggregate.pk_snapshots
	ptr_index_repo := db_aggregate.pk_index_repo

	// read table meta_dir and compare with repository (in memory)
	rows, err := db_aggregate.tx.Queryx("SELECT * FROM meta_dir")

	print_count := 0
	equal := true
	for rows.Next() {
		var row MetaDirRecordMem
		err = rows.StructScan(&row)
		if err != nil {
			Printf("check_db_meta_dir_v2.StructScan failed %v\n", err)
			return false
		}

		// need the back mapping to snapshots and repo_index
		// and a composite index
		snap_id := ptr_snapshot[row.Id_snap_id]
		meta_blob := ptr_index_repo[row.Id_idd]
		sn := repositoryData.snap_map[snap_id]
		id_ptr := Ptr2ID(*sn.ID(), repositoryData)
		set_data, ok := repositoryData.meta_dir_map[id_ptr]
		if !ok {
			equal = false
			if print_count < 10 {
				Printf("snap %s not in repositoryData.meta_dir_map\n", snap_id)
			}
			print_count++
			continue
		}

		if !set_data.Contains(meta_blob) {
			if print_count < 10 {
				Printf("meta_blob %6d not found in set\n", meta_blob)
			}
			equal = false
			print_count++
			continue
		}
	}
	rows.Close()
	return equal
}

// compare contents table
func check_db_contents_v2(db_aggregate *DBAggregate, repositoryData *RepositoryData) bool {

	equal := true
	print_count := 0
	ptr_index_repo := db_aggregate.pk_index_repo

	// read table contents
	rows, err := db_aggregate.tx.Queryx("SELECT * FROM contents")
	defer rows.Close()

	for rows.Next() {
		var row ContentsRecordMem
		err = rows.StructScan(&row)
		if err != nil {
			Printf("check_db_contents_v2.StructScan failed %v\n", err)
			return false
		}

		// convert P.id_blob to meta_blob via back pointer in index_repo
		meta_blob := ptr_index_repo[row.Id_blob]
		data_blob := ptr_index_repo[row.Id_data_idd]
		position := row.Position
		offset := row.Offset

		// access memory
		meta := repositoryData.directory_map[meta_blob][position]
		data_content_int := meta.content[offset]
		if data_content_int != data_blob {
			equal = false
			if print_count < 10 {
				Printf("contents data mismatch for %6d.%3d.%3d\n", meta_blob, position, offset)
				Printf("db value %6d mem value %6d\n", data_blob, data_content_int)
				print_count++
			}
		}
	}
	rows.Close()
	return equal
}

// compare the largest table (index_repo) with its counterpart in memory
// that is repositoryData.index_handle
func check_db_index_repo_v2(db_aggregate *DBAggregate, repositoryData *RepositoryData) bool {
	equal := true
	print_count := 0

	// loop over the contents of table index_repo
	for ix, ptr_row := range db_aggregate.Table_index_repo {
		// ix is a meta_blob_int (IntID)
		var index_type string
		id := repositoryData.index_to_blob[ix]
		data := repositoryData.index_handle[id]
		pack_index := data.pack_index
		if data.Type == restic.TreeBlob {
			index_type = "tree"
		} else {
			index_type = "data"
		}

		// back pointer to packfiles
		//ptr_packID := &(repositoryData.index_to_blob[pack_index])
		data3, ok := db_aggregate.Table_packfiles[pack_index]
		if !ok {
			Printf("No matching packfile for pack_index %6d\n", pack_index)
			return false
		}
		// create a new IndexRepoRecordMem
		mem_value := IndexRepoRecordMem{Idd_size: 0,
			Index_type: index_type, Id_pack_id: data3.Id, Idd: id.String(),
			Status: "", Id: 1}

		// prepare row:
		row := *ptr_row
		row.Status = ""
		row.Id = 1
		row.Idd_size = 0

		if row != mem_value {
			equal = false
			if print_count < 10 {
				Printf("cmp_index_repo.blob = %s\n", mem_value.Idd[:12])
				Printf("db  %+v\n", row)
				Printf("mem %+v\n", mem_value)
				print_count++
			}
		}
	}
	return equal
}

// check the pack_files table, the memory equivalent is repositoryData.index_handle
func check_db_packfiles_v2(db_aggregate *DBAggregate, repositoryData *RepositoryData) bool {
	equal := true
	print_count := 0

	// we have to create a memory represenation of all current packfiles in memory
	// collect all packfiles from the index_handle
	pack_IDs := mapset.NewSet[IntID]()
	for _, handle := range repositoryData.index_handle {
		pack_IDs.Add(handle.pack_index)
	}

	// read data from table packfiles and check
	for ix := range db_aggregate.Table_packfiles {
		if pack_IDs.Contains(ix) {
			continue
		}

		equal = false
		if print_count < 10 {
			packID := repositoryData.index_to_blob[ix]
			Printf("packfile %s not found in repository\n", packID.String()[:12])
			print_count++
		}
	}
	pack_IDs = nil // reset Set
	return equal
}
