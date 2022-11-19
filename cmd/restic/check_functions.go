package main

import (
	// sets
	"github.com/wplapper/restic/library/mapset"

	// restic library
	//"github.com/wplapper/restic/library/restic"

)

func check_db_snapshots(db_aggregate *DBAggregate, repositoryData *RepositoryData, newComers *Newcomers) bool {
	// compare snapshots from repo with snapshots stored in the database
	// step 1: build memory table to allow the comparison
	mem_snapshots := CreateMemSnapshots(db_aggregate, repositoryData, newComers)

	// compare snapshot keys
	equal := CompareKeys("snapshots", db_aggregate.Table_snapshots, mem_snapshots)
	if !equal {
		set_db_keys  := mapset.NewSet[string]()
		set_mem_keys := mapset.NewSet[string]()
		for key := range db_aggregate.Table_snapshots {
			set_db_keys.Add(key)
		}
		for key := range mem_snapshots {
			set_mem_keys.Add(key)
		}

		len_db  := set_db_keys.Cardinality()
		len_mem := set_mem_keys.Cardinality()

		var diff mapset.Set[string]
		var which string
		if len_db > len_mem {
			diff = set_db_keys.Difference(set_mem_keys)
			which = "mem"
		} else {
			diff = set_mem_keys.Difference(set_db_keys)
			which = "db "
		}

		count := 0
		for comp_ix := range diff.Iter() {
			Printf("check_db_snapshots key %s %s\n", which, comp_ix)
			count++
			if count > 20 {
				break
			}
		}
		return equal
	}

	// compare snapshot values
	compare_equals := true
	for db_key, db_value := range db_aggregate.Table_snapshots {
		mem_value := mem_snapshots[db_key]
		if db_value.Snap_host != mem_value.Snap_host || db_value.Snap_time != mem_value.Snap_time {
			Printf("db  %s %s %s\n", db_value.Snap_time, db_value.Snap_host, db_value.Snap_fsys)
			Printf("mem %s %s %s\n", mem_value.Snap_time, mem_value.Snap_host, mem_value.Snap_fsys)
			compare_equals = false
		}
	}
	return compare_equals
}

func check_db_index_repo(db_aggregate *DBAggregate, repositoryData *RepositoryData,
newComers *Newcomers) bool {

	mem_repo_index_map := CreateMemIndexRepo(db_aggregate, repositoryData, newComers)
	if mem_repo_index_map == nil {
		Printf("Cannot create CreateMemIndexRepo\n")
		return false
	}
	equal := CompareKeys("index_repo", db_aggregate.Table_index_repo, mem_repo_index_map)
	if !equal {
		return equal
	}

	// compare table values
	compare_equals := true
	for db_key, db_value := range db_aggregate.Table_index_repo {
		mem_value := mem_repo_index_map[db_key]
		if db_value.Idd_size != mem_value.Idd_size || db_value.Index_type != mem_value.Index_type {
			Printf("v db  %7d\n", db_value.Idd_size)
			Printf("v mem %7d\n", mem_value.Idd_size)
			compare_equals = false
		}
	}
	return compare_equals
}

func check_db_names(db_aggregate *DBAggregate, repositoryData *RepositoryData,
newComers *Newcomers) bool {
	// build a memory map of the names, whic come from three(3) different sources
	table_name := "names"
	mem_names_map := CreateMemNames(db_aggregate, repositoryData, newComers)
	equal := CompareKeys(table_name, db_aggregate.Table_names, mem_names_map)
	if equal {
		return equal
	}

	set_db_keys  := mapset.NewSet[string]()
	set_mem_keys := mapset.NewSet[string]()
	for key := range db_aggregate.Table_names {
		set_db_keys.Add(key)
	}
	for key := range mem_names_map {
		set_mem_keys.Add(key)
	}

	len_db  := set_db_keys.Cardinality()
	len_mem := set_mem_keys.Cardinality()

	var diff mapset.Set[string]
	var which string
	if len_db > len_mem {
		diff = set_db_keys.Difference(set_mem_keys)
		which = "mem"
	} else {
		diff = set_mem_keys.Difference(set_db_keys)
		which = "db "
	}

	count := 0
	for comp_ix := range diff.Iter() {
		Printf("%s %s\n", which, comp_ix)
		count++
		if count > 20 {
			break
		}
	}
	return equal
}

func check_db_packfiles(db_aggregate *DBAggregate, repositoryData *RepositoryData,
newComers *Newcomers) bool {
	//table_name := "packfiles"
	// build a memory map of the packfiles
	mem_packfiles_map := CreateMemPackfiles(db_aggregate, repositoryData, newComers)
	if mem_packfiles_map == nil {
		return false
	}
	// compare keys
	return CompareKeys("packfiles", db_aggregate.Table_packfiles, mem_packfiles_map)
}

// check_db_contents
func check_db_contents(db_aggregate *DBAggregate, repositoryData *RepositoryData,
newComers *Newcomers) bool {
	//table_name := "contents"
	// build a memory map of the contents
	mem_contents_map := CreateMemContents(db_aggregate, repositoryData, newComers)
	if mem_contents_map == nil {
		return false
	}

	// compare keys
	equal := CompareKeys("contents", db_aggregate.Table_contents, mem_contents_map)
	if !equal {
		Printf("**** Key mismatch for contents ***\n")
		set_db_keys  := mapset.NewSet[CompContents]()
		set_mem_keys := mapset.NewSet[CompContents]()
		for key := range db_aggregate.Table_contents {
			set_db_keys.Add(key)
		}
		for key := range db_aggregate.Table_contents {
			set_mem_keys.Add(key)
		}
		diff := set_db_keys.Difference(set_mem_keys)
		Printf("missing from memory %d keys\n", diff.Cardinality())
		count := 0
		for comp_ix := range diff.Iter() {
			meta_blob := comp_ix.meta_blob
			Printf("missing %s %3d %3d\n", meta_blob.String()[:12], comp_ix.position,
				comp_ix.offset)
			count++
			if count > 20 {
				break
			}
		}
		return equal
	}

	// loop over data_blobs
	equal = true
	//count := 0
	/*
	for db_key, _ := range db_aggregate.Table_contents {
		mem_value := mem_contents_map[db_key]
		// db_value.id_data_idd is not stored, it needs to be calculated
		db_value_id_data_idd_blob := db_aggregate.pk_index_repo[mem_value.Id_data_idd]
		ix := int(repositoryData.blob_to_index[db_value_id_data_idd_blob])
		if ix != mem_value.Id_data_idd {
			equal = false
			count++
		}
	}
	if count == 0 {
		return equal
	}

	count = 0
	Printf("*** Check contents FAIL ***\n")
	for db_key, db_value := range db_aggregate.Table_contents {
		mem_value := mem_contents_map[db_key]
		mem_value_id_data := db_aggregate.pk_index_repo[mem_value.Id_data_idd]
		ix := int(repositoryData.blob_to_index[mem_value_id_data])
		if ix != db_value.Id_data_idd {
			Printf("key db  %s %3d %3d\n", db_key.meta_blob.String()[:12], db_key.position,
				db_key.offset)
			Printf("  db  %6d %+v\n", db_value.Id_data_idd, db_value)
			Printf("  mem %6d %+v\n", ix, mem_value)
			count++
			if count > 20 {
				break
			}
		}
	}*/
	return equal
}

func check_db_meta_dir(db_aggregate *DBAggregate, repositoryData *RepositoryData,
newComers *Newcomers) bool {
	mem_meta_dir_map := CreateMemMetaDir(db_aggregate, repositoryData, newComers)
	if mem_meta_dir_map == nil {
		return false
	}

	// compare keys
	equal := CompareKeys("meta_dir", db_aggregate.Table_meta_dir, mem_meta_dir_map)
	if !equal {
		set_db_keys  := mapset.NewSet[CompMetaDir]()
		set_mem_keys := mapset.NewSet[CompMetaDir]()
		for key := range db_aggregate.Table_meta_dir {
			set_db_keys.Add(key)
		}
		for key := range mem_meta_dir_map {
			set_mem_keys.Add(key)
		}

		len_db  := set_db_keys.Cardinality()
		len_mem := set_mem_keys.Cardinality()

		var diff mapset.Set[CompMetaDir]
		var which string
		if len_db > len_mem {
			diff = set_db_keys.Difference(set_mem_keys)
			which = "mem"
		} else {
			diff = set_mem_keys.Difference(set_db_keys)
			which = "db "
		}

		count := 0
		count_empty_node := 0
		for comp_ix := range diff.Iter() {
			if comp_ix.meta_blob == EMPTY_NODE_ID {
				count_empty_node++
				continue
			}
			Printf("%s %s %s\n", which, comp_ix.snap_id, comp_ix.meta_blob)
			count++
			if count > 10 {
				break
			}
		}
		if count_empty_node == diff.Cardinality() {
			equal = true
		}
	}

	if !equal {
		Printf("mismatch keys for table %s\n", "meta_dir")
	}
	return equal
}

func check_db_idd_file(db_aggregate *DBAggregate, repositoryData *RepositoryData,
newComers *Newcomers) bool {
	table_name := "idd_file"
	// build a memory map of the packfiles
	mem_idd_file_map := CreateMemIddFile(db_aggregate, repositoryData, newComers)
	if mem_idd_file_map == nil {
		return false
	}
	// compare keys
	equal := CompareKeys(table_name, db_aggregate.Table_idd_file, mem_idd_file_map)
	if !equal {
		return equal
	}

	// check contents of idd_file
	count_print := 0
	for db_key, db_value := range db_aggregate.Table_idd_file {
		mem_value := mem_idd_file_map[db_key]
		if mem_value.Inode != db_value.Inode || mem_value.Size != db_value.Size ||
			mem_value.Mtime  != db_value.Mtime || mem_value.Type != db_value.Type {
			equal = false
			Printf("key %s.%3d\n", db_key.meta_blob.String()[:12], db_key.position)
			Printf("  db   %8d %7d %s %-4s\n", db_value.Inode, db_value.Size,
				db_value.Mtime, db_value.Type)
			Printf("  mem  %8d %7d %s %-4s\n", mem_value.Inode, mem_value.Size,
				mem_value.Mtime, mem_value.Type)
			count_print++
			if count_print > 20 {
				break
			}
		}
	}
	return equal
}
