package main

import (
	// sets
	"github.com/wplapper/restic/library/mapset"

	"strings"
)

/*
 * All check functions are used in command db_verify to compare Database tables
 * with the equivalent memory tables built by reading in repository data
 */

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

		diff_db :=  set_db_keys.Difference(set_mem_keys)
		diff_mem := set_mem_keys.Difference(set_db_keys)

		len_db  := diff_db.Cardinality()
		len_mem := diff_mem.Cardinality()

		var diff mapset.Set[string]
		if len_mem > 0 {
			Printf("\nSnapshots are  missing from the database.\n")
			diff = diff_mem
		} else if len_db > 0 {
			Printf("\nThere are more snapshots in the database.\n")
			diff = diff_db
		}

		len_diff := diff.Cardinality()
		var add string = ""
		if len_diff > 5 {
			len_diff = 6
			add = "..."
		}
		snap_ids := make([]string, len_diff)
		ix := 0
		for snap_id := range diff.Iter() {
			snap_ids[ix] = snap_id
			if ix >= len_diff - 2 && add != "" {
				snap_ids[ix + 1] = add
				break
			}
			ix++
		}
		Printf("%s\n", strings.Join(snap_ids, ", "))
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
		for key := range mem_contents_map {
			set_mem_keys.Add(key)
		}
		diff := set_db_keys.Difference(set_mem_keys)
		Printf("missing from memory %d keys\n", diff.Cardinality())
		count := 0
		for comp_ix := range diff.Iter() {
			meta_blob := comp_ix.meta_blob
			Printf("missing %6d.%3d.%3d\n", meta_blob, comp_ix.position,
				comp_ix.offset)
			count++
			if count > 20 {
				break
			}
		}
		return equal
	}
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
			if comp_ix.meta_blob == EMPTY_NODE_ID_TRANSLATED {
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
			Printf("key %6d.%3d\n", db_key.meta_blob, db_key.position)
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
