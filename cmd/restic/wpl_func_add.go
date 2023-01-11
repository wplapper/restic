package main

import (
	"golang.org/x/sync/errgroup"

	// restic library
	"github.com/wplapper/restic/library/restic"
	"github.com/wplapper/restic/library/sqlite"

	// sets
	"github.com/deckarep/golang-set/v2"
)

/*
 * The combination of the two corresponding functions Deliver...IDs and
 * ForAll... pick up data from the repository and create a correctly filled
 * row for the table in question.
 * ! Important: Only missing entries are created !
 * These functions are used to determine new rows for the database.
 */

func DeliverSnapShotsIDs(repositoryData *RepositoryData, db_aggregate *DBAggregate,
fn func(id string) error) error {
	for _, sn := range repositoryData.snaps {
		snap_id := sn.ID().Str()
		if _, ok := db_aggregate.Table_snapshots[snap_id]; ok {
			continue
		}
		fn(snap_id)
	}
	return nil
}

func ForAllSnapShots(gopts GlobalOptions, repositoryData *RepositoryData,
db_aggregate *DBAggregate, newComers *Newcomers) error {

	// there are two cooperating go routines dispatched to reduce memory consumption
	// go1: generate all snapshots from memory
	// go3: generate new record
	wg, ctx := errgroup.WithContext(gopts.ctx)
	ch_snap_id := make(chan string)

	// attach first go routine which generates all new snap_ids from memory
	wg.Go(func() error {
		defer close(ch_snap_id)

		// this callback function needs to return an 'id'
		return DeliverSnapShotsIDs(repositoryData, db_aggregate, func(id string) error {
			select {
			case <-ctx.Done():
				return nil
			case ch_snap_id <- id:
			}
			return nil
		})
	})

	// attach second go routine which receives the detailed snap_id requests
	// on 'ch_snap_id' and generates new 'SnapshotRecordMem'
	wg.Go(func () error {
		high_id := sqlite.Get_high_id("snapshots")
		for snap_id := range ch_snap_id {
			mem := repositoryData.snap_map[snap_id]
			row := SnapshotRecordMem{Id: high_id, Snap_id: snap_id,
				Snap_time: mem.Time.String()[:19], Snap_host: mem.Hostname, Snap_fsys: mem.Paths[0],
				Id_snap_root: mem.Tree.String(), Status: "new"}
			newComers.Mem_snapshots[snap_id] = row
			Printf("new snap_id %s %+v\n", snap_id, row)
			high_id++
		}
		return nil
	})
	return wg.Wait()
}

func DeliverIndexRepoIds(repositoryData *RepositoryData, db_aggregate *DBAggregate,
fn func(id restic.IntID) error) error {
	for id := range repositoryData.index_handle {
		id_int := repositoryData.blob_to_index[id]
		if _, ok := db_aggregate.Table_index_repo[id_int]; ok {
			continue
		}
		fn(id_int)
	}
	return nil
}

func ForAllIndexRepo(gopts GlobalOptions, repositoryData *RepositoryData,
db_aggregate *DBAggregate, newComers *Newcomers) error {

	// there are two cooperating go routines dispatched to reduce memory consumption
	// go1: generate all new IDs
	// go2: generate new record
	wg, ctx := errgroup.WithContext(gopts.ctx)
	ch_blob := make(chan restic.IntID)

	// attach first go routine which generates all IDs from memory
	wg.Go(func() error {
		defer close(ch_blob)

		// this callback function needs to return an 'id'
		return DeliverIndexRepoIds(repositoryData, db_aggregate, func(id restic.IntID) error {
			select {
			case <-ctx.Done():
				return nil
			case ch_blob <- id:
			}
			return nil
		})
	})

	wg.Go(func () error {
		high_id := sqlite.Get_high_id("index_repo")
		var index_type string
		for id_int := range ch_blob {
			var mem_ix_repo IndexRepoRecordMem
			ID := repositoryData.index_to_blob[id_int]
			mem := repositoryData.index_handle[ID]
			if mem.Type == restic.TreeBlob {
				index_type = "tree"
			} else {
				index_type = "data"
			}

			data3 := newComers.Mem_packfiles[mem.pack_index]
			mem_ix_repo = IndexRepoRecordMem{Id: high_id, Idd: ID.String(),
				Idd_size: int(mem.size), Index_type: index_type,
				Id_pack_id: data3.Id, Status: "new"}
			newComers.Mem_index_repo[id_int] = &mem_ix_repo
			//Printf("new ix_repo %6d %+v\n", id_int, mem_ix_repo)
			high_id++
		}
		return nil
	})
	return wg.Wait()
}

// table packfiles
func DeliverPackfileIds(repositoryData *RepositoryData, db_aggregate *DBAggregate,
fn func(id restic.IntID) error) error {
	for _, handle := range repositoryData.index_handle {
		if _, ok := db_aggregate.Table_packfiles[handle.pack_index]; ok {
			continue
		}
		fn(handle.pack_index)
	}
	return nil
}

func ForAllPackfiles(gopts GlobalOptions, repositoryData *RepositoryData,
db_aggregate *DBAggregate, newComers *Newcomers) error {

	// there are two cooperating go routines dispatched to reduce memory consumption
	// go1: generate all new IDs
	// go2: generate new record
	wg, ctx := errgroup.WithContext(gopts.ctx)
	ch_pack := make(chan restic.IntID)

	// attach first go routine which generates all IDs from memory
	wg.Go(func() error {
		defer close(ch_pack)

		// this callback function needs to return an 'id'
		return DeliverPackfileIds(repositoryData, db_aggregate, func(id restic.IntID) error {
			select {
			case <-ctx.Done():
				return nil
			case ch_pack <- id:
			}
			return nil
		})
	})

	// and attach the processing go routine
	wg.Go(func () error {
		high_id := sqlite.Get_high_id("packfiles")
		for ix := range ch_pack {
			target := repositoryData.index_to_blob[ix]
			p := PackfilesRecordMem{Id: high_id, Packfile_id: target.String(),
				Status: "new"}
			newComers.Mem_packfiles[ix] = &p
			high_id++
		}
		return nil
	})
	return wg.Wait()
}

// table meta_dir
func DeliverMetaDirIds(repositoryData *RepositoryData, db_aggregate *DBAggregate,
fn func(id CompMetaDir) error) error {
	for snap_id, blob_set := range repositoryData.meta_dir_map {
		for meta_blob := range blob_set {
			ix := CompMetaDir{snap_id: snap_id.Str(), meta_blob: meta_blob}
			if _, ok := db_aggregate.Table_meta_dir[ix]; ok {
				continue
			}
			fn(ix)
		}
	}
	return nil
}

func ForAllMetaDir(gopts GlobalOptions, repositoryData *RepositoryData,
db_aggregate *DBAggregate, newComers *Newcomers) error {

	// there are two cooperating go routines dispatched to reduce memory consumption
	// go1: generate all new IDs
	// go2: generate new record
	wg, ctx := errgroup.WithContext(gopts.ctx)
	ch_meta_dir := make(chan CompMetaDir)

	// attach first go routine which generates all IDs from memory
	wg.Go(func() error {
		defer close(ch_meta_dir)

		// this callback function needs to return an 'id'
		return DeliverMetaDirIds(repositoryData, db_aggregate, func(id CompMetaDir) error {
			select {
			case <-ctx.Done():
				return nil
			case ch_meta_dir <- id:
			}
			return nil
		})
	})

	// and attach the processing go routine
	wg.Go(func () error {
		high_id := sqlite.Get_high_id("meta_dir")
		for ix := range ch_meta_dir {
			snap_id := ix.snap_id
			meta_blob := ix.meta_blob
			// access snap back pointer
			mem_snap, ok := newComers.Mem_snapshots[snap_id]
			if !ok {
				// hope it is an existing snapshot
				data_snap, ok2 := db_aggregate.Table_snapshots[snap_id]
				if !ok2 {
					panic("no snapshot row found")
				} else {
					mem_snap = data_snap
				}
			}

			// access Idd back pointer
			mem_ix_repo, ok := newComers.Mem_index_repo[meta_blob]
			if !ok {
				// hope it is an existing index_repo row
				data_ix_repo, ok2 := db_aggregate.Table_index_repo[meta_blob]
				if !ok2 {
					panic("no index_repo row found")
				} else {
					mem_ix_repo = data_ix_repo
				}
			}
			mem := MetaDirRecordMem{Id: high_id, Id_snap_id: mem_snap.Id,
				Id_idd: mem_ix_repo.Id, Status: "new"}
			newComers.Mem_meta_dir[ix] = &mem
			high_id++
		}
		return nil
	})
	return wg.Wait()
}

// table names
func DeliverNames(repositoryData *RepositoryData, db_aggregate *DBAggregate,
fn func(string) error) error {
	seen := mapset.NewSet[string]()
	for _, file_list := range repositoryData.directory_map {
		for _, meta := range file_list {
			switch meta.Type {
			case "file", "dir":
				name := meta.name
				if _, ok := db_aggregate.Table_names[name]; ok {
					continue
				}
				if seen.Contains(name) {
					continue
				}
				seen.Add(name)
				fn(name)
			}
		}
	}
	seen = nil
	return nil
}

func ForAllNames(gopts GlobalOptions, repositoryData *RepositoryData,
db_aggregate *DBAggregate, newComers *Newcomers) error {

	// there are two cooperating go routines dispatched to reduce memory consumption
	// go1: generate all new IDs
	// go2: generate new record
	wg, ctx := errgroup.WithContext(gopts.ctx)
	ch_names := make(chan string)

	// attach first go routine which generates all IDs from memory
	wg.Go(func() error {
		defer close(ch_names)

		// this callback function needs to return an 'id'
		return DeliverNames(repositoryData, db_aggregate, func(id string) error {
			select {
			case <-ctx.Done():
				return nil
			case ch_names <- id:
			}
			return nil
		})
	})

	// and attach the processin go routine
	wg.Go(func () error {
		high_id := sqlite.Get_high_id("names")
		for name := range ch_names {
			mem := NamesRecordMem{Id: high_id, Name: name, Status: "new"}
			newComers.Mem_names[name] = &mem
			Printf("new name %s %+v\n", name, mem)
			high_id++
		}
		return nil
	})

	return wg.Wait()
}

// table idd_file
func DeliverIddFile(repositoryData *RepositoryData, db_aggregate *DBAggregate,
fn func(CompIddFile) error) error {
	for meta_blob_int, file_list := range repositoryData.directory_map {
		//meta_blob := repositoryData.index_to_blob[meta_blob_int]
		for position, meta := range file_list {
			ix := CompIddFile{meta_blob: meta_blob_int, position: position}
			switch meta.Type {
			case "file", "dir":
				if _, ok := db_aggregate.Table_idd_file[ix]; ok {
					continue
				}
				fn(ix)
			}
		}
	}
	return nil
}

func ForAllIddFile(gopts GlobalOptions, repositoryData *RepositoryData,
db_aggregate *DBAggregate, newComers *Newcomers) error {

	// there are two cooperating go routines dispatched to reduce memory consumption
	// go1: generate all new IDs
	// go2: generate new record
	wg, ctx := errgroup.WithContext(gopts.ctx)
	ch_idd_file := make(chan CompIddFile)

	// attach first go routine which generates all IDs from memory
	wg.Go(func() error {
		defer close(ch_idd_file)

		// this callback function needs to return an 'id'
		return DeliverIddFile(repositoryData, db_aggregate, func(id CompIddFile) error {
			select {
			case <-ctx.Done():
				return nil
			case ch_idd_file <- id:
			}
			return nil
		})
	})

	// and attach the processing go routine
	wg.Go(func () error {
		high_id := sqlite.Get_high_id("idd_file")
		for ix := range ch_idd_file {
			var mem IddFileRecordMem
			var name string
			var id_name int
			meta_blob := ix.meta_blob
			position  := ix.position
			node := repositoryData.directory_map[meta_blob][position]
			name = node.name
			db_names, ok := db_aggregate.Table_names[name]
			if ok {
				// name found, has Id
				id_name = db_names.Id
			} else {
				Mem_names, ok2 := newComers.Mem_names[name]
				if !ok2 {
					Printf("ForAllIddFile: Not mapped '%s'\n", name)
					panic("No name back pointer!")
				} else {
					id_name = Mem_names.Id
				}
			}

			// we need back pointer to Id_blob
			mem_ix_repo, ok := newComers.Mem_index_repo[meta_blob]
			if !ok {
				Printf("ForAllIddFile: Not mapped meta_blob %6d\n",
					meta_blob)
				panic("No index_repo back pointer!")
			}
			id_blob := mem_ix_repo.Id

			mem = IddFileRecordMem{Id: high_id, Position: position, Size: int(node.size),
				Id_blob: id_blob, Inode: int64(node.inode), Mtime: node.mtime.String()[:19],
				Type: node.Type, Id_name: id_name, Status: "new"}
			newComers.Mem_idd_file[ix] = &mem
			high_id++
		}
		return nil
	})
	return wg.Wait()
}

// table contents
func DeliverContents(repositoryData *RepositoryData, db_aggregate *DBAggregate,
fn func(CompContents) error) error {
	for meta_blob_int, file_list := range repositoryData.directory_map {
		for position, meta := range file_list {
			for offset := range meta.content {
				ix := CompContents{meta_blob: meta_blob_int, position: position, offset: offset}
				if _, ok := db_aggregate.Table_contents[ix]; ok {
					continue
				}
				fn(ix)
			}
		}
	}
	return nil
}

func ForAllContents(gopts GlobalOptions, repositoryData *RepositoryData,
db_aggregate *DBAggregate, newComers *Newcomers) error {

	// there are two cooperating go routines dispatched to reduce memory consumption
	// go1: generate all new IDs
	// go2: generate new record
	wg, ctx := errgroup.WithContext(gopts.ctx)
	ch_contents := make(chan CompContents)

	// attach first go routine which generates all IDs from memory
	wg.Go(func() error {
		defer close(ch_contents)

		// this callback function needs to return an 'id'
		return DeliverContents(repositoryData, db_aggregate, func(id CompContents) error {
			select {
			case <-ctx.Done():
				return nil
			case ch_contents <- id:
			}
			return nil
		})
	})

	// and attach the processing go routine
	wg.Go(func () error {
		high_id := sqlite.Get_high_id("contents")
		for ix := range ch_contents {
			var (
				mem        ContentsRecordMem
				id_data_id int
				meta_blob  restic.IntID = ix.meta_blob
				position   int          = ix.position
				offset     int          = ix.offset
			)

			// we need back pointer to Id_blob
			mem_ix_repo, ok := newComers.Mem_index_repo[meta_blob]
			if !ok {
				Printf("ForAllContents: Not mapped meta_blob %6d\n",
					meta_blob)
				panic("ForAllContents: No index_repo back pointer!")
			}
			id_blob := mem_ix_repo.Id

			// construct 'id_data_id' back pointer
			meta := repositoryData.directory_map[meta_blob][position]
			data_blob := meta.content[offset]
			row_ix_repo, ok :=  newComers.Mem_index_repo[data_blob]
			if !ok {
				db_ix_repo, ok2 := db_aggregate.Table_index_repo[data_blob]
				if !ok2 {
					Printf("not mapped contents index is %+v\n", ix)
					Printf("ForAllContents: No index_repo back pointer for data_blob %6d\n",
						data_blob)
					panic("ForAllContents: No index_repo back pointer for data_blob!")
				} else {
					id_data_id = db_ix_repo.Id
				}
			} else {
				id_data_id = row_ix_repo.Id
			}

			mem = ContentsRecordMem{Id: high_id, Position: position, Offset: offset,
				Id_data_idd: id_data_id, Id_blob: id_blob, Status: "new"}
			newComers.Mem_contents[ix] = &mem
			high_id++
		}
		return nil
	})
	return wg.Wait()
}

func DeliverDirChildrenIDs(repositoryData *RepositoryData, db_aggregate *DBAggregate,
fn func(id CompDirChildren) error) error {
	for parent, children := range repositoryData.children {
		for child := range children {
			ix := CompDirChildren{meta_blob_parent: parent, meta_blob_child: child}
			if _, ok := db_aggregate.Table_dir_children[ix]; ok {
				continue
			}
			fn(ix)
		}
	}
	return nil
}

func ForAllDirChildren(gopts GlobalOptions, repositoryData *RepositoryData,
db_aggregate *DBAggregate, newComers *Newcomers) error {

	// there are two cooperating go routines dispatched to reduce memory consumption
	// go1: generate all new IDs
	// go2: generate new record
	wg, ctx := errgroup.WithContext(gopts.ctx)
	ch_dir_children := make(chan CompDirChildren)

	// attach first go routine which generates all IDs from memory
	wg.Go(func() error {
		defer close(ch_dir_children)

		// this callback function needs to return an 'id'
		return DeliverDirChildrenIDs(repositoryData, db_aggregate, func(id CompDirChildren) error {
			select {
			case <-ctx.Done():
				return nil
			case ch_dir_children <- id:
			}
			return nil
		})
	})

	// and attach the processing go routine
	wg.Go(func () error {
		high_id := sqlite.Get_high_id("dir_children")
		newComers.Mem_dir_children = make(map[CompDirChildren]*DirChildrenMem)
		for ix := range ch_dir_children {
			// need to convert to database id_parent, id_child
			meta_blob_parent := ix.meta_blob_parent
			meta_blob_child  := ix.meta_blob_child
			mem_p, okp1 := newComers.Mem_index_repo[meta_blob_parent]
			mem_c, okc1 := newComers.Mem_index_repo[meta_blob_child]
			if !okp1 {
				// check for existing DB row
				dbp, okp2 := db_aggregate.Table_index_repo[meta_blob_parent]
				if !okp2 {
					Printf("not mapped ix=%+v\n", ix)
					panic("ForAllDirChildren: could not find back reference for parent")
				} else {
					mem_p = dbp
				}
			}
			if !okc1 {
				// check for existing DB row
				dbc, okc2 := db_aggregate.Table_index_repo[meta_blob_child]
				if !okc2 {
					Printf("not mapped ix=%+v\n", ix)
					panic("ForAllDirChildren: could not find back reference for child")
				} else {
					mem_c = dbc
				}
			}
			mem := DirChildrenMem{Id: high_id, Id_parent: mem_p.Id, Id_child: mem_c.Id,
				Status: "new"}
			newComers.Mem_dir_children[ix] = &mem
			//Printf("new child %6d %+v\n", ix, mem)
			high_id++
		}
		return nil
	})
	return wg.Wait()
}

// GatherDirNameIDs memorizes the realtionship between directory name aund
// directory blob
func GatherDirNameIDs(repositoryData *RepositoryData, db_aggregate *DBAggregate,
newComers *Newcomers) map[restic.IntID]int {
	map_blob_2_nameID := make(map[restic.IntID]int)
	for _, meta_list := range repositoryData.directory_map {
		for _, meta := range meta_list {
			if meta.Type != "dir" {
				continue
			}
			blob := meta.subtree_ID
			if _, ok := db_aggregate.Table_dir_name_id[blob]; ok {
				continue
			}
			//Printf("GatherDirNameIDs.name %s\n", meta.name)
			row, ok1 := db_aggregate.Table_names[meta.name]
			if !ok1 {
				mem, ok2 := newComers.Mem_names[meta.name]
				if !ok2 {
					Printf("name %s not mapped\n", meta.name)
					panic("GatherDirNameIDs: No name mapping!")
				} else {
					row = mem
				}
			}
			map_blob_2_nameID[blob] = row.Id
		}
	}
	return map_blob_2_nameID
}

func DeliverDirNameIDs(map_blob_2_nameID map[restic.IntID]int, fn func(restic.IntID) error) error {
	for blob := range map_blob_2_nameID {
		fn(blob)
	}
	return nil
}

func ForAllDirNameIDs(gopts GlobalOptions, repositoryData *RepositoryData,
db_aggregate *DBAggregate, newComers *Newcomers) error {

	map_blob_2_nameID := GatherDirNameIDs(repositoryData, db_aggregate, newComers)
	wg, ctx := errgroup.WithContext(gopts.ctx)
	ch_dir_name_id := make(chan restic.IntID)
	newComers.Mem_dir_name_id =make(map[restic.IntID]*DirNameIdMem)

	// there are two cooperating go routines dispatched to reduce memory consumption
	// go1: generate all new IDs
	// go2: generate new record
	// attach first go routine which generates all new IDs from memory
	wg.Go(func() error {
		defer close(ch_dir_name_id)

		// this callback function needs to return an 'id'
		return DeliverDirNameIDs(map_blob_2_nameID, func(id restic.IntID) error {
			select {
			case <-ctx.Done():
				return nil
			case ch_dir_name_id <- id:
			}
			return nil
		})
	})

	// and attach the processing go routine
	wg.Go(func () error {
		for ix := range ch_dir_name_id {
			// need to translate ix into an Id
			row, ok1 := db_aggregate.Table_index_repo[ix]
			if !ok1 {
				mem, ok2 := newComers.Mem_index_repo[ix]
				if !ok2 {
					Printf("meta_blob %6d not mapped\n", ix)
					panic("ForAllDirNameIDs: No mapping of meta_blob")
				} else {
					row = mem
				}
			}

			mem := DirNameIdMem{Id: row.Id, Id_name: map_blob_2_nameID[ix], Status: "new"}
			newComers.Mem_dir_name_id[ix] = &mem
			Printf("new name_id %6d %+v\n", ix, mem)
		}
		return nil
	})

	// finale
	res := wg.Wait()
	map_blob_2_nameID = nil
	return res
}

func DeliverFullname(repositoryData *RepositoryData, db_aggregate *DBAggregate,
fn func(string) error) error {
	//unique_names := mapset.NewSet[string]()
	for _, fullpath := range repositoryData.fullpath {
		//unique_names.Add(fullpath)
		if _, ok := db_aggregate.Table_fullname[fullpath]; ok {
			continue
		}
		if len(fullpath) > 2 {
			fn(fullpath[2:])
		} else {
			fn(fullpath[0:1])
		}
	}
	//Printf("There are %6d unique names in repositoryData.fullpath\n", unique_names.Cardinality())
	return nil
}

func ForAllFullname(gopts GlobalOptions, repositoryData *RepositoryData,
db_aggregate *DBAggregate, newComers *Newcomers) error {

	wg, ctx := errgroup.WithContext(gopts.ctx)
	ch_fullname := make(chan string)

	// there are two cooperating go routines dispatched to reduce memory consumption
	// go1: generate all new IDs
	// go2: generate new record
	// attach first go routine which generates all new IDs from memory
	wg.Go(func() error {
		defer close(ch_fullname)

		// this callback function needs to return an 'id'
		return DeliverFullname(repositoryData, db_aggregate, func(id string) error {
			select {
			case <-ctx.Done():
				return nil
			case ch_fullname <- id:
			}
			return nil
		})
	})

	// and attach the processing go routine
	wg.Go(func () error {
		high_id := sqlite.Get_high_id("fullname")
		newComers.Mem_fullname = make(map[string]*FullnameMem)
		for path := range ch_fullname {
			if _, ok := newComers.Mem_fullname[path]; ok {
				continue
			}
			if _, ok := db_aggregate.Table_fullname[path]; !ok {
				mem := FullnameMem{Id: high_id, Pathname: path, Status: "new"}
				newComers.Mem_fullname[path] = &mem
				//Printf("new fulname %+v\n", mem)
				high_id++
			}
		}
		return nil
	})
	return wg.Wait()
}

// deliver the blob for new entries in repositoryData.fullpath
func DeliverDirPathId(repositoryData *RepositoryData, db_aggregate *DBAggregate,
fn func(restic.IntID) error) error {
	for blob := range repositoryData.fullpath {
		if _, ok := db_aggregate.Table_dir_path_id[blob]; ok {
			continue
		}
		fn(blob)
	}
	return nil
}

func ForAllDirPathIDs(gopts GlobalOptions, repositoryData *RepositoryData,
db_aggregate *DBAggregate, newComers *Newcomers) error {

	wg, ctx := errgroup.WithContext(gopts.ctx)
	ch_path_id := make(chan restic.IntID)

	// there are two cooperating go routines dispatched to reduce memory consumption
	// go1: generate all new IDs
	// go2: generate new record
	// attach first go routine which generates all new IDs from memory
	wg.Go(func() error {
		defer close(ch_path_id)

		// this callback function needs to return an 'id'
		return DeliverDirPathId(repositoryData, db_aggregate, func(id restic.IntID) error {
			select {
			case <-ctx.Done():
				return nil
			case ch_path_id <- id:
			}
			return nil
		})
	})

	// and attach the processing go routine
	wg.Go(func () error {
		newComers.Mem_dir_path_id = make(map[restic.IntID]*DirPathIdMem)
		for blob := range ch_path_id {
			if _, ok := newComers.Mem_dir_path_id[blob]; ok {
				continue
			}
			if _, ok := db_aggregate.Table_dir_path_id[blob]; !ok {
				pathname := repositoryData.fullpath[blob][2:]
				if pathname == "" {
					pathname = "/"
				}
				//Printf("ForAllDirPathIDs.blob %6d path %s\n", blob, pathname)

				// map pathname back to an id in table "fullname"
				db, ok1 := db_aggregate.Table_fullname[pathname]
				if !ok1 {
					// try memory
					mem, ok2 := newComers.Mem_fullname[pathname]
					if !ok2 {
						Printf("ForAllDirPathIDs.blob %6d path %s unmapped\n", blob, pathname)
						panic("ForAllDirPathId.pathname - no mapping of pathname")
					} else {
						db = mem
					}
				}

				// translate blob to database id in table dir_path_id
				row, ok1 := db_aggregate.Table_index_repo[blob]
				if !ok1 {
					mem, ok2 := newComers.Mem_index_repo[blob]
					if !ok2 {
						Printf("meta_blob %6d not mapped\n", blob)
						panic("ForAllDirNameIDs: No mapping of meta_blob")
					} else {
						row = mem
					}
				}
				mem := DirPathIdMem{Id: row.Id, Id_pathname: db.Id, Status: "new"}
				newComers.Mem_dir_path_id[blob] = &mem
				Printf("new path id %+v\n", mem)
			}
		}
		return nil
	})
	return wg.Wait()
}
