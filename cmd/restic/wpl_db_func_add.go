package main

import (
	"context"
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
 * row for the requested table.
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

func ForAllSnapShots(gopts GlobalOptions, ctx context.Context, repositoryData *RepositoryData,
	db_aggregate *DBAggregate, newComers *Newcomers) error {

	// there are two cooperating go routines dispatched to reduce memory consumption
	// go1: generate all snapshots from memory
	// go3: generate new record
	wg, _ := errgroup.WithContext(ctx)
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
	wg.Go(func() error {
		high_id := sqlite.Get_high_id("snapshots")
		for snap_id := range ch_snap_id {
			mem := repositoryData.snap_map[snap_id]
			row := SnapshotRecordMem{Id: high_id, Snap_id: snap_id,
				Snap_time: mem.Time.String()[:19], Snap_host: mem.Hostname,
				Snap_fsys: mem.Paths[0], Id_snap_root: mem.Tree.String(), Status: "new"}
			newComers.Mem_snapshots[snap_id] = row
			high_id++
		}
		return nil
	})
	res := wg.Wait()
	if res != nil {
		return res
	}

	for ix, row := range newComers.Mem_snapshots {
		db_aggregate.Table_snapshots[ix] = row
	}
	return nil
}

func DeliverIndexRepoIds(repositoryData *RepositoryData, db_aggregate *DBAggregate,
	fn func(id IntID) error) error {
	for id := range repositoryData.index_handle {
		id_int := repositoryData.blob_to_index[id]
		if _, ok := db_aggregate.Table_index_repo[id_int]; ok {
			continue
		}
		fn(id_int)
	}
	return nil
}

func ForAllIndexRepo(gopts GlobalOptions, ctx context.Context, repositoryData *RepositoryData,
	db_aggregate *DBAggregate, newComers *Newcomers) error {
	// there are two cooperating go routines dispatched to reduce memory consumption
	// go1: generate all new IDs
	// go2: generate new record
	wg, _ := errgroup.WithContext(ctx)
	ch_blob := make(chan IntID)

	// attach first go routine which generates all IDs from memory
	wg.Go(func() error {
		defer close(ch_blob)

		// this callback function needs to return an 'id'
		return DeliverIndexRepoIds(repositoryData, db_aggregate, func(id IntID) error {
			select {
			case <-ctx.Done():
				return nil
			case ch_blob <- id:
			}
			return nil
		})
	})

	wg.Go(func() error {
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

			data3, ok := db_aggregate.Table_packfiles[mem.pack_index]
			if !ok {
				Printf("packfile index %6d not mapped\n", mem.pack_index)
				panic("ForAllIndexRepo,Table_packfiles not mapped")
			}
			mem_ix_repo = IndexRepoRecordMem{Id: high_id, Idd: ID.String(),
				Idd_size: int(mem.size), Index_type: index_type,
				Id_pack_id: data3.Id, Status: "new"}
			newComers.Mem_index_repo[id_int] = &mem_ix_repo
			high_id++
		}
		return nil
	})

	res := wg.Wait()
	if res != nil {
		return res
	}

	// copy table index_repo
	for ix, row := range newComers.Mem_index_repo {
		db_aggregate.Table_index_repo[ix] = row
	}
	return nil
}

// table packfiles
func DeliverPackfileIds(repositoryData *RepositoryData, db_aggregate *DBAggregate,
	fn func(id IntID) error) error {
	for _, handle := range repositoryData.index_handle {
		if _, ok := db_aggregate.Table_packfiles[handle.pack_index]; ok {
			continue
		}
		fn(handle.pack_index)
	}
	return nil
}

func ForAllPackfiles(gopts GlobalOptions, ctx context.Context, repositoryData *RepositoryData,
	db_aggregate *DBAggregate, newComers *Newcomers) error {

	// there are two cooperating go routines dispatched to reduce memory consumption
	// go1: generate all new IDs
	// go2: generate new record
	wg, _ := errgroup.WithContext(ctx)
	ch_pack := make(chan IntID)

	// attach first go routine which generates all IDs from memory
	wg.Go(func() error {
		defer close(ch_pack)

		// this callback function needs to return an 'id'
		return DeliverPackfileIds(repositoryData, db_aggregate, func(id IntID) error {
			select {
			case <-ctx.Done():
				return nil
			case ch_pack <- id:
			}
			return nil
		})
	})

	// and attach the processing go routine
	wg.Go(func() error {
		high_id := sqlite.Get_high_id("packfiles")
		//newComers.Mem_packfiles = make(map[IntID]*PackfilesRecordMem)
		for ix := range ch_pack {
			target := repositoryData.index_to_blob[ix]
			row := PackfilesRecordMem{Id: high_id, Packfile_id: target.String(),
				Status: "new"}
			newComers.Mem_packfiles[ix] = &row
			high_id++
		}
		return nil
	})
	res := wg.Wait()
	if res != nil {
		return res
	}
	// copy to table packfiles
	for ix, row := range newComers.Mem_packfiles {
		db_aggregate.Table_packfiles[ix] = row
	}
	return nil
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

func ForAllNames(gopts GlobalOptions, ctx context.Context, repositoryData *RepositoryData,
	db_aggregate *DBAggregate, newComers *Newcomers) error {

	// there are two cooperating go routines dispatched to reduce memory consumption
	// go1: generate all new IDs
	// go2: generate new record
	wg, _ := errgroup.WithContext(ctx)
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
	wg.Go(func() error {
		high_id := sqlite.Get_high_id("names")
		for name := range ch_names {
			row := NamesRecordMem{Id: high_id, Name: name, Status: "new"}
			newComers.Mem_names[name] = &row
			high_id++
		}
		return nil
	})

	res := wg.Wait()
	if res != nil {
		return res
	}
	// copy new entries of table names
	for ix, row := range newComers.Mem_names {
		db_aggregate.Table_names[ix] = row
	}
	return nil
}

// table meta_dir
func DeliverMetaDirIds(repositoryData *RepositoryData, db_aggregate *DBAggregate,
	fn func(id CompMetaDir) error) error {
	for snap_id, blob_set := range repositoryData.meta_dir_map {
		for meta_blob := range blob_set.Iter() {
			ix := CompMetaDir{snap_id: snap_id.Str(), meta_blob: meta_blob}
			if _, ok := db_aggregate.Table_meta_dir[ix]; ok {
				continue
			}
			fn(ix)
		}
	}
	return nil
}

func ForAllMetaDir(gopts GlobalOptions, ctx context.Context, repositoryData *RepositoryData,
	db_aggregate *DBAggregate, newComers *Newcomers) error {

	// there are two cooperating go routines dispatched to reduce memory consumption
	// go1: generate all new IDs
	// go2: generate new record
	wg, _ := errgroup.WithContext(ctx)
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
	wg.Go(func() error {
		high_id := sqlite.Get_high_id("meta_dir")
		for ix := range ch_meta_dir {
			snap_id := ix.snap_id
			meta_blob := ix.meta_blob
			// access snap back pointer
			data_snap, ok2 := db_aggregate.Table_snapshots[snap_id]
			if !ok2 {
				panic("ForAllMetaDir: no snapshot row found")
			}

			// access Idd back pointer
			data_ix_repo, ok2 := db_aggregate.Table_index_repo[meta_blob]
			if !ok2 {
				if meta_blob == EMPTY_NODE_ID_TRANSLATED {
					continue
				}

				Printf("missing meta blob %s\n", repositoryData.index_to_blob[meta_blob].String()[:12])
				panic("ForAllMetaDir: no index_repo row found")
			}
			row := MetaDirRecordMem{Id: high_id, Id_snap_id: data_snap.Id,
				Id_idd: data_ix_repo.Id, Status: "new"}
			//newComers.Mem_meta_dir[ix] = &row
			newComers.Mem_meta_dir_slice = append(newComers.Mem_meta_dir_slice, &row)
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

func ForAllIddFile(gopts GlobalOptions, ctx context.Context, repositoryData *RepositoryData,
	db_aggregate *DBAggregate, newComers *Newcomers) error {

	// there are two cooperating go routines dispatched to reduce memory consumption
	// go1: generate all new IDs
	// go2: generate new record
	wg, _ := errgroup.WithContext(ctx)
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
	wg.Go(func() error {
		high_id := sqlite.Get_high_id("idd_file")
		for ix := range ch_idd_file {
			var row IddFileRecordMem
			var name string
			meta_blob := ix.meta_blob
			position := ix.position
			node := repositoryData.directory_map[meta_blob][position]
			name = node.name
			db_names, ok := db_aggregate.Table_names[name]
			if !ok {
				Printf("ForAllIddFile: Not mapped name '%s'\n", name)
				panic("ForAllIddFile: no name back pointer!")
			}

			// we need back pointer to Id_blob
			db_ix_repo, ok := db_aggregate.Table_index_repo[meta_blob]
			if !ok {
				Printf("ForAllIddFile: Not mapped meta_blob %6d\n",
					meta_blob)
				panic("ForAllIddFile: no index_repo back pointer!")
			}

			row = IddFileRecordMem{Id: high_id, Position: position, Size: int(node.size),
				Id_blob: db_ix_repo.Id, Inode: int64(node.inode), Mtime: node.mtime.String()[:19],
				Type: node.Type, Id_name: db_names.Id, Status: "new"}
			//newComers.Mem_idd_file[ix] = &row
			newComers.Mem_idd_file_slice = append(newComers.Mem_idd_file_slice, &row)
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

func ForAllContents(gopts GlobalOptions, ctx context.Context, repositoryData *RepositoryData,
	db_aggregate *DBAggregate, newComers *Newcomers) error {

	// there are two cooperating go routines dispatched to reduce memory consumption
	// go1: generate all new IDs
	// go2: generate new record
	wg, _ := errgroup.WithContext(ctx)
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
	wg.Go(func() error {
		high_id := sqlite.Get_high_id("contents")
		for ix := range ch_contents {
			var (
				row        ContentsRecordMem
				id_data_id int
				meta_blob  IntID = ix.meta_blob
				position   int   = ix.position
				offset     int   = ix.offset
			)

			// we need back pointer to Id_blob
			mem_ix_repo, ok := db_aggregate.Table_index_repo[meta_blob]
			if !ok {
				Printf("ForAllContents: Not mapped meta_blob %6d\n", meta_blob)
				panic("ForAllContents: No index_repo back pointer!")
			}
			id_blob := mem_ix_repo.Id

			// construct 'id_data_id' back pointer
			meta := repositoryData.directory_map[meta_blob][position]
			data_blob := meta.content[offset]
			db_ix_repo, ok2 := db_aggregate.Table_index_repo[data_blob]
			if !ok2 {
				Printf("not mapped contents index is %+v\n", ix)
				Printf("ForAllContents: No index_repo back pointer for data_blob %6d\n", data_blob)
				panic("ForAllContents: No index_repo back pointer for data_blob!")
			} else {
				id_data_id = db_ix_repo.Id
			}

			row = ContentsRecordMem{Id: high_id, Position: position, Offset: offset,
				Id_data_idd: id_data_id, Id_blob: id_blob, Status: "new"}
			//newComers.Mem_contents[ix] = &row
			newComers.Mem_contents_slice = append(newComers.Mem_contents_slice, &row)
			high_id++
		}
		return nil
	})
	return wg.Wait()
}

func DeliverFullname(repositoryData *RepositoryData, db_aggregate *DBAggregate,
	fn func(string) error) error {
	for _, fullpath := range repositoryData.fullpath {
		if len(fullpath) >= 2 {
			if _, ok := db_aggregate.Table_fullname[fullpath[2:]]; ok {
				continue
			}
		} else if _, ok := db_aggregate.Table_fullname["/"]; ok {
			continue
		}
		if len(fullpath) > 2 {
			fn(fullpath[2:])
		} else {
			fn("/")
		}
	}
	return nil
}

func ForAllFullname(gopts GlobalOptions, ctx context.Context, repositoryData *RepositoryData,
	db_aggregate *DBAggregate, newComers *Newcomers) error {

	wg, _ := errgroup.WithContext(ctx)
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
	wg.Go(func() error {
		high_id := sqlite.Get_high_id("fullname")
		newComers.Mem_fullname = make(map[string]*FullnameMem)
		for path := range ch_fullname {
			if _, ok := newComers.Mem_fullname[path]; ok {
				continue
			}
			if _, ok := db_aggregate.Table_fullname[path]; !ok {
				row := FullnameMem{Id: high_id, Pathname: path, Status: "new"}
				newComers.Mem_fullname[path] = &row
				high_id++
			}
		}
		return nil
	})
	wg.Wait()

	// copy mem table to dabase table
	for ix, row := range newComers.Mem_fullname {
		db_aggregate.Table_fullname[ix] = row
	}
	return nil
}

// deliver the blob for new entries in repositoryData.fullpath
func DeliverDirPathId(repositoryData *RepositoryData, db_aggregate *DBAggregate,
	fn func(IntID) error) error {
	for blob := range repositoryData.fullpath {
		if _, ok := db_aggregate.Table_dir_path_id[blob]; ok {
			continue
		}
		fn(blob)
	}
	return nil
}

func ForAllDirPathIDs(gopts GlobalOptions, ctx context.Context, repositoryData *RepositoryData,
	db_aggregate *DBAggregate, newComers *Newcomers) error {

	wg, _ := errgroup.WithContext(ctx)
	ch_path_id := make(chan IntID)

	// there are two cooperating go routines dispatched to reduce memory consumption
	// go1: generate all new IDs
	// go2: generate new record
	// attach first go routine which generates all new IDs from memory
	wg.Go(func() error {
		defer close(ch_path_id)

		// this callback function needs to return an 'id'
		return DeliverDirPathId(repositoryData, db_aggregate, func(id IntID) error {
			select {
			case <-ctx.Done():
				return nil
			case ch_path_id <- id:
			}
			return nil
		})
	})

	// and attach the processing go routine
	wg.Go(func() error {
		//newComers.Mem_dir_path_id = make(map[IntID]*DirPathIdMem)
		for blob := range ch_path_id {
			if _, ok := newComers.Mem_index_repo[blob]; ok {
				continue
			}
			if _, ok := db_aggregate.Table_dir_path_id[blob]; !ok {
				pathname := repositoryData.fullpath[blob][2:]
				if pathname == "" {
					pathname = "/"
				}

				// map pathname back to an id in table "fullname"
				db, ok1 := db_aggregate.Table_fullname[pathname]
				if !ok1 {
					Printf("ForAllDirPathIDs.blob %6d path %s unmapped\n", blob, pathname)
					panic("ForAllDirPathId.pathname - no mapping of pathname")
				}

				// translate blob to database id in table dir_path_id
				row1, ok1 := db_aggregate.Table_index_repo[blob]
				if !ok1 {
					Printf("meta_blob %6d not mapped\n", blob)
					panic("ForAllDirNameIDs: No mapping of meta_blob")
				}
				row := DirPathIdMem{Id: row1.Id, Id_pathname: db.Id, Status: "new"}
				newComers.Mem_dir_path_id_sl = append(newComers.Mem_dir_path_id_sl, &row)
			}
		}
		return nil
	})
	return wg.Wait()
}
