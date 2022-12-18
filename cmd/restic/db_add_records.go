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
 * ForAll... pick up dat from the repository and create a correctly filled
 * row for the table in question.
 * ! Important: Only missing entries are created !
 */

func DeliverSnapShotsIDs(repositoryData *RepositoryData, db_aggregate *DBAggregate,
fn func(id string) error) error {
	for _, sn := range repositoryData.snaps {
		snap_id := sn.ID().Str()
		_, ok := db_aggregate.Table_snapshots[snap_id]
		if ok {
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
	//count := 0
	wg.Go(func () error {
		high_id := sqlite.Get_high_id("snapshots")
		for snap_id := range ch_snap_id {
			mem := repositoryData.snap_map[snap_id]
			data := SnapshotRecordMem{Id: high_id, Snap_id: snap_id,
				Snap_time: mem.Time.String()[:19], Snap_host: mem.Hostname, Snap_fsys: mem.Paths[0],
				Id_snap_root: mem.Tree.String(), Status: "new"}
			newComers.Mem_snapshots[snap_id] = data
			high_id++
		}
		return nil
	})

	res := wg.Wait()
	return res
}

func DeliverIndexRepoIds(repositoryData *RepositoryData, db_aggregate *DBAggregate,
fn func(id restic.IntID) error) error {
	for id := range repositoryData.index_handle {
		id_int := repositoryData.blob_to_index[id]
		_, ok := db_aggregate.Table_index_repo[id_int]
		if ok {
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
		//count := 0
		high_id := sqlite.Get_high_id("index_repo")
		var index_type string
		for id_int := range ch_blob {
			var mem_ix_repo IndexRepoRecordMem
			var ok bool
			_, ok = db_aggregate.Table_index_repo[id_int]
			if !ok {
				ID := repositoryData.index_to_blob[id_int]
				mem := repositoryData.index_handle[ID]
				if mem.Type == restic.TreeBlob {
					index_type = "tree"
				} else {
					index_type = "data"
				}

				pack_index := mem.pack_index
				//ptr_packID := &(repositoryData.index_to_blob[pack_index])
				data3, ok3 := newComers.Mem_packfiles[pack_index]
				if !ok3 {
					// that will not do!
					panic("ForAllIndexRepo: invalid packfile pointer!")
				}
				id_pack_id_mem := data3.Id

				mem_ix_repo = IndexRepoRecordMem{Id: high_id, Idd: ID.String(),
					Idd_size: int(mem.size), Index_type: index_type,
					Id_pack_id: id_pack_id_mem, Status: "new"}
			} else {
				mem_ix_repo = *(db_aggregate.Table_index_repo[id_int])
			}
			newComers.Mem_index_repo[id_int] = &mem_ix_repo
			high_id++
			//count++
		}
		//Printf("Table index_repo  %6d new rows\n", count)
		return nil
	})

	res := wg.Wait()
	return res
}

// table packfiles
func DeliverPackfileIds(repositoryData *RepositoryData, db_aggregate *DBAggregate,
fn func(id restic.IntID) error) error {
	//Printf("size of index_handle is %6d\n", len(repositoryData.index_handle))
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

	res := wg.Wait()
	return res
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

	// and attach the processin go routine
	//count := 0
	wg.Go(func () error {
		high_id := sqlite.Get_high_id("meta_dir")
		var id_snap_id int
		var id_idd int
		for ix := range ch_meta_dir {
			snap_id := ix.snap_id
			meta_blob := ix.meta_blob
			// access snap back pointer
			mem_snap, ok := newComers.Mem_snapshots[snap_id]
			if !ok {
				// might be an existin snapshot
				data_snap, ok2 := db_aggregate.Table_snapshots[snap_id]
				if !ok2 {
					panic("no snapshot row found")
				} else {
					id_snap_id = data_snap.Id
				}
			} else {
				id_snap_id = mem_snap.Id
			}

			// access Idd back pointer
			mem_ix_repo, ok := newComers.Mem_index_repo[meta_blob]
			if !ok {
				// might be an existin index_repo
				data_ix_repo, ok2 := db_aggregate.Table_index_repo[meta_blob]
				if !ok2 {
					panic("no index_repo row found")
				} else {
					id_idd = data_ix_repo.Id
				}
			} else {
				id_idd = mem_ix_repo.Id
			}

			mem := MetaDirRecordMem{Id: high_id, Id_snap_id: id_snap_id,
				Id_idd: id_idd, Status: "new"}
			newComers.Mem_meta_dir[ix] = &mem
			high_id++
			//count++
		}
		//Printf("Table meta_dir      %6d new rows\n", count)
		return nil
	})

	res := wg.Wait()
	return res
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
	//count := 0
	wg.Go(func () error {
		high_id := sqlite.Get_high_id("names")
		for name := range ch_names {
			mem := NamesRecordMem{Id: high_id, Name: name, Status: "new"}
			newComers.Mem_names[name] = &mem
			high_id++
			//count++
		}
		//Printf("Table names      %6d new rows\n", count)
		return nil
	})

	res := wg.Wait()
	return res
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
	count := 0
	wg.Go(func () error {
		high_id := sqlite.Get_high_id("idd_file")
		for ix := range ch_idd_file {
			var mem IddFileRecordMem
			var name string
			var id_name int
			meta_blob := ix.meta_blob
			position  := ix.position

			mem_ptr, ok := db_aggregate.Table_idd_file[ix]
			if !ok {
				// we have to build new row
				node := repositoryData.directory_map[meta_blob][position]
				name = node.name
				db_names, ok := db_aggregate.Table_names[name]
				if ok {
					// name found, has Id
					id_name = db_names.Id
				} else {
					Mem_names, ok2 := newComers.Mem_names[name]
					if !ok2 {
						Printf("ForAllIddFile: No name back pointer for %s\n", name)
						panic("No name back pointer!")
					} else {
						id_name = Mem_names.Id
					}
				}

				// we need back pointer to Id_blob
				mem_ix_repo, ok := newComers.Mem_index_repo[meta_blob]
				if !ok {
					Printf("ForAllIddFile: No ndex_repo back pointer for meta_blob %6d\n",
						meta_blob)
					panic("No index_repo back pointer!")
				}
				id_blob := mem_ix_repo.Id

				mem = IddFileRecordMem{Position: position, Size: int(node.size),
					Id_blob: id_blob, Inode: int64(node.inode), Mtime: node.mtime.String()[:19],
					Type: node.Type, Id_name: id_name, Status: "new"}
			} else {
				mem = *mem_ptr
				mem.Status = "new"
			}
			mem.Id = high_id
			newComers.Mem_idd_file[ix] = &mem
			high_id++
			count++
		}
		//Printf("Table idd_file %6d new rows\n", count)
		return nil
	})

	res := wg.Wait()
	return res
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
	count := 0
	wg.Go(func () error {
		high_id := sqlite.Get_high_id("contents")
		for ix := range ch_contents {
			var mem ContentsRecordMem
			var id_data_id int
			meta_blob := ix.meta_blob
			position  := ix.position
			offset    := ix.offset

			db_ptr, ok := db_aggregate.Table_contents[ix]
			if !ok {
				// we have to build new row

				// we need back pointer to Id_blob
				mem_ix_repo, ok := newComers.Mem_index_repo[meta_blob]
				if !ok {
					Printf("ForAllContents: No ndex_repo back pointer for meta_blob %6d\n",
						meta_blob)
					panic("ForAllContents: No index_repo back pointer!")
				}
				id_blob := mem_ix_repo.Id

				meta := repositoryData.directory_map[meta_blob][position]
				data_blob := meta.content[offset]
				row_ix_repo, ok :=  newComers.Mem_index_repo[data_blob]
				if !ok {
					db_ix_repo, ok2 := db_aggregate.Table_index_repo[data_blob]
					if !ok2 {
						Printf("contents index is %+v\n", ix)
						Printf("ForAllContents: No index_repo back pointer for data_blob %6d\n",
							data_blob)
						panic("ForAllContents: No index_repo back pointer for data_blob!")
					} else {
						id_data_id = db_ix_repo.Id
					}
				} else {
					id_data_id = row_ix_repo.Id
				}

				mem = ContentsRecordMem{Position: position, Offset: offset,
					Id_data_idd: id_data_id, Id_blob: id_blob, Status: "new"}
			} else {
				mem = *db_ptr
				mem.Status = "new"
			}
			mem.Id = high_id
			newComers.Mem_contents[ix] = &mem
			high_id++
			count++
		}
		//Printf("Table contents %6d new rows\n", count)
		return nil
	})
	res := wg.Wait()
	return res
}
