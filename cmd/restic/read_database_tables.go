package main

import (
	//"time"
	"errors"
	"strings"

	// sqlx for SQLite3
	"database/sql"
	"github.com/jmoiron/sqlx"

	// library
	"github.com/wplapper/restic/library/restic"
)

// do a SELECT count(*) on all tables
func readAllTablesAndCounts(db_conn *sqlx.DB, table_counts map[string]int) error {
	// get table names
	sql := "SELECT tbl_name FROM sqlite_master WHERE type = 'table'"
	tbl_names := make([]string, 0)
	err := db_conn.Select(&tbl_names, sql)
	if err != nil {
		Printf("readAllTablesAndCounts.Error in Select %v\n", err)
		return err
	}

	// get counts per table
	var count int
	for _, tbl_name := range tbl_names {
		sql := "SELECT count(*) FROM " + tbl_name
		err := db_conn.Get(&count, sql)
		if err != nil {
			Printf("Query error for Get %s is %v\n", sql, err)
			return err
		}
		table_counts[tbl_name] = count
	}
	return nil
}

type TableInfo struct {
	// from sqlite3 header ouputs
	// cid|name|type|notnull|dflt_value|pk
	Cid        int
	Name       string
	Type       string
	Notnull    int
	Dflt_value sql.NullString
	PK         int
}

func GetColumnNames(db_conn *sqlx.DB) (map[string][]string, error) {
	// utilize the pseudo table pragma_table_info to seect the column names
	table_column_names := make(map[string][]string)
	sql := "SELECT tbl_name FROM sqlite_master WHERE type = 'table'"
	table_names := make([]string, 0)
	err := db_conn.Select(&table_names, sql)
	if err != nil {
		Printf("SELECT tbl_name FROM sqlite_master failed err %v\n", err)
		return nil, err
	}

	var result TableInfo
	for _, tbl_name := range table_names {
		column_names := make([]string, 0)
		rows, err := db_conn.Queryx("SELECT * FROM pragma_table_info('" + tbl_name + "')")
		if err != nil {
			Printf("SELECT * FROM pragma_table_info error is %v\n", err)
			return nil, err
		}

		for rows.Next() {
			err = rows.StructScan(&result)
			if err != nil {
				Printf("GetColumnNames.StructScan failed %v\n", err)
				return nil, err
			}
			column_names = append(column_names, result.Name)
		}
		table_column_names[tbl_name] = column_names
	}
	return table_column_names, nil
}

/* The reading of these SQLite tables could be done in overlapping mode,
 * provided that the database driver does not get confused
 */

// load the database rows for table snapshots into memory
func ReadSnapshotTable(db_conn *sqlx.DB, db_aggregate *DBAggregate) error {

	db_snapshots := make(map[string]SnapshotRecordMem,
		db_aggregate.table_counts["snapshots"])
	PK_snapshots := make(map[int]string, db_aggregate.table_counts["snapshots"])
	rows, err := db_conn.Queryx("SELECT * FROM snapshots")
	defer rows.Close()

	// collect rows
	for rows.Next() {
		/* columns are:
		id INTEGER PRIMARY KEY,             -- ID of table row
		snap_id VARCHAR(8) NOT NULL,        -- snap ID, UNIQUE INDEX
		snap_time VARCHAR(19) NOT NULL,     -- time of snap
		snap_host VARCHAR(50) NOT NULL,     -- host which is to be backep up
		snap_fsys VARCHAR(100) NOT NULL,    -- filesystem to be backup up
		id_snap_root INTEGER NOT NULL       -- the root of the snap
		*/
		var p SnapshotRecordDB
		err = rows.StructScan(&p)
		if err != nil {
			Printf("ReadSnapshotTable.StructScan failed %v\n", err)
			return err
		}

		// convert root to restic.ID
		idd, err := restic.ParseID(p.Id_snap_root)
		if err != nil {
			Printf("ReadSnapshotTable.ParseID failed %v\n", err)
			return err
		}

		// insert database record into memory
		/*
			Id        		int
			Snap_id       string
			Snap_time 		string
			Snap_host 		string
			Snap_fsys 		string
			Id_snap_root 	string
		*/
		root_ptr := Ptr2ID(idd, db_aggregate.repositoryData) // ReadSnapshotTable
		db_snapshots[p.Snap_id] = SnapshotRecordMem{SnapshotRecordDB: p,
			root: root_ptr, Status: "db"}
		//Printf("DB:snapshots:%s %#v\n", p.Snap_id, db_snapshots[p.Snap_id])

		// map snapshot PK to db_snapshots
		PK_snapshots[p.Id] = p.Snap_id
	}
	rows.Close()
	db_aggregate.table_snapshots = &db_snapshots
	db_aggregate.pk_snapshots = &PK_snapshots
	return nil
}

// load the database rows for table index_repo into memory
func ReadIndexRepoTable(db_conn *sqlx.DB, db_aggregate *DBAggregate) error {

	// store results in db_index_repo
	db_index_repo := make(map[restic.ID]IndexRepoRecordMem,
		db_aggregate.table_counts["index_repo"])
	PK_index_repo := make(map[int]restic.ID,
		db_aggregate.table_counts["index_repo"])
	rows, err := db_conn.Queryx("SELECT * FROM index_repo")
	defer rows.Close()

	// collect rows
	for rows.Next() {
		/* columns are:
		id INTEGER PRIMARY KEY,           -- the primary key
		idd VARCHAR(64) NOT NULL,         -- the idd, UNIQUE INDEX
		idd_size INTEGER NOT NULL,        -- idd size
		index_type VARCHAR(4) NOT NULL,   -- type tree / data (might need INDEX)
		id_pack_id INTEGER NOT NULL,      -- pack_id (needs INDEX)
		*/
		var p IndexRepoRecordDB
		err = rows.StructScan(&p)
		if err != nil {
			Printf("ReadIndexRepoTable:StructScan failed %v\n", err)
			return err
		}

		// convert idd to ID
		idd_as_ID, err := restic.ParseID(p.Idd)
		if err != nil {
			Printf("Parse failed for %s %v\n", p.Idd, err)
			return err
		}
		repositoryData := db_aggregate.repositoryData
		ptr_packfile := &repositoryData.index_to_blob[repositoryData.blob_to_index[idd_as_ID]]
		db_index_repo[idd_as_ID] = IndexRepoRecordMem{IndexRepoRecordDB: p,
			Status: "db", packfile: ptr_packfile}

		// need a mapping from p.Id to db_index_repo
		PK_index_repo[p.Id] = idd_as_ID
	}
	rows.Close()
	db_aggregate.table_index_repo = &db_index_repo
	db_aggregate.pk_index_repo = &PK_index_repo
	return nil
}

// load the database rows for table index_repo into memory
func ReadMetaDirTable(db_conn *sqlx.DB, db_aggregate *DBAggregate) error {

	// store results in db_index_repo
	ptr_snapshot := db_aggregate.pk_snapshots
	ptr_index_repo := db_aggregate.pk_index_repo
	db_meta_dir := make(map[CompMetaDir]MetaDirRecordMem,
		db_aggregate.table_counts["index_repo"])
	rows, err := db_conn.Queryx("SELECT * FROM meta_dir")

	// collect rows
	for rows.Next() {
		var p MetaDirRecordDB
		err = rows.StructScan(&p)
		if err != nil {
			Printf("ReadMetaDirTable.StructScan failed %v\n", err)
			return err
		}

		// need the back mapping to snapshots and repo_index
		// and a composite index
		snap_id   := (*ptr_snapshot)[p.Id_snap_id]
		meta_blob := (*ptr_index_repo)[p.Id_idd]
		db_meta_dir[CompMetaDir{snap_id: snap_id, meta_blob: meta_blob}] = MetaDirRecordMem{
			MetaDirRecordDB: p, Status: "db"}
	}
	rows.Close()
	db_aggregate.table_meta_dir = &db_meta_dir
	return nil
}

// load the database rows for table index_repo into memory
func ReadIddFileTable(db_conn *sqlx.DB, db_aggregate *DBAggregate) error {
	/*
		id INTEGER PRIMARY KEY,         -- the primary key
		id_blob INTEGER NOT NULL,       -- ptr to blob table
		position INTEGER NOT NULL,      -- offset in idd_file
		id_name INTEGER NOT NULL,       -- ptr to name table, INDEX
		size INTEGER NOT NULL,          -- size of file or 0 for directories
		inode INTEGER NOT NULL,         -- inode number in filesystem
		mtime VARCHAR,                  -- mtime of file
		type VARCHAR,                   -- type of entry (d/f/l)
	 }*/

	ptr_index_repo := db_aggregate.pk_index_repo
	ptr_name := db_aggregate.pk_names
	// store results in db_idd_file
	db_idd_file := make(map[CompIddFile]IddFileRecordMem,
		db_aggregate.table_counts["idd_file"])
	rows, err := db_conn.Queryx("SELECT * FROM idd_file")

	// collect rows
	for rows.Next() {
		var p IddFileRecordDB
		err = rows.StructScan(&p)
		if err != nil {
			Printf("ReadIddFileTable.StructScan failed %v\n", err)
			return err
		}

		// need the back mapping to snapshots and repo_index
		// and a composite index
		// meta_blob is a *restic.ID
		meta_blob := (*ptr_index_repo)[p.Id_blob]
		name := (*ptr_name)[p.Id_name]
		p.Mtime = strings.Replace(p.Mtime, "T", " ", 1) // replace T with " "
		db_idd_file[CompIddFile{meta_blob: meta_blob, position: p.Position}] = (
			IddFileRecordMem{IddFileRecordDB: p, name: name, Status: "db"})
	}
	rows.Close()
	db_aggregate.table_idd_file = &db_idd_file
	return nil
}

// load the database rows for table names into memory
func ReadNamesTable(db_conn *sqlx.DB, db_aggregate *DBAggregate) error {
	table_name := "names"
	/*
	  id INTEGER PRIMARY KEY,         -- the primary key
	  name TEXT,                      -- all names collected from restic system
	  name_type TEXT --one of b/d/p=basename,dirname,fullpath, INDEX
	*/

	db_names := make(map[string]NamesRecordMem, db_aggregate.table_counts[table_name])
	PK_names := make(map[int]string, db_aggregate.table_counts[table_name])
	rows, err := db_conn.Queryx("SELECT * FROM " + table_name)

	// collect rows
	for rows.Next() {
		var p NamesRecordDB
		err = rows.StructScan(&p)
		if err != nil {
			Printf("ReadNamesTable.StructScan failed %v\n", err)
			return err
		}

		db_names[p.Name] = NamesRecordMem{NamesRecordDB: p, Status: "db"}
		// back pointer to name
		PK_names[p.Id] = p.Name
	}
	rows.Close()
	db_aggregate.table_names = &db_names
	db_aggregate.pk_names = &PK_names
	return nil
}

// load the database rows for table packfiles into memory
func ReadPackfilesTable(db_conn *sqlx.DB, db_aggregate *DBAggregate) error {
	table_name := "packfiles"
	/*
		id INTEGER PRIMARY KEY,         -- the primary key
		packfile_id VARCHAR(64)         -- the packfile ID, UNIQUE INDEX
	*/

	db_packfiles := make(map[*restic.ID]PackfilesRecordMem, db_aggregate.table_counts[table_name])
	PK_packfiles := make(map[int]*restic.ID, db_aggregate.table_counts[table_name])
	rows, err := db_conn.Queryx("SELECT * FROM " + table_name)

	// collect rows
	for rows.Next() {
		var p PackfilesRecordDB
		err = rows.StructScan(&p)
		if err != nil {
			Printf("ReadPackfilesTable.StructScan %v\n", err)
			return err
		}

		// convert packfile_id to restic.ID
		id, err := restic.ParseID(p.Packfile_id)
		if err != nil {
			Printf("ReadPackfilesTableParse failed for %s %v\n", p.Packfile_id, err)
			return err
		}

		id_ptr := Ptr2ID(id, db_aggregate.repositoryData) // ReadPackfilesTable
		db_packfiles[id_ptr] = PackfilesRecordMem{PackfilesRecordDB: p, Status: "db"}
		// back pointer to name
		PK_packfiles[p.Id] = id_ptr
	}
	rows.Close()
	db_aggregate.table_packfiles = &db_packfiles
	db_aggregate.pk_packfiles = &PK_packfiles
	return nil
}

// load the database rows for table packfiles into memory
func ReadContentsTable(db_conn *sqlx.DB, db_aggregate *DBAggregate) error {
	table_name := "contents"
	/*
		id INTEGER PRIMARY KEY,         -- the primary key
		id_data_idd INTEGER NOT NULL,   -- ptr to data_idd, INDEX
		id_blob  INTEGER NOT NULL,      -- ptr to idd_file, needs INDEX
		position INTEGER NOT NULL,      -- position in idd_file
		offset   INTEGER NOT NULL,      -- the offset of the contents list
		id_fullpath INTEGER NOT NULL,   -- reference to names.id
	*/
	ptr_index_repo := db_aggregate.pk_index_repo
	db_contents := make(map[CompContents]ContentsRecordMem,
		db_aggregate.table_counts[table_name])
	rows, err := db_conn.Queryx("SELECT * FROM " + table_name)
	defer rows.Close()

	// collect rows
	for rows.Next() {
		var p ContentsRecordDB
		err = rows.StructScan(&p)
		if err != nil {
			Printf("ReadContentsTable.StructScan failed %v\n", err)
			return err
		}

		// convert P.id_blob to meta_blob via back pointer in index_repo
		meta_blob, ok := (*ptr_index_repo)[p.Id_blob]
		if !ok {
			Printf("missing id_blob pointer in index_repo for %v\n", p.Id_blob)
			return errors.New("ReadContentsTable.index_repo incomplete meta")
		}

		if !ok {
			Printf("missing id_blob ponter in index_repo for %v\n", p.Id_data_idd)
			return errors.New("ReadContentsTable.index_repo incomplete data")
		}

		ix := CompContents{meta_blob: meta_blob, position: p.Position, offset: p.Offset}
		db_contents[ix] = ContentsRecordMem{ContentsRecordDB: p,
			Status: "db"}
	}
	db_aggregate.table_contents = &db_contents
	return nil
}
