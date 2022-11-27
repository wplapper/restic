package main

import (
	"strings"
	//"reflect"

	// sqlx for SQLite3
	"database/sql"
	"github.com/jmoiron/sqlx"

	// library
	"github.com/wplapper/restic/library/restic"
)

// read all tables counts
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
	// cid|name|type|notnull|dflt_value|pk
	Cid        int
	Name       string
	Type       string
	Notnull    int
	Dflt_value sql.NullString
	Pk         int
}

// GetColumnNames creates a slice of all column names per table in the database
func GetColumnNames(db_conn *sqlx.DB) (map[string][]string, error) {
	// utilize the pseudo table pragma_table_info to select the column names
	table_column_names := make(map[string][]string)
	// get table nams first
	sql := "SELECT tbl_name FROM sqlite_master WHERE type = 'table'"
	Table_names := make([]string, 0)
	err := db_conn.Select(&Table_names, sql)
	if err != nil {
		Printf("SELECT tbl_name FROM sqlite_master failed err %v\n", err)
		return nil, err
	}

	for _, tbl_name := range Table_names {
		column_names := make([]string, 0)
		rows, err := db_conn.Queryx("SELECT * FROM pragma_table_info('" + tbl_name + "')")
		if err != nil {
			Printf("SELECT * FROM pragma_table_info error is %v\n", err)
			return nil, err
		}

		for rows.Next() {
			var result TableInfo
			err = rows.StructScan(&result)
			if err != nil {
				Printf("GetColumnNames.StructScan failed %s %v\n", tbl_name, err)
				return nil, err
			}
			column_names = append(column_names, result.Name)
		}
		table_column_names[tbl_name] = column_names
		rows.Close()
	}
	return table_column_names, nil
}

// load the database rows for table snapshots into memory
func ReadSnapshotTable(db_conn *sqlx.DB, db_aggregate *DBAggregate) error {

	db_snapshots := make(map[string]*SnapshotRecordMem)
	PK_snapshots := make(map[int]string)
	rows, err := db_conn.Queryx("SELECT * FROM snapshots")
	defer rows.Close()

	/* SnapshotRecordDB
	Id           intf
	Snap_id      string
	Snap_time    string
	Snap_host    string
	Snap_fsys    string
	Id_snap_root string
	*/
	for rows.Next() {
		var p SnapshotRecordMem
		err = rows.StructScan(&p)
		if err != nil {
			Printf("ReadSnapshotTable.StructScan failed %v\n", err)
			return err
		}

		p.Status = "db"
		db_snapshots[p.Snap_id] = &p
		PK_snapshots[p.Id] = p.Snap_id
	}

	rows.Close()
	db_aggregate.Table_snapshots = db_snapshots
	db_aggregate.pk_snapshots = PK_snapshots
	return nil
}

// load the database rows for table index_repo into memory
func ReadIndexRepoTable(db_conn *sqlx.DB, db_aggregate *DBAggregate) error {

	// store results in db_index_repo
	db_index_repo := make(map[restic.IntID]*IndexRepoRecordMem)
	PK_index_repo := make(map[int]restic.IntID)
	repositoryData := db_aggregate.repositoryData
	rows, err := db_conn.Queryx("SELECT * FROM index_repo")
	defer rows.Close()

	//print_count := 0
	for rows.Next() {
		var p IndexRepoRecordMem
		err = rows.StructScan(&p)
		if err != nil {
			Printf("ReadIndexRepoTable:StructScan failed %v\n", err)
			return err
		}
		//Printf("row ix_repo %+v\n", p)

		// convert idd to ID
		idd_as_ID, err := restic.ParseID(p.Idd)
		if err != nil {
			Printf("Parse failed for %s %v\n", p.Idd, err)
			return err
		}
		Ptr2ID(idd_as_ID, repositoryData)
		idd_as_IntID := repositoryData.blob_to_index[idd_as_ID]

		/* IndexRepoRecordDB:
		Id         int
		Idd        string
		Idd_size   int
		Index_type string
		Id_pack_id int
		*/
		p.Status = "db"
		db_index_repo[idd_as_IntID] = &p

		// need a mapping from p.Id to db_index_repo
		PK_index_repo[p.Id] = idd_as_IntID
	}
	rows.Close()
	db_aggregate.Table_index_repo = db_index_repo
	db_aggregate.pk_index_repo = PK_index_repo
	return nil
}

// load the database rows for table index_repo into memory
func ReadMetaDirTable(db_conn *sqlx.DB, db_aggregate *DBAggregate) error {

	// store results in db_index_repo
	ptr_snapshot := db_aggregate.pk_snapshots
	ptr_index_repo := db_aggregate.pk_index_repo
	db_meta_dir := make(map[CompMetaDir]*MetaDirRecordMem)
	rows, err := db_conn.Queryx("SELECT * FROM meta_dir")

	for rows.Next() {
		var p MetaDirRecordMem
		err = rows.StructScan(&p)
		if err != nil {
			Printf("ReadMetaDirTable.StructScan failed %v\n", err)
			return err
		}

		// need the back mapping to snapshots and repo_index
		// and a composite index
		snap_id := ptr_snapshot[p.Id_snap_id]
		meta_blob := ptr_index_repo[p.Id_idd]
		/* MetaDirRecordDB:
		Id         int
		Id_snap_id int // map back to snapshots
		Id_idd     int // map back to index_repo
		*/
		p.Status = "db"
		db_meta_dir[CompMetaDir{snap_id: snap_id, meta_blob: meta_blob}] = &p
	}
	rows.Close()
	db_aggregate.Table_meta_dir = db_meta_dir
	return nil
}

// load the database rows for table index_repo into memory
func ReadIddFileTable(db_conn *sqlx.DB, db_aggregate *DBAggregate) error {
	// we need the back pointers
	ptr_index_repo := db_aggregate.pk_index_repo
	db_idd_file := make(map[CompIddFile]*IddFileRecordMem)
	rows, err := db_conn.Queryx("SELECT * FROM idd_file")

	for rows.Next() {
		var p IddFileRecordMem
		err = rows.StructScan(&p)
		if err != nil {
			Printf("ReadIddFileTable.StructScan failed %v\n", err)
			return err
		}

		// need the back mapping to snapshots and repo_index
		// and a composite index
		// meta_blob is a *restic.ID
		meta_blob := ptr_index_repo[p.Id_blob]
		p.Mtime = strings.Replace(p.Mtime, "T", " ", 1) // replace T with " "
		/* IddFileRecordDB:
		Id       int
		Id_blob  int // map back to index_repo
		Position int
		Id_name  int
		Size     int
		Inode    int64
		Mtime    string
		Type     string		 */
		p.Status = "db"
		db_idd_file[CompIddFile{meta_blob: meta_blob, position: p.Position}] = &p
	}
	rows.Close()
	db_aggregate.Table_idd_file = db_idd_file
	return nil
}

// load the database rows for table names into memory
func ReadNamesTable(db_conn *sqlx.DB, db_aggregate *DBAggregate) error {
	table_name := "names"
	db_names := make(map[string]*NamesRecordMem)
	rows, err := db_conn.Queryx("SELECT * FROM " + table_name)

	for rows.Next() {
		var p NamesRecordMem
		err = rows.StructScan(&p)
		if err != nil {
			Printf("ReadNamesTable.StructScan failed %v\n", err)
			return err
		}
		/* NamesRecordDB:
		Id        int
		Name      string
		Name_type string
		*/
		p.Status = "db"
		db_names[p.Name] = &p
	}
	rows.Close()
	db_aggregate.Table_names = db_names
	return nil
}

// load the database rows for table packfiles into memory
func ReadPackfilesTable(db_conn *sqlx.DB, db_aggregate *DBAggregate) error {
	table_name := "packfiles"
	db_packfiles := make(map[*restic.ID]*PackfilesRecordMem)
	rows, err := db_conn.Queryx("SELECT * FROM " + table_name)

	for rows.Next() {
		var p PackfilesRecordMem
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
		/* Id          int Packfile_id string */
		p.Status = "db"
		db_packfiles[id_ptr] = &p
	}
	rows.Close()
	db_aggregate.Table_packfiles = db_packfiles
	return nil
}

// load the database rows for table packfiles into memory
func ReadContentsTable(db_conn *sqlx.DB, db_aggregate *DBAggregate) error {
	table_name := "contents"
	ptr_index_repo := db_aggregate.pk_index_repo
	db_contents := make(map[CompContents]*ContentsRecordMem)
	rows, err := db_conn.Queryx("SELECT * FROM " + table_name)
	defer rows.Close()

	for rows.Next() {
		var p ContentsRecordMem
		err = rows.StructScan(&p)
		if err != nil {
			Printf("ReadContentsTable.StructScan failed %v\n", err)
			return err
		}

		// convert P.id_blob to meta_blob via back pointer in index_repo
		meta_blob, ok := ptr_index_repo[p.Id_blob]
		if !ok {
			Printf("missing id_blob pointer in index_repo for %v\n", p.Id_blob)
			panic("ReadContentsTable.index_repo incomplete meta")
		}

		_, ok = ptr_index_repo[p.Id_data_idd]
		if !ok {
			Printf("missing id_blob ponter in index_repo for %v\n", p.Id_data_idd)
			panic("ReadContentsTable.index_repo incomplete data")
		}

		ix := CompContents{meta_blob: meta_blob, position: p.Position, offset: p.Offset}
		/* ContentsRecordDB:
		Id          int
		Id_data_idd int // map back to index_repo
		Id_blob     int // map back to index_repo
		Position    int
		Offset      int
		Id_fullpath int // deadbeef		 */
		p.Status = "db"
		db_contents[ix] = &p
	}
	rows.Close()
	db_aggregate.Table_contents = db_contents
	return nil
}
