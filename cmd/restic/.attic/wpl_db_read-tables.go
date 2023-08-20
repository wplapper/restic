package main

import (
	"strings"

	// sqlx for SQLite3
	"database/sql"
	"github.com/jmoiron/sqlx"

	// library
	"github.com/wplapper/restic/library/restic"
)

// read all tables counts
func readAllTablesAndCounts(db_conn *sqlx.Tx, table_counts map[string]int) error {
	// get table names
	sql := "SELECT tbl_name FROM sqlite_master WHERE type='table'"
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
	// from sqlite3 header ouputs for pragma_table_info
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
	// utilize the pseudo table pragma_table_info to retrieve the column names
	table_column_names := make(map[string][]string)
	// get table names first
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
func ReadSnapshotTable(db_conn *sqlx.Tx, db_aggregate *DBAggregate) error {

	db_aggregate.Table_snapshots = make(map[string]SnapshotRecordMem)
	db_aggregate.pk_snapshots = make(map[int]string)
	rows, err := db_conn.Queryx("SELECT * FROM snapshots")
	defer rows.Close()

	/* SnapshotRecordMem
	Id           int
	Snap_id      string
	Snap_time    string
	Snap_host    string
	Snap_fsys    string
	Id_snap_root string
	*/
	for rows.Next() {
		var row SnapshotRecordMem
		err = rows.StructScan(&row)
		if err != nil {
			Printf("ReadSnapshotTable.StructScan failed %v\n", err)
			return err
		}

		row.Status = "db"
		db_aggregate.Table_snapshots[row.Snap_id] = row
		db_aggregate.pk_snapshots[row.Id] = row.Snap_id
	}

	rows.Close()
	return nil
}

// load the database rows for table index_repo into memory
func ReadIndexRepoTable(db_conn *sqlx.Tx, db_aggregate *DBAggregate) error {

	db_aggregate.Table_index_repo = make(map[IntID]*IndexRepoRecordMem)
	db_aggregate.pk_index_repo = make(map[int]IntID)
	repositoryData := db_aggregate.repositoryData
	rows, err := db_conn.Queryx("SELECT * FROM index_repo")
	defer rows.Close()
	for rows.Next() {
		var row IndexRepoRecordMem
		err = rows.StructScan(&row)
		if err != nil {
			Printf("ReadIndexRepoTable:StructScan failed %v\n", err)
			return err
		}

		// convert idd to ID
		idd_as_ID, err := restic.ParseID(row.Idd)
		if err != nil {
			Printf("ReadIndexRepoTable:Parse failed for %s %v\n", row.Idd, err)
			return err
		}
		Ptr2ID3(idd_as_ID, repositoryData, "ix_repo_DB")
		idd_as_IntID := repositoryData.BlobToIndex[idd_as_ID]

		/* IndexRepoRecordMem:
		Id         int
		Idd        string
		Idd_size   int
		Index_type string
		Id_pack_id int
		*/
		row.Status = "db"
		db_aggregate.Table_index_repo[idd_as_IntID] = &row

		// need a mapping from row.Id to db_index_repo
		db_aggregate.pk_index_repo[row.Id] = idd_as_IntID
	}
	rows.Close()
	return nil
}

// load the database rows for table meta_dir into memory
func ReadMetaDirTable(db_conn *sqlx.Tx, db_aggregate *DBAggregate) error {
	ptr_snapshot := db_aggregate.pk_snapshots
	ptr_index_repo := db_aggregate.pk_index_repo
	db_aggregate.Table_meta_dir = make(map[CompMetaDir]*MetaDirRecordMem)
	rows, err := db_conn.Queryx("SELECT * FROM meta_dir")
	defer rows.Close()

	for rows.Next() {
		var row MetaDirRecordMem
		err = rows.StructScan(&row)
		if err != nil {
			Printf("ReadMetaDirTable.StructScan failed %v\n", err)
			return err
		}

		// need the back mapping to snapshots and repo_index
		// and a composite index
		snap_id := ptr_snapshot[row.Id_snap_id]
		meta_blob := ptr_index_repo[row.Id_idd]
		/* MetaDirRecordMem:
		Id         int
		Id_snap_id int // map back to snapshots
		Id_idd     int // map back to index_repo
		*/
		row.Status = "db"
		db_aggregate.Table_meta_dir[CompMetaDir{snap_id: snap_id, meta_blob: meta_blob}] = &row
	}
	rows.Close()
	return nil
}

// load the database rows for table index_repo into memory
func ReadIddFileTable(db_conn *sqlx.Tx, db_aggregate *DBAggregate) error {
	// we need the back pointers
	ptr_index_repo := db_aggregate.pk_index_repo
	db_aggregate.Table_idd_file = make(map[CompIddFile]*IddFileRecordMem)
	rows, err := db_conn.Queryx("SELECT * FROM idd_file")
	defer rows.Close()

	for rows.Next() {
		var row IddFileRecordMem
		err = rows.StructScan(&row)
		if err != nil {
			Printf("ReadIddFileTable.StructScan failed %v\n", err)
			return err
		}

		// need the back mapping to snapshots and repo_index
		// and a composite index
		// meta_blob is a IntID
		meta_blob := ptr_index_repo[row.Id_blob]
		row.Mtime = strings.Replace(row.Mtime, "T", " ", 1) // replace T with " "
		/* IddFileRecordMem:
		Id       int
		Id_blob  int // map back to index_repo
		Position int
		Id_name  int
		Size     int
		Inode    int64
		Mtime    string
		Type     string
		*/
		row.Status = "db"
		db_aggregate.Table_idd_file[CompIddFile{meta_blob: meta_blob, position: row.Position}] = &row
	}
	rows.Close()
	return nil
}

// load the database rows for table names into memory
func ReadNamesTable(db_conn *sqlx.Tx, db_aggregate *DBAggregate) error {
	db_aggregate.Table_names = make(map[string]*NamesRecordMem)
	rows, err := db_conn.Queryx("SELECT * FROM names")
	defer rows.Close()

	for rows.Next() {
		var row NamesRecordMem
		err = rows.StructScan(&row)
		if err != nil {
			Printf("ReadNamesTable.StructScan failed %v\n", err)
			return err
		}
		/* NamesRecordMem:
		Id        int
		Name      string
		*/
		row.Status = "db"
		db_aggregate.Table_names[row.Name] = &row
	}
	rows.Close()
	return nil
}

// load the database rows for table packfiles into memory
func ReadPackfilesTable(db_conn *sqlx.Tx, db_aggregate *DBAggregate) error {
	//table_name := "packfiles"
	db_aggregate.Table_packfiles = make(map[IntID]*PackfilesRecordMem)
	rows, err := db_conn.Queryx("SELECT * FROM packfiles")
	defer rows.Close()

	for rows.Next() {
		var row PackfilesRecordMem
		err = rows.StructScan(&row)
		if err != nil {
			Printf("ReadPackfilesTable.StructScan %v\n", err)
			return err
		}

		id, err := restic.ParseID(row.Packfile_id)
		if err != nil {
			Printf("ReadPackfilesTableParse failed for %s %v\n", row.Packfile_id, err)
			return err
		}

		Ptr2ID3(id, db_aggregate.repositoryData, "ReadPackfilesTable")
		int_id := db_aggregate.repositoryData.BlobToIndex[id]
		row.Status = "db"
		db_aggregate.Table_packfiles[int_id] = &row
	}
	rows.Close()
	return nil
}

// load the database rows for table contents into memory
func ReadContentsTable(db_conn *sqlx.Tx, db_aggregate *DBAggregate) error {
	//table_name := "contents"
	ptr_index_repo := db_aggregate.pk_index_repo
	db_aggregate.Table_contents = make(map[CompContents]*ContentsRecordMem)
	rows, err := db_conn.Queryx("SELECT * FROM contents")
	defer rows.Close()

	for rows.Next() {
		var row ContentsRecordMem
		err = rows.StructScan(&row)
		if err != nil {
			Printf("ReadContentsTable.StructScan failed %v\n", err)
			return err
		}

		// convert P.id_blob to meta_blob via back pointer in index_repo
		meta_blob := ptr_index_repo[row.Id_blob]
		ix := CompContents{meta_blob: meta_blob, position: row.Position, offset: row.Offset}
		/* ContentsRecordMem:
		Id          int
		Id_data_idd int // map back to index_repo
		Id_blob     int // map back to index_repo
		Position    int
		Offset      int
		*/
		row.Status = "db"
		db_aggregate.Table_contents[ix] = &row
	}
	rows.Close()
	return nil
}

func ReadFullnameTable(db_conn *sqlx.Tx, db_aggregate *DBAggregate) error {
	db_aggregate.pk_fullname = make(map[int]string)
	db_aggregate.Table_fullname = make(map[string]*FullnameMem)
	rows, err := db_conn.Queryx("SELECT * FROM fullname")
	defer rows.Close()

	for rows.Next() {
		var row FullnameMem
		/* FullnameMem
		Id        int -> repo_index.id
		Pathname  string
		Status    string
		*/
		err = rows.StructScan(&row)
		if err != nil {
			Printf("ReadFullnameTable.StructScan failed %v\n", err)
			return err
		}

		row.Status = "db"
		db_aggregate.Table_fullname[row.Pathname] = &row
		db_aggregate.pk_fullname[row.Id] = row.Pathname
	}
	rows.Close()
	return nil
}

// Table_dir_path_id
func ReadDirPathIdTable(db_conn *sqlx.Tx, db_aggregate *DBAggregate) error {
	ptr_index_repo := db_aggregate.pk_index_repo
	ptr_fullname := db_aggregate.pk_fullname
	db_aggregate.Table_dir_path_id = make(map[IntID]*DirPathIdMem)

	rows, err := db_conn.Queryx("SELECT * FROM dir_path_id")
	defer rows.Close()

	for rows.Next() {
		var row DirPathIdMem
		/* type DirPathIdMem struct {
		Id          int
		Id_pathname int
		Status      string
		*/
		err = rows.StructScan(&row)
		if err != nil {
			Printf("ReadDirPathIdTable.StructScan failed %v\n", err)
			return err
		}

		row.Status = "db"
		ix, ok := ptr_index_repo[row.Id]
		if !ok {
			Printf("No mapping for primary index %6d\n", row.Id)
			panic("ReadDirPathIdTable No mapping of primary index")
		}

		_, ok = ptr_fullname[row.Id_pathname]
		if !ok {
			Printf("No mapping for Id_pathname %d\n", row.Id_pathname)
			panic("ReadDirPathIdTable - no mapping for Id_pathname")
		}
		db_aggregate.Table_dir_path_id[ix] = &row
	}
	rows.Close()
	return nil
}
