package main

import (
	// system
	"sort"
	"fmt"
	"strings"
	"reflect"

	// database
	"database/sql"

	// sqlite
	"github.com/jmoiron/sqlx"
	"github.com/wplapper/restic/library/sqlite"
	//"github.com/wplapper/restic/library/restic"
)

const (
	skip     = iota
	DBOK     = iota
	DBNEW    = iota
	DBUPDATE = iota
	DBDELETE = iota
)

type SnapshotRecordMem struct {
	Status       uint8
	Id           int
	Snap_id      string // == UNIQUE
	Snap_time    string
	Snap_host    string
	Snap_fsys    string
	Snap_root    string // tree root, is actually a restic.ID
}

type IndexRepoRecordMem struct {
	Status             uint8
	Id                 int
	Blob               []byte // == UNIQUE
	Type               string
	Offset             int
	Length             int
	UncompressedLength int
	Pack__id           int // back pointer to packfiles
}

type NamesRecordMem struct {
	Status uint8
	Id     int
	Name   string // == UNIQUE
}

type MetaDirRecordMem struct {
	Status uint8
	Id         int
	Snap__id   int // map back to snapshots
	Blob__id   int // map back to index_repo
	// tuple (Snap__id, Blob__id) == UNIQUE
}

type ContentsRecordMem struct {
	Status      uint8
	Id          int
	Data__id    int // map back to index_repo.id (data)
	Blob__id    int // map back to index_repo.id (owning directory / meta_blob)
	Position    int // with triple (Id_blob, Position, Offset) == UNIQUE
	Offset      int
}

type IddFileRecordMem struct {
	Status   uint8
	Id       int
	Blob__id int // back pointer to index_repo
	Position int
	Name__id int // back pointer to names
	Size     int
	Inode    int64
	Mtime    string
	Type     string
}

type PackfilesRecordMem struct {
	Status      uint8
	Id          int
	Packfile_id []byte // == UNIQUE
}

type FullnameMem struct {
	Status   uint8
	Id       int
	Pathname string // UNIQUE
}

type DirPathIdMem struct {
	Status      uint8
	Id          int
	Pathname__id int // back pointer to fullpath, INDEX
}

// Composite indices for maps
type CompMetaDir struct {
	// composite index on MetaDirRecordMem
	snap_id   int
	meta_blob IntID
}

type CompContents struct {
	Blob__id  IntID
	Position  int
	Offset    int
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

// read all tables counts
func readAllTablesAndCounts(db_conn *sqlx.Tx) (table_counts map[string]int, err error) {
	// get table names

	table_counts = map[string]int{}
	sql := "SELECT tbl_name FROM sqlite_master WHERE type='table'"
	tbl_names := []string{}
	err = db_conn.Select(&tbl_names, sql)
	if err != nil {
		Printf("readAllTablesAndCounts.Error in Select %v\n", err)
		return nil, err
	}

	// get counts per table
	var count int
	for _, tbl_name := range tbl_names {
		sql := "SELECT count(*) FROM " + tbl_name
		err = db_conn.Get(&count, sql)
		if err != nil {
			Printf("Query error for Get %s is %v\n", sql, err)
			return nil, err
		}
		table_counts[tbl_name] = count
	}
	return table_counts, nil
}

// print all table counts
func PrintTableCounts(tx *sqlx.Tx) {
	names_and_counts, err := readAllTablesAndCounts(tx)
	if err != nil { return }

	// sort table names in ascending order
	tbl_names_sorted := make([]string, len(names_and_counts))
	ix := 0
	for tbl_name := range names_and_counts {
		tbl_names_sorted[ix] = tbl_name
		ix++
	}
	sort.Strings(tbl_names_sorted)

	// step 6.3: print table counts
	for _, tbl_name := range tbl_names_sorted {
		Printf("%-12s %8d\n", tbl_name, names_and_counts[tbl_name])
	}

	err = sqlite.Get_all_high_ids()
	if err != nil {
		Printf("db_add_record: Could not get Get_all_high_ids, error is %v\n", err)
		return
	}
}

func InsertTable[KEY comparable, MEM any](tbl_name string, mem_table map[KEY]MEM,
	tx *sqlx.Tx, column_names map[string][]string, changes_made *bool) error {

	// build INSERT statement - bulk INSERT is the only SQL statement which
	// does not need a loop over all new rows
	column_list := column_names[tbl_name]

	// copy all (new) rows from 'mem_table' (values) to 't_insert' which are NEW
	t_insert := make([]MEM, 0, len(mem_table))
	for _, data := range mem_table {
		// dynamic check via 'reflect'
		//check data.Status, Status is always the very first field of all these structs
		if reflect.ValueOf(data).Field(0).Interface().(uint8) == DBNEW {
			t_insert = append(t_insert, data)
		}
	}
	if len(t_insert) == 0 { return nil }

	Printf("INSERT INTO %-15s with %6d rows.\n", tbl_name, len(t_insert))
	sql := fmt.Sprintf("INSERT INTO %s(%s) VALUES(:%s)", tbl_name,
		strings.Join(column_list, ","), strings.Join(column_list, ",:"))

	// PREPAREd statement do not work here, since the number of columns to
	// be inserted is dependent on the table to be inserted into.

	// do the INSERT
	// the splitting into segments could a bit more dynamic
	// its is limited by the product 'number-of-columns' * 'rows_inserted'
	const OFFSET = 4000 // max 8 columns * 4000 = 32000 < 32k
	max := len(t_insert)
	var count int64
	for offset := 0; offset < len(t_insert); offset += OFFSET {
		if max > OFFSET {
			max = OFFSET
		}
		if offset+max > len(t_insert) {
			max = len(t_insert) - offset
		}
		r, err := tx.NamedExec(sql, t_insert[offset:offset+max])
		if err != nil {
			Printf("error INSERT %s error is: %v\n", tbl_name, err)
			return err
		}
		count, _ = r.RowsAffected()
	}

	if count > 0 {
		*changes_made = true
	}
	return nil
}

// GetColumnNames creates a slice of all column names per table in the database
func GetColumnNames(tx *sqlx.Tx) (table_column_names map[string][]string, err error) {
	// utilize the pseudo table pragma_table_info to retrieve the column names
	table_column_names = map[string][]string{}
	// get table names first
	sql := "SELECT tbl_name FROM sqlite_master WHERE type = 'table'"
	Table_names := make([]string, 0)
	err = tx.Select(&Table_names, sql)
	if err != nil {
		Printf("SELECT tbl_name FROM sqlite_master failed err %v\n", err)
		return nil, err
	}

	for _, tbl_name := range Table_names {
		column_names := make([]string, 0)
		rows, err := tx.Queryx("SELECT * FROM pragma_table_info('" + tbl_name + "')")
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
