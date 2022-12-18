package sqlite

import (
	"fmt"
	"os"
	"time"

	// sets
	"github.com/deckarep/golang-set/v2"

	// sqlite3 interface
	"database/sql"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

var SQLITE_TABLES = map[string]string{
  "snapshots": `CREATE TABLE snapshots (
  id INTEGER PRIMARY KEY,             -- ID of table row
  snap_id VARCHAR(8) NOT NULL,        -- snap ID, UNIQUE INDEX
  snap_time VARCHAR(19) NOT NULL,     -- time of snap
  snap_host VARCHAR(50) NOT NULL,     -- host wich is to be backep up
  snap_fsys VARCHAR(100) NOT NULL,    -- filesystem to be backup up
  id_snap_root INTEGER NOT NULL       --  the root of the snap
  )`,

  "index_repo": `CREATE TABLE index_repo (
  -- maintains the contents of the index/ * files
  id INTEGER PRIMARY KEY,           -- the primary key
  idd VARCHAR(64) NOT NULL,         -- the idd, UNIQUE INDEX
  idd_size INTEGER NOT NULL,        -- idd size
  index_type VARCHAR(4) NOT NULL,   -- type tree / data (might need INDEX)
  id_pack_id INTEGER NOT NULL,      -- pack_id (needs INDEX)
  FOREIGN KEY(id_pack_id)   REFERENCES packfiles(id)
  )`,

  "packfiles": `CREATE TABLE packfiles (
  -- needed for the relationship between packfiles and blobs
  id INTEGER PRIMARY KEY,         -- the primary key
  packfile_id VARCHAR(64)         -- the packfile ID, UNIQUE INDEX
  )`,

  "meta_dir": `CREATE TABLE meta_dir (
  -- many to many relationship table between snaps and directory idds
  id INTEGER PRIMARY KEY,           -- the primary key
  id_snap_id INTEGER NOT NULL,      -- the snap_id , INDEX
  id_idd INTEGER NOT NULL,          -- the idd pointer, INDEX
  -- the tuple (id_snap_id, id_idd) is UNIQUE INDEX
  FOREIGN KEY(id_snap_id)   REFERENCES snaphots(id),
  FOREIGN KEY(id_idd)       REFERENCES index_repo(id)
  )`,

  "idd_file": `CREATE TABLE idd_file (
  id INTEGER PRIMARY KEY,         -- the primary key
  id_blob INTEGER NOT NULL,       -- ptr to blob table
  position INTEGER NOT NULL,      -- offset in idd_file
  -- the tuple (id_blob, position) is UNIQUE INDEX
  id_name INTEGER NOT NULL,       -- ptr to name table, INDEX
  size INTEGER NOT NULL,          -- size of file or 0 for directories
  inode INTEGER NOT NULL,         -- inode number in filesystem
  mtime VARCHAR,                  -- mtime of file
  type VARCHAR,                   -- type of entry (d/f/l)
  FOREIGN KEY(id_blob) REFERENCES index_repo(id),
  FOREIGN KEY(id_name) REFERENCES names(id)
  )`,

  "names": `CREATE TABLE names (
  id INTEGER PRIMARY KEY,         -- the primary key
  name TEXT                       -- all names collected from restic system
  -- name is UNIQUE INDEX
  )`,

  "timestamp": `CREATE TABLE timestamp (
  id INTEGER PRIMARY KEY,                 -- the primary key
  restic_updated   TIMESTAMP NOT NULL,    --changes when restic gets updated
  database_updated TIMESTAMP NOT NULL,    --last update of database
  ts_created       TIMESTAMP NOT NULL     --creation date of database
  )`,

  "contents": `CREATE TABLE contents (
  id INTEGER PRIMARY KEY,         -- the primary key
  id_data_idd INTEGER NOT NULL,   -- ptr to data_idd, INDEX
  id_blob  INTEGER NOT NULL,      -- ptr to idd_file, needs INDEX
  position INTEGER NOT NULL,      -- position in idd_file
  offset   INTEGER NOT NULL,      -- the offset of the contents list
  -- the triple (id_blob, position, offset) is a UNIQUE INDEX
  FOREIGN KEY(id_data_idd) REFERENCES index_repo(id),
  FOREIGN KEY(id_blob) REFERENCES     index_repo(id)
  )`,

  // history section
  "snapshots_history": `CREATE TABLE snapshots_history (
  id INTEGER PRIMARY KEY,         -- ID of table row
  timestamp  TIMESTAMP NOT NULL,
  action     TEXT NULL,           -- action type (INSERT, UPDATE, DELETE)
  id_history INTEGER NOT NULL,    -- ID of snapshot table
  snap_id    TEXT NOT NULL,        -- snap ID, UNIQUE INDEX
  snap_time  TEXT NOT NULL,        -- time of snap
  snap_host  TEXT NOT NULL,        -- host wich is to be backep up
  snap_fsys  TEXT NOT NULL,        -- filesystem to be backup up
  id_snap_root INTEGER NOT NULL   -- ref to the root of the snap
  )`,

  "timestamp_history": `CREATE TABLE timestamp_history (
  id INTEGER PRIMARY KEY,           -- ID of table row
  timestamp TIMESTAMP NOT NULL,
  action    TEXT NOT NULL,          -- action type (INSERT, UPDATE, DELETE)
  id_history INTEGER NOT NULL,      -- ID of snapshot table
  restic_updated    TIMESTAMP NOT NULL,  --change when restic gets updated
  database_updated  TIMESTAMP NOT NULL,  --last update of database
  ts_created        TIMESTAMP NOT NULL   --creation date of database
  )`}

var SQLITE_INDEX = map[string][]ListIndexMaps{
	"index_repo": {
		ListIndexMaps{ixname: "ux_ix_repo_idd",     on: "idd", unique: "UNIQUE"},
		ListIndexMaps{ixname: "ix_ix_repo_pack_id", on: "id_pack_id", unique: ""},
		ListIndexMaps{ixname: "ix_ix_type",         on: "index_type", unique: ""}},

	"packfiles": {
		ListIndexMaps{ixname: "ux_packf_idd", on: "packfile_id", unique: "UNIQUE"}},

	"meta_dir": {
		ListIndexMaps{ixname: "ux_meta_dir_snap_id_idd", on: "id_snap_id,id_idd", unique: "UNIQUE"},
		ListIndexMaps{ixname: "ix_meta_dir_idd",         on: "id_idd", unique: ""}},

	"idd_file": {
		ListIndexMaps{ixname: "ux_idd_file_blob_pos", on: "id_blob,position", unique: "UNIQUE"},
		ListIndexMaps{ixname: "ix_idd_file_name",     on: "id_name", unique: ""}},

	"snapshots": {
		ListIndexMaps{ixname: "ux_snaphshots_snap_id", on: "snap_id", unique: "UNIQUE"}},

	"names": {
		ListIndexMaps{ixname: "ux_names_name", on: "name", unique: "UNIQUE"}},

	"contents": {
		ListIndexMaps{ixname: "ux_cont_blob_pos_off", on: "id_blob,position,offset", unique: "UNIQUE"},
		ListIndexMaps{ixname: "ix_cont_data_idd",     on: "id_data_idd", unique: ""}},

	"snapshots_history": {
		ListIndexMaps{ixname: "ix_snaphist_snap_id", on: "snap_id", unique: ""},
		ListIndexMaps{ixname: "ix_snaphist_action",  on: "action", unique: ""}}}

type DBDescriptor struct {
	DB_ptr          *sqlx.DB
	verbose         int
	echo            bool
	table_names     []string
	table_names_map mapset.Set[string]
}

type TableIndex struct {
	Name     string
	Tbl_name string
}

var db_descriptor *DBDescriptor

// Printf writes the message to the configured stdout stream.
func Printf(format string, args ...interface{}) {
	_, err := fmt.Fprintf(os.Stdout, format, args...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to write to stdout: %v\n", err)
	}
}

func OpenDatabase(data_base_path string, echo bool, verbose int, index bool) (*sqlx.DB, error) {
	Printf("opening %s\n", data_base_path)
	db, err := sqlx.Open("sqlite3", data_base_path)
	if err != nil {
		Printf("sqlx.Open failed %v\n", err)
		return nil, err
	}

	// allocate DBDescriptor
	db_descriptor = &DBDescriptor{DB_ptr: db, verbose: verbose, echo: echo}

	sql := "SELECT tbl_name FROM sqlite_master WHERE type = 'table' ORDER BY tbl_name"
	db_descriptor.table_names = make([]string, 0)
	err = db.Select(&db_descriptor.table_names, sql)
	if err != nil {
		Printf("Get failed err %v\n", err)
		return nil, err
	}

	db_descriptor.table_names_map = mapset.NewSet[string]()
	for _, table_name := range db_descriptor.table_names {
		db_descriptor.table_names_map.Add(table_name)
	}

	//Printf("sqlite: calling init_tables\n")
	err = init_tables()
	if err != nil {
		Printf("error in init_tables %v\n", err)
		return nil, err
	}
	if index {
		err := build_index(echo)
		if err != nil {
			Printf("error in build_index %v\n", err)
			return nil, err
		}
	}

	err = build_triggers(echo)
	if err != nil {
		Printf("error in build_triggers %v\n", err)
		return nil, err
	}
	return db, nil
}

func init_tables() error {
	/*
	  Create and manage the following tables:
	  snapshots       - o2o straight from the snapshots json data
	  index_repo      - o2o idd, size, type, packid back pointer
			  - o2m for pack_id -> blob
	  packfile        - o2o between idd and pack_ID
	  meta_dir        - m2m between snap_id and idd
	  file_idd        - o2o idd, offset, ptr to name, ptr to contents, size
			  - o2m for blob ptr
	  names           - o2o id, name, name_type
	  contents        - o2o between contents composite ptr and contents elem
	  timestamp       - o2o info about changes to the real restic files and
				info about last balance check
	  snapshots_history - 02m history when snap was added to the system or
				  removed
	  timestamp_history - o2o all changes to the database are recorded here
				  with a timestamp
	*/
	//SQLite tables and INDEX definitions
	for table_name, init_string := range SQLITE_TABLES {
		if ok := db_descriptor.table_names_map.Contains(table_name); ok {
			continue
		}

		// create the table
		if db_descriptor.verbose > 0 {
			Printf("%s %s\n", time.Now().Format("2006-01-02 15:04:05.999"), init_string)
		}

		_, err := db_descriptor.DB_ptr.Exec(init_string)
		if err != nil {
			Printf("%q: %s\n", err, init_string)
			return err
		}
	}
	return nil
}

type ListIndexMaps struct {
	ixname string
	on     string
	unique string
}

func build_index(echo bool) error {

	result := make([]TableIndex, 0)
	sql1 := "SELECT name, tbl_name FROM sqlite_master WHERE type = 'index'"
	err := db_descriptor.DB_ptr.Select(&result, sql1)
	if err != nil {
		Printf("Select error on %s %v\n", sql1, err)
		return err
	}

	// build a map of index_names which point back to the owning table
	index_names  := make(map[string]string)
	indices_have := mapset.NewSet[string]()
	indices_want := mapset.NewSet[string]()

	// this loop is initially empty!! -- collect existing index names
	for _, row := range result {
		indices_have.Add(row.Name)
	}

	// need to consult SQLITE_INDEX about all indices wanted
	for table_name, action_list := range SQLITE_INDEX {
		for _, action := range action_list {
			// map index to table_name
			index_names[action.ixname] = table_name
			indices_want.Add(action.ixname)
		}
	}

	// which indices do we still need?
	for ixname := range indices_want.Difference(indices_have).Iter() {
		table_name, ok  := index_names[ixname]
		if !ok {
			Printf("No entry for %s in index_names\n", ixname)
			panic("index_name does not map back to table_name`")
		}

		//Printf("ix_name.new %-25s -> %s\n", ixname, table_name)
		for _, entry := range SQLITE_INDEX[table_name] {
			if indices_have.Contains(entry.ixname) {
				continue
			}
			sql1 = fmt.Sprintf("CREATE %-6s INDEX %s ON %s(%s)", entry.unique,
				entry.ixname, table_name, entry.on)
			if db_descriptor.verbose > 0 || echo {
				Printf("%s %s\n", time.Now().Format("2006-01-02 15:04:05.999"), sql1)
			}

			_, err := db_descriptor.DB_ptr.Exec(sql1)
			if err != nil {
				Printf("failed to CREATE INDEX. Error is %v\n", err)
				return err
			}
			indices_have.Add(entry.ixname)
		}
	}
	return nil
}

var tables_high_id map[string]int

func Get_all_high_ids() error {
	tables_high_id = make(map[string]int)

	sql1 := "SELECT tbl_name FROM sqlite_master WHERE type = 'table'"
	table_names := make([]string, 0)
	err := db_descriptor.DB_ptr.Select(&table_names, sql1)
	if err != nil {
		Printf("Get_all_high_ids.Select. Error in Select %v\n", err)
		return err
	}

	var max sql.NullInt64
	for _, tbl_name := range table_names {
		err := db_descriptor.DB_ptr.Get(&max, "SELECT max(id) from " + tbl_name)
		if err != nil {
			Printf("Get_all_high_ids.Query error for Get max(id) is %v\n", err)
			return err
		}
		if !max.Valid {
			tables_high_id[tbl_name] = 1
		} else {
			tables_high_id[tbl_name] = int(max.Int64) + 1
		}
	}
	return nil
}

func Get_high_id(tbl_name string) int {
	value, ok := tables_high_id[tbl_name]
	if !ok {
		Printf("table %s not found in tables_high_id\n", tbl_name)
		panic("Get_high_id invalid table_name!")
	}
	return value
}

var SQLITE_TRIGGERS = map[string]string {
"tr_snapshots_i": `CREATE TRIGGER tr_snapshots_i AFTER INSERT ON snapshots
	BEGIN
    INSERT INTO snapshots_history (timestamp, action, id_history,
                snap_id, snap_time, snap_host, snap_fsys, id_snap_root)
    VALUES (datetime('now','localtime'), 'INSERT', new.id, new.snap_id,
    new.snap_time, new.snap_host, new.snap_fsys, new.id_snap_root
        );
	END;`,

"tr_snapshots_d": `CREATE TRIGGER tr_snapshots_d AFTER DELETE ON snapshots
	BEGIN
    INSERT INTO snapshots_history (timestamp, action, id_history,
                snap_id, snap_time, snap_host, snap_fsys, id_snap_root)
    VALUES (datetime('now','localtime'), 'DELETE', old.id, old.snap_id,
        old.snap_time, old.snap_host, old.snap_fsys, old.id_snap_root);
	END;`,

"tr_timestamp_i": `CREATE TRIGGER tr_timestamp_i AFTER INSERT ON timestamp
	BEGIN
    INSERT INTO timestamp_history (timestamp, action, id_history,
                restic_updated, database_updated, ts_created)
    VALUES (datetime('now','localtime'), 'INSERT', new.id, new.restic_updated,
        new.database_updated, new.ts_created);
	END;`,

"tr_timestamp_u": `CREATE TRIGGER tr_timestamp_u AFTER UPDATE ON timestamp
	BEGIN
    INSERT INTO timestamp_history (timestamp, action, id_history,
                restic_updated, database_updated, ts_created)
    VALUES (datetime('now','localtime'), 'UPDATE', new.id, new.restic_updated,
        new.database_updated, new.ts_created);
	END;`}
func build_triggers(echo bool) error {
	// read from sqlite_master the existing triggers
	result := make([]string, 0)
	sql1 := "SELECT name FROM sqlite_master WHERE type = 'trigger'"
	err := db_descriptor.DB_ptr.Select(&result, sql1)
	if err != nil {
		Printf("Select error on %s %v\n", sql1, err)
		return err
	}

	// convert 'result' into a Set
	trigger_names := mapset.NewSet[string]()
	for _, trigger_name := range result {
		trigger_names.Add(trigger_name)
	}

	// compare against list of all triggers
	for trigger_name, trigger_action := range SQLITE_TRIGGERS {
		if trigger_names.Contains(trigger_name) {
			continue
		}

		// execute sql to CREATE TRIGGER
		if db_descriptor.verbose > 0 || echo {
			Printf("%s %s\n", time.Now().Format("2006-01-02 15:04:05.999"), trigger_action)
		}

		_, err := db_descriptor.DB_ptr.Exec(trigger_action)
		if err != nil {
			Printf("failed to CREATE TRIGGER. Error is %v\n", err)
			return err
		}
	}
	return nil
}
