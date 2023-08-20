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

type ListIndexMaps struct {
  ixname string
  on     string
  unique string
}

var db_descriptor *DBDescriptor

var SQLITE_TABLES = map[string]string{
  // primary table
  "snapshots": `CREATE TABLE snapshots (
  id INTEGER PRIMARY KEY,             -- ID of table row
  snap_id VARCHAR(8) NOT NULL,        -- snap ID, UNIQUE INDEX
  snap_time VARCHAR(19) NOT NULL,     -- time of snap
  snap_host VARCHAR(50) NOT NULL,     -- host wich is to be backep up
  snap_fsys VARCHAR(100) NOT NULL,    -- filesystem to be backup up
  id_snap_root TEXT NOT NULL       --  the root of the snap
  )`,

  // primary tables
  "index_repo": `CREATE TABLE index_repo (
  -- maintains the contents of the index/ * files
  id INTEGER PRIMARY KEY,           -- the primary key
  idd VARCHAR(64) NOT NULL,         -- the idd, UNIQUE INDEX
  idd_size INTEGER NOT NULL,        -- idd size
  index_type VARCHAR(4) NOT NULL,   -- type tree / data (might need INDEX)
  id_pack_id INTEGER NOT NULL,      -- pack_id (needs INDEX)
  FOREIGN KEY(id_pack_id) REFERENCES packfiles(id)
  )`,

  // more scondary table
  "names": `CREATE TABLE names (
  id INTEGER PRIMARY KEY,         -- the primary key
  name TEXT                       -- all names collected from restic system
  )`,

  // semi primary, because of back reference from 'index_repo' to 'packfiles'
  "packfiles": `CREATE TABLE packfiles (
  -- needed for the relationship between packfiles and blobs
  id INTEGER PRIMARY KEY,         -- the primary key
  packfile_id TEXT                -- the packfile ID, UNIQUE INDEX
  )`,

  //secondary, can be easily rebuilt
  "meta_dir": `CREATE TABLE meta_dir (
  -- many to many relationship table between snaps and directory idds
  id INTEGER PRIMARY KEY,           -- the primary key
  id_snap_id INTEGER NOT NULL,      -- the snap_id , INDEX
  id_idd INTEGER NOT NULL,          -- the idd pointer, INDEX
  -- the tuple (id_snap_id, id_idd) is UNIQUE INDEX
  FOREIGN KEY(id_snap_id)   REFERENCES snaphots(id),
  FOREIGN KEY(id_idd)       REFERENCES index_repo(id)
  )`,

  // primary table
  "idd_file": `CREATE TABLE idd_file (
  id INTEGER PRIMARY KEY,         -- the primary key
  id_blob INTEGER NOT NULL,       -- ptr to blob table
  position INTEGER NOT NULL,      -- offset in idd_file
  -- the tuple (id_blob, position) is UNIQUE INDEX
  id_name INTEGER NOT NULL,       -- ptr to name table, INDEX
  size INTEGER NOT NULL,          -- size of file or 0 for directories
  inode INTEGER NOT NULL,         -- inode number in filesystem
  mtime TEXT,                     -- mtime of file
  type TEXT,                      -- type of entry (d/f/l)
  FOREIGN KEY(id_blob) REFERENCES index_repo(id),
  FOREIGN KEY(id_name) REFERENCES names(id)
  )`,

  // internal use
  "timestamp": `CREATE TABLE timestamp (
  id INTEGER PRIMARY KEY,                 -- the primary key
  restic_updated   TIMESTAMP NOT NULL,    --changes when restic gets updated
  database_updated TIMESTAMP NOT NULL,    --last update of database
  ts_created       TIMESTAMP NOT NULL     --creation date of database
  )`,

  //secondary table, can be easily rebuilt
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


  //secondary table, can be easily rebuilt
  "fullname": `CREATE TABLE fullname (
    id INTEGER PRIMARY KEY,         -- the primary key, GENERIC ascending
    pathname TEXT NOT NULL          -- full pathname of directory UNIQUE INDEX
  )`,

  //secondary table, can be easily rebuilt
  "dir_path_id": `CREATE TABLE dir_path_id (
    id INTEGER PRIMARY KEY,         -- the primary key
    id_pathname INTEGER NOT NULL,   -- ptr to "fullname", INDEX
    FOREIGN KEY(id_pathname) REFERENCES fullname(id),
    FOREIGN KEY(id)          REFERENCES index_repo(id)
  )`,

  // history section - internal use
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


  // history section - internal use
  "timestamp_history": `CREATE TABLE timestamp_history (
  id INTEGER PRIMARY KEY,           -- ID of table row
  timestamp TIMESTAMP NOT NULL,
  action    TEXT NOT NULL,          -- action type (INSERT, UPDATE, DELETE)
  id_history INTEGER NOT NULL,      -- ID of snapshot table
  restic_updated    TIMESTAMP NOT NULL,  --change when restic gets updated
  database_updated  TIMESTAMP NOT NULL,  --last update of database
  ts_created        TIMESTAMP NOT NULL   --creation date of database
  )`,
}

var SQLITE_INDEX = map[string][]ListIndexMaps{
  "snapshots": {
    ListIndexMaps{ixname: "ux_snaphshots_snap_id",   on: "snap_id", unique: "UNIQUE"}},

  "index_repo": {
    ListIndexMaps{ixname: "ux_ix_repo_idd",          on: "idd", unique: "UNIQUE"},
    ListIndexMaps{ixname: "ix_ix_repo_pack_id",      on: "id_pack_id", unique: ""},
    ListIndexMaps{ixname: "ix_ix_type",              on: "index_type", unique: ""}},

  "packfiles": {
    ListIndexMaps{ixname: "ux_packf_idd",            on: "packfile_id", unique: "UNIQUE"}},

  "names": {
    ListIndexMaps{ixname: "ux_names_name",           on: "name", unique: "UNIQUE"}},

  "meta_dir": {
    ListIndexMaps{ixname: "ux_meta_dir_snap_id_idd", on: "id_snap_id,id_idd", unique: "UNIQUE"},
    ListIndexMaps{ixname: "ix_meta_dir_idd",         on: "id_idd", unique: ""}},

  "idd_file": {
    ListIndexMaps{ixname: "ux_idd_file_blob_pos",    on: "id_blob,position", unique: "UNIQUE"},
    ListIndexMaps{ixname: "ix_idd_file_name",        on: "id_name", unique: ""}},

  "contents": {
    ListIndexMaps{ixname: "ux_cont_blob_pos_off",    on: "id_blob,position,offset", unique: "UNIQUE"},
    ListIndexMaps{ixname: "ix_cont_data_idd",        on: "id_data_idd", unique: ""}},

  "snapshots_history": {
    ListIndexMaps{ixname: "ix_snaphist_snap_id",     on: "snap_id", unique: ""},
    ListIndexMaps{ixname: "ix_snaphist_action",      on: "action",  unique: ""}},

  "fullname": {
    ListIndexMaps{ixname: "ux_fname_path",           on: "pathname", unique: "UNIQUE"}},

  "dir_path_id": {
    ListIndexMaps{ixname: "ix_dpath_path",           on: "id_pathname", unique: ""}},
}

// Printf writes the message to the configured stdout stream.
func Printf(format string, args... interface{}) {
  _, err := fmt.Fprintf(os.Stdout, format, args...)
  if err != nil {
    fmt.Fprintf(os.Stderr, "unable to write to stdout: %v\n", err)
  }
}

func OpenDatabase(data_base_path string, echo bool, verbose int, index bool) (*sqlx.DB, error) {
  //Printf("opening %s\n", data_base_path)

  var err error
  db := sqlx.MustOpen("sqlite3", data_base_path)
  // allocate DBDescriptor
  db_descriptor = &DBDescriptor{DB_ptr: db, verbose: verbose, echo: echo}

  sql := "SELECT tbl_name FROM sqlite_master WHERE type = 'table' ORDER BY tbl_name"
  db_descriptor.table_names = make([]string, 0)
  err = db.Select(&db_descriptor.table_names, sql)
  if err != nil {
    Printf("Get failed err %v\n", err)
    return nil, err
  }

  db_descriptor.table_names_map = mapset.NewSet[string](db_descriptor.table_names ...)
  init_tables(echo)

  if index {
    build_index(echo)
  }

  build_triggers(echo)
  return db, nil
}

func init_tables(echo bool) {
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
    if db_descriptor.verbose > 0 || echo {
      Printf("%s %s\n", time.Now().Format("2006-01-02 15:04:05.000"), init_string)
    }

    db_descriptor.DB_ptr.MustExec(init_string)
  }
}

// build all INDEXes which do not exist yet: use CREATE INDEX IF NOT EXISTS
func build_index(echo bool) {
  // gather existing INDEX names
  sql1 := "SELECT name FROM sqlite_master WHERE type = 'index'"
  index_names := make([]string, 0)
  db_descriptor.DB_ptr.Select(&index_names, sql1)
  index_set := mapset.NewSet[string](index_names ...)
  for table_name, action_list := range SQLITE_INDEX {
    for _, entry := range action_list {
      sql1 = fmt.Sprintf("CREATE %-6s INDEX IF NOT EXISTS %s ON %s(%s)", entry.unique,
        entry.ixname, table_name, entry.on)
      if ! index_set.Contains(entry.ixname) && (db_descriptor.verbose > 0 || echo) {
        Printf("%s %s\n", time.Now().Format("2006-01-02 15:04:05.000"), sql1)
      }
      db_descriptor.DB_ptr.MustExec(sql1)
    }
  }
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
    panic("Get_high_id: invalid table_name!")
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
  END;`,
}

func build_triggers(echo bool)  {
  // read from sqlite_master the existing triggers
  tr_names := make([]string, 0)
  sql1 := "SELECT name FROM sqlite_master WHERE type = 'trigger'"
  err := db_descriptor.DB_ptr.Select(&tr_names, sql1)
  if err != nil {
    Printf("Select error on %s %v\n", sql1, err)
    return
  }

  // convert 'tr_names' into a Set
  trigger_names := mapset.NewSet[string](tr_names...)

  // compare against list of all triggers
  for trigger_name, trigger_action := range SQLITE_TRIGGERS {
    if trigger_names.Contains(trigger_name) {
      continue
    }

    // execute sql to CREATE TRIGGER
    if db_descriptor.verbose > 0 || echo {
      Printf("%s %s\n", time.Now().Format("2006-01-02 15:04:05.000"), trigger_action)
    }

    db_descriptor.DB_ptr.MustExec(trigger_action)
  }
}
