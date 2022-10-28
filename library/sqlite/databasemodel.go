package sqlite

import (
	"fmt"
  "database/sql"
  "log"
  "os"
  "time"
  //"reflect"

  // sets
  "github.com/deckarep/golang-set"

  // sqlite3 interface
  _ "github.com/mattn/go-sqlite3"
)

var SQLITE_TABLES = map[string]string {
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
  name TEXT,                      -- all names collected from restic system
  -- name is UNIQUE INDEX
  name_type TEXT --one of b/d/p=basename,dirname,fullpath, INDEX
  )`,

"fullpath2": `CREATE TABLE fullpath2 (
  id      INTEGER PRIMARY KEY,   -- the primary key
  id_blob INTEGER NOT NULL,      -- ptr to index_repo, UNIQUE INDEX
  id_name INTEGER NOT NULL,      -- ptr to names, INDEX
  FOREIGN KEY(id_blob) REFERENCES index_repo(id),
  FOREIGN KEY(id_name) REFERENCES names(id)
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
  id_fullpath INTEGER NOT NULL,   -- reference to names.id
  FOREIGN KEY(id_data_idd) REFERENCES index_repo(id),
  FOREIGN KEY(id_blob) REFERENCES     index_repo(id),
  FOREIGN KEY(id_fullpath) REFERENCES names(id)
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
  "fullpath2":  {ListIndexMaps{ixname: "ux_fp2_blob",           on: "id_blob",      unique: "UNIQUE"},
                 ListIndexMaps{ixname: "ix_fp2_name",           on: "id_name",      unique: ""},},

  "index_repo": {ListIndexMaps{ixname: "ux_ix_repo_idd",        on: "idd",          unique: "UNIQUE"},
                 ListIndexMaps{ixname: "ix_ix_repo_pack_id",    on: "id_blob",      unique: ""},
                 ListIndexMaps{ixname: "ix_ix_type",            on: "index_type",   unique: ""},},

  "packfiles":  {ListIndexMaps{ixname: "ux_packf_idd",          on: "idd",          unique: "UNIQUE"},},

  "meta_dir":   {ListIndexMaps{ixname: "ux_meta_dir_snap_id_idd", on: "id_snap_id,id_idd", unique: "UNIQUE"},
                 ListIndexMaps{ixname: "ix_meta_dir_idd",       on: "id_idd",       unique: ""},},

  "idd_file":   {ListIndexMaps{ixname: "ux_idd_file_blob_pos",  on: "id_blob,position", unique: "UNIQUE"},
                 ListIndexMaps{ixname: "ix_idd_file_name",      on: "id_name",     unique: ""},},


  "snapshots":  {ListIndexMaps{ixname: "ux_snaphshots_snap_id", on: "snap_id",      unique: "UNIQUE"},},

  "names":      {ListIndexMaps{ixname: "ux_names_name",         on: "name",         unique: "UNIQUE"},
                 ListIndexMaps{ixname: "ix_names_type",         on: "name_type",    unique: ""},},

  "contents":   {ListIndexMaps{ixname: "ux_cont_blob_pos_off",  on: "id_blob,position,offset", unique: "UNIQUE"},
                 ListIndexMaps{ixname: "ix_cont_fpath",         on: "id_fullpath",  unique: ""},
                 ListIndexMaps{ixname: "ix_cont_data_idd",      on: "id_data_idd",  unique: ""},},

  "snapshots_history":
                {ListIndexMaps{ixname: "ix_snaphist_snap_id",   on: "snap_id",      unique: ""},
                 ListIndexMaps{ixname: "ix_snaphist_action",    on: "action",       unique: ""},},}


type DBDescriptor struct {
  DB_ptr          *sql.DB
  verbose         int
  echo            bool
  table_names     []string
  table_names_map map[string]struct{}
}
var db_descriptor *DBDescriptor

// Printf writes the message to the configured stdout stream.
func Printf(format string, args... interface{}) {
  _, err := fmt.Fprintf(os.Stdout, format, args...)
  if err != nil {
    fmt.Fprintf(os.Stderr, "unable to write to stdout: %v\n", err)
  }
}

func OpenDatabase(data_base_path string, echo bool, verbose int, index bool) (*sql.DB, error) {
  Printf("opening %s\n", data_base_path)
  db, err := sql.Open("sqlite3", data_base_path)
  if err != nil {
    return nil, err
  }
  //Printf("sqlite.DB open\n")

  // allocate DBDescriptor
  db_descriptor = &DBDescriptor{DB_ptr: db, verbose: verbose, echo: echo}
  // return open DB descriptor
  //defer db.Close()

  sql := "SELECT tbl_name FROM sqlite_master WHERE type = 'table' ORDER BY tbl_name"
  rows, err := db.Query(sql)
  if err != nil {
    Printf("Query error is %v\n", err)
    return nil, err
  }

  // gather the table names
  Table_names := make([]string, 0)

  for rows.Next() {
      var tbl_name string
      err = rows.Scan(&tbl_name)

      if err != nil {
          log.Fatal(err)
      }
      Table_names = append(Table_names, tbl_name)
  }
  rows.Close()

  table_names_map := make(map[string]struct{}, len(Table_names))
  for _, table_name := range Table_names {
    table_names_map[table_name] = struct{}{}
  }
  db_descriptor.table_names_map = table_names_map

  //Printf("sqlite: calling init_tables\n")
  err = init_tables()
  if err != nil {
      log.Fatal(err)
  }
  if index {
    err := build_index()
    if err != nil {
        log.Fatal(err)
    }
  }
  //Printf("sqlite: returning to caller\n")
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
  fullpath2       - o2o between names and directory blobs
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
    _, ok := db_descriptor.table_names_map[table_name]
    if ok {
      continue
    }

    // create the table
    if db_descriptor.verbose > 0 {
      Printf("%s %s\n", time.Now().Format("2006-01-02 15:04:05.999"), init_string)
    }

    _, err := db_descriptor.DB_ptr.Exec(init_string)
    if err != nil {
      log.Printf("%q: %s\n", err, init_string)
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

func build_index() error {
  //Printf("sqlite:build_index\n")
  sql := "SELECT name, tbl_name FROM sqlite_master WHERE type = 'index'"
  rows, err := db_descriptor.DB_ptr.Query(sql)
  if err != nil {
    log.Printf("%q: %s\n", err, sql)
    return err
  }
  defer rows.Close()

  // build a map of index_names which point back to the owning table
  index_names  := make(map[string]string)
  indices_have := mapset.NewSet()
  indices_want := mapset.NewSet()
  for rows.Next() {
    var index_name string
    var table_name string
    err = rows.Scan(&index_name, &table_name)
    if err != nil {
        log.Fatal(err)
    }
    index_names[index_name] = table_name
    indices_have.Add(index_name)
  }
  rows.Close()

  // check the indices we want
  for _, action_list := range SQLITE_INDEX {
    for _, action := range action_list {
      indices_want.Add(action.ixname)
    }
  }

  // which indices do we still need?
  for ixname := range indices_want.Difference(indices_have).Iter() {
    ixname_conf := ixname.(string)
    table_name  := index_names[ixname_conf]
    for _,entry := range SQLITE_INDEX[table_name] {
      if indices_have.Contains(entry.ixname) {
        continue
      }

      // need to build sql string
      sql = fmt.Sprintf("CREATE %-6s INDEX %s ON %s", entry.unique, entry.ixname, table_name)
      if db_descriptor.verbose > 0 {
        Printf("%s %s\n", time.Now().Format("2006-01-02 15:04:05.999"), sql)
      }

      _, err := db_descriptor.DB_ptr.Exec(sql)
      if err != nil {
          log.Fatal(err)
      }
      indices_have.Add(entry.ixname)
    }
  }
  //Printf("sqlite:build_index ended\n")
  return nil
}
