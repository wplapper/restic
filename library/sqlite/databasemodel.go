package sqlite
/*
 * the databasemodel describes the tables used to interface with the restic
 * backup system.
 * When no tables are found, the tables below and their indexes are created.
 * For INSERT functions, the highest Ids of each of the tables are exposed
 */

import (
  "fmt"
  "os"
  "time"

  // sets
  "github.com/deckarep/golang-set/v2"

  // sqlite3 interface
  //"database/sql"
  "github.com/jmoiron/sqlx"
  _ "github.com/mattn/go-sqlite3"
)

type DBDescriptor struct {
  DB_ptr          *sqlx.DB
  verbose         int
  echo            bool
  table_names     []string
  table_names_set mapset.Set[string]
}

type ListIndexMaps struct {
  ixname string
  on     string
  unique string
}

var db_descriptor *DBDescriptor

var SQLITE_TABLES = map[string]string{
  // primary table - anchors for the backup system
"snapshots": `CREATE TABLE snapshots (
  id INTEGER PRIMARY KEY,             -- ID of table row
  snap_id VARCHAR(8) NOT NULL,        -- snap ID, UNIQUE INDEX
  snap_time VARCHAR(19) NOT NULL,     -- time of snap
  snap_host VARCHAR(50) NOT NULL,     -- host wich is to be backep up
  snap_fsys VARCHAR(100) NOT NULL,    -- filesystem to be backup up
  snap_root TEXT NOT NULL             --  the root of the snap
)`,

  // primary table - derived from master index
"index_repo": `CREATE TABLE index_repo (
  -- maintains the contents of the index/ * files
  id INTEGER PRIMARY KEY,             -- the primary key
  blob BLOB NOT NULL,                 -- the idd, UNIQUE INDEX, as []byte, len 32
  type TEXT NOT NULL,                 -- type tree / data
  offset INTEGER NOT NULL,            -- offset in packfile
  length INTEGER NOT NULL,            -- length of blob
  uncompressedlength INTEGER NOT NULL,-- uncompressed length
  pack__id INTEGER NOT NULL,          -- back ptr to packflies, INDEX
  FOREIGN KEY(pack__id)               REFERENCES packfiles(id)
)`,

  // primary, because of back reference from 'index_repo' to 'packfiles'
"packfiles": `CREATE TABLE packfiles (
  -- needed for the relationship between packfiles and blobs
  id INTEGER PRIMARY KEY,  -- the primary key
  packfile_id BLOB         -- the packfile ID, UNIQUE INDEX, as []byte, len 32
)`,

  // primary table - contains all meta data about files, directories etc.
"idd_file": `CREATE TABLE idd_file (
  id INTEGER PRIMARY KEY,             -- the primary key
  blob__id INTEGER NOT NULL,          -- ptr to blob table
  position INTEGER NOT NULL,          -- offset in idd_file
  -- the tuple (blob__id, position) is UNIQUE INDEX
  name__id INTEGER NOT NULL,          -- ptr to name table, INDEX
  size INTEGER NOT NULL,              -- size of file or 0 for directories
  inode INTEGER NOT NULL,             -- inode number in filesystem
  mtime TEXT,                         -- mtime of file
  type TEXT,                          -- type of entry (d/f/l)
  FOREIGN KEY(blob__id)               REFERENCES index_repo(id),
  FOREIGN KEY(name__id)               REFERENCES names(id)
)`,

"names": `CREATE TABLE names (
  id INTEGER PRIMARY KEY,             -- the primary key
  name TEXT                           -- all names collected from restic system
)`,

"meta_dir": `CREATE TABLE meta_dir (
  -- many to many relationship table between snaps and directory idds
  id INTEGER PRIMARY KEY,             -- the primary key
  snap__id INTEGER NOT NULL,          -- the snap_id , INDEX
  blob__id INTEGER NOT NULL,          -- the idd pointer, INDEX
  -- the tuple (id_snap_id, id_idd) is UNIQUE INDEX
  FOREIGN KEY(snap__id)               REFERENCES snaphots(id),
  FOREIGN KEY(blob__id)               REFERENCES index_repo(id)
)`,

"contents": `CREATE TABLE contents (
  id INTEGER PRIMARY KEY,             -- the primary key
  data__id INTEGER NOT NULL,          -- ptr to data_idd, INDEX
  blob__id  INTEGER NOT NULL,         -- ptr to idd_file, needs INDEX
  position INTEGER NOT NULL,          -- position in idd_file
  offset   INTEGER NOT NULL,          -- the offset of the contents list
  -- the triple (id_blob, position, offset) is a UNIQUE INDEX
  FOREIGN KEY(data__id)               REFERENCES index_repo(id),
  FOREIGN KEY(blob__id)               REFERENCES index_repo(id)
)`,

"fullname": `CREATE TABLE fullname (
    id INTEGER PRIMARY KEY,           -- the primary key, GENERIC ascending
    pathname TEXT NOT NULL            -- full pathname of directory UNIQUE INDEX
)`,

"dir_path_id": `CREATE TABLE dir_path_id (
    id INTEGER PRIMARY KEY,           -- the primary key
    pathname__id INTEGER NOT NULL,    -- ptr to "fullname", INDEX7
    FOREIGN KEY(pathname__id)         REFERENCES fullname(id),
    FOREIGN KEY(id)                   REFERENCES index_repo(id)
)`,
}

var SQLITE_INDEX = map[string][]ListIndexMaps{
  "snapshots": {
    ListIndexMaps{ixname: "ux_snaphshots_snap_id",   on: "snap_id", unique: "UNIQUE"}},

  "index_repo": {
    ListIndexMaps{ixname: "ux_ix_blob",              on: "blob", unique: "UNIQUE"},
    ListIndexMaps{ixname: "ix_ix_pack",              on: "pack__id", unique: ""},
    ListIndexMaps{ixname: "ix_ix_type",              on: "type", unique: ""}},

  "packfiles": {
    ListIndexMaps{ixname: "ux_packfile",             on: "packfile_id", unique: "UNIQUE"}},

  "names": {
    ListIndexMaps{ixname: "ux_names_name",           on: "name", unique: "UNIQUE"}},

  "meta_dir": {
    ListIndexMaps{ixname: "ux_metadir_snap_blob",    on: "snap__id, blob__id", unique: "UNIQUE"},
    ListIndexMaps{ixname: "ix_metadir_blob",         on: "blob__id", unique: ""}},

  "idd_file": {
    ListIndexMaps{ixname: "ux_iddfile_blob_pos",     on: "blob__id, position", unique: "UNIQUE"},
    ListIndexMaps{ixname: "ix_iddfile_name",         on: "name__id", unique: ""}},

  "contents": {
    ListIndexMaps{ixname: "ux_cont_blob_pos_off",    on: "blob__id, position, offset", unique: "UNIQUE"},
    ListIndexMaps{ixname: "ix_cont_data_id",         on: "data__id", unique: ""}},

  "fullname": {
    ListIndexMaps{ixname: "ux_fname_path",           on: "pathname", unique: "UNIQUE"}},

  "dir_path_id": {
    ListIndexMaps{ixname: "ix_dpath_path",           on: "pathname__id", unique: ""}},
}

// Printf writes the message to the configured stdout stream.
func Printf(format string, args... interface{}) {
  _, err := fmt.Fprintf(os.Stdout, format, args...)
  if err != nil {
    fmt.Fprintf(os.Stderr, "unable to write to stdout: %v\n", err)
  }
}

func OpenDatabase(data_base_path string, echo bool, verbose int, index bool) (*sqlx.DB, error) {

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

  db_descriptor.table_names_set = mapset.NewSet[string](db_descriptor.table_names ...)
  init_tables(echo)
  build_index(echo)
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
  */
  //SQLite tables and INDEX definitions
  for table_name, init_string := range SQLITE_TABLES {
    if ok := db_descriptor.table_names_set.Contains(table_name); ok {
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
  index_names := []string{}
  db_descriptor.DB_ptr.Select(&index_names, sql1)
  // and convert Slice to Set
  index_set := mapset.NewSet[string](index_names ...)

  // action
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
// collect highest Id for each TABLE from the Id column
func Get_all_high_ids() error {
  tables_high_id = map[string]int{}

  sql1 := "SELECT tbl_name FROM sqlite_master WHERE type = 'table'"
  table_names := make([]string, 0)
  err := db_descriptor.DB_ptr.Select(&table_names, sql1)
  if err != nil {
    Printf("Get_all_high_ids.Select. Error in Select %v\n", err)
    return err
  }

  var max int
  for _, tbl_name := range table_names {
    err := db_descriptor.DB_ptr.Get(&max, "SELECT coalesce(max(id), 0) from " + tbl_name)
    if err != nil {
      Printf("Get_all_high_ids.Query error for Get max(id) is %v\n", err)
      return err
    }
    tables_high_id[tbl_name] = max + 1
  }
  return nil
}

// return highest Id from a given input table
// panics if wrong table name given
func Get_high_id(tbl_name string) int {
  value, ok := tables_high_id[tbl_name]
  if !ok {
    Printf("table %s not found in tables_high_id\n", tbl_name)
    panic("Get_high_id: invalid table_name!")
  }
  return value
}
