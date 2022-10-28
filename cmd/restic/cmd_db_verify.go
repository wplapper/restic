package main
// compile with "go run build.go -tags debug""
// run with DEBUG_LOG=/home/wplapper/restic/debug.log fully-qualified-name/restic -r <repo> overview

// ref to onedrive: rclone:onedrive:restic_backups

import (
	// system
	"time"
	"sort"
  "database/sql"

	//"log"

	//argparse
	"github.com/spf13/cobra"

  // sets
  "github.com/deckarep/golang-set"

	// restic library
	"github.com/wplapper/restic/library/restic"
	"github.com/wplapper/restic/library/sqlite"
)

type DBOptions struct {
	echo bool
	print_count_tables bool
	altDB string
}
var dbOptions DBOptions

type DBSnapshotRecord struct {
	snap_time string
	snap_host string
	snap_fsys string
	root 			restic.ID
}

type DBIndexRepoRecord struct {
	idd 				restic.ID
	idd_size 		int
	index_type 	string
	id_pack_id	int // ptr to packfiles table, no validation here
}

type DBAggregate struct {
	db_conn *sql.DB
	table_counts map[string]int
}

// map repo to database
var DATABASE_NAMES = map[string]string {
	// master
	"/media/mount-points/Backup-ext4-Mate/restic_master":
		"/media/mount-points/home/wplapper/restic/db/restic-master_nfs.db",
	"/media/mount-points/Backup-ext4-Mate/restic_master/":
		"/media/mount-points/home/wplapper/restic/db/restic-master_nfs.db",

	// onedrive
	"rclone:onedrive:restic_backups":
		"/media/mount-points/home/wplapper/restic/db/restic-onedrive.db",

	// most
	"/media/mount-points/Backup-ext4-Mate/restic_most":
		"/media/mount-points/home/wplapper/restic/db/restic-most_nfs.db",
	"/media/mount-points/Backup-ext4-Mate/restic_most/":
		"/media/mount-points/home/wplapper/restic/db/restic-most_nfs.db",

	// data
	"/media/wplapper/internal-fast/restic_Data":
		"/home/wplapper/restic/db/XPS-restic-data_nfs.db",
	"/media/wplapper/internal-fast/restic_Data/":
		"/home/wplapper/restic/db/XPS-restic-data_nfs.db",

	// test
	"/media/wplapper/internal-fast/restic_test":
		"/home/wplapper/restic/db/XPS-restic-test.db",
	"/media/wplapper/internal-fast/restic_test/":
		"/home/wplapper/restic/db/XPS-restic-test.db",}

var cmdDBVerify = &cobra.Command{
	Use:   "db_verify [flags]",
	Short: "verify SQLite database with repository",
	Long: `gverify SQLite database with repository.

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
	DisableAutoGenTag: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runDBVerify(globalOptions, args)
	},
}

func init() {
	cmdRoot.AddCommand(cmdDBVerify)
	flags := cmdDBVerify.Flags()
	flags.BoolVarP(&dbOptions.echo, "echo", "E", false, "echo database operations to stdout")
}

func runDBVerify(gopts GlobalOptions, args []string) error {

	var db_aggregate DBAggregate
	// step 0: setup global stuff
	start := time.Now()
	_ = start
	repositoryData := init_repositoryData()
	EMPTY_NODE_ID = restic.Hash([]byte("{\"nodes\":[]}\n"))
	gOptions = gopts

	// step 1: open repository
	repo, err := OpenRepository(gopts)
	if err != nil {
		return err
	}
	Printf("Repository is %s\n", gopts.Repo)
	//timeMessage("%-30s %10.1f seconds\n", "open repository", time.Now().Sub(start).Seconds())

	// step 2: manage Index Records
	start = time.Now()
	if err = HandleIndexRecords(gopts, repo, repositoryData); err != nil {
		return err
	}
	//timeMessage("%-30s %10.1f seconds\n", "read index records", time.Now().Sub(start).Seconds())

	// extract all information about indexes from a call to <master_index>.Each()
	// setup index_handle, blob_to_index and index_to_blob
	// step 3: collect all snapshot related information
	start = time.Now()
	GatherAllRepoData(gopts, repo, repositoryData)
	//timeMessage("%-30s %10.1f seconds\n", "GatherAllRepoData (sum)", time.Now().Sub(start).Seconds())

	// step 4: open database which beongs to the repository
	db_name, ok := DATABASE_NAMES[gopts.Repo]
	if !ok {
		Printf("database name for repo %s is missing!\n", gopts.Repo)
		return nil
	}

	db_conn, err := sqlite.OpenDatabase(db_name, true, 1, true)
	if err != nil {
		Printf("db_verify: OpenDatabase failed, error is %v\n", err)
		return err
	}
	db_aggregate.db_conn = db_conn

	names_and_counts := make(map[string]int)
	err = readAllTablesAndCounts(db_conn, names_and_counts)
	if err != nil {
		Printf("readAllTablesAndCounts error is %v\n", err)
		return err
	}
	db_aggregate.table_counts = names_and_counts

	// sort names
	tbl_names_sorted := make([]string, len(names_and_counts))
	ix := 0
	for tbl_name := range names_and_counts {
		tbl_names_sorted[ix] = tbl_name
		ix++
	}
	sort.Strings(tbl_names_sorted)
	// print table counts

	for _, tbl_name := range tbl_names_sorted {
		count := names_and_counts[tbl_name]
		Printf("%-25s %8d\n", tbl_name, count)
	}

	// read snapshot table
	db_snapshots, err := ReadSnapshotTable(db_conn, db_aggregate)
	if err != nil {
		return err
	}

	// compare snapshots
	equal := check_db_snapshots(db_snapshots, repositoryData.snaps)
	if equal {
		Printf("database snapshots compares equal\n")
	} else {
		Printf("database snapshots mismatch!\n")
	}

	// read index_repo
	db_index_repo, err := ReadIndexRepoTable(db_conn, db_aggregate)
	if err != nil {
		return err
	}
	equal = check_db_index_repo(db_index_repo, db_aggregate, repositoryData)
	if equal {
		Printf("database index_repo compares equal\n")
	} else {
		Printf("database index_repo mismatch!\n")
	}
	return nil
}

// load the database rows for table snapshots into memory
func ReadSnapshotTable(db_conn *sql.DB,
db_aggregate DBAggregate) (map[string]DBSnapshotRecord, error) {

	// store results in db_snapshots
	db_snapshots := make(map[string]DBSnapshotRecord, db_aggregate.table_counts["snapshots"])
	rows, err := db_conn.Query("SELECT * FROM snapshots")
	if err != nil {
		Printf("db_verify:query snapshots err %v\n", err)
		return nil, err
	}
	defer rows.Close()
	//Printf("db_verify:issued Query\n")

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
		var id 				int
		var snap_id 	string
		var snap_time string
		var snap_host string
		var snap_fsys string
		var root 			string

		err = rows.Scan(&id, &snap_id, &snap_time, &snap_host, &snap_fsys, &root)
		if err != nil {
			Printf("db_verify:query failed %v\n", err)
			return nil, err
		}

		// convert timestamp
		date_time, err := time.Parse("2006-01-02 15:04:05", snap_time)
		if err != nil {
			Printf("date Parse %v\n", err)
			return nil, err
		}

		// convert root to ID
		idd, err := restic.ParseID(root)
		if err != nil {
			Printf("Parse failed %v\n", err)
			return nil, err
		}

		db_snapshots[snap_id] = DBSnapshotRecord{snap_time: date_time.String()[:19],
			snap_host: snap_host, snap_fsys: snap_fsys, root: idd}
	}
	rows.Close()
	return db_snapshots, nil
}

func check_db_snapshots(db_snapshots map[string]DBSnapshotRecord,
snaps []*restic.Snapshot) bool {
	// compare snapshots from repo with snapshots stored in the database
	// step 1: build memory table to allow the comparison

	set_db_keys := mapset.NewSet()
	set_mem_keys := mapset.NewSet()
	mem_snapshots := make(map[string]DBSnapshotRecord, len(snaps))
	for _, sn := range snaps {
		mem_snapshots[sn.ID().Str()] = DBSnapshotRecord{snap_time: sn.Time.String()[:19],
			snap_host: sn.Hostname, snap_fsys: sn.Paths[0], root: *sn.Tree}
		set_mem_keys.Add(sn.ID().Str())
	}

	// build sets for the keys to compare
	for key := range db_snapshots {
		set_db_keys.Add(key)
	}

	// compare keys being equal
	keys_equal := set_mem_keys.Equal(set_db_keys)
	if !keys_equal {
		diff := mapset.NewSet()
		tab := ""
		len_db_set  := set_db_keys.Cardinality()
		len_mem_set := set_mem_keys.Cardinality()
		Printf("keys don't match\n")
		Printf("db_snapshots  %5d\n", len_db_set)
		Printf("mem_snapshots %5d\n", len_mem_set)
		if len_mem_set > len_db_set {
			diff = set_mem_keys.Difference(set_db_keys)
			tab = "db "
		} else if len_db_set > len_mem_set {
			diff = set_db_keys.Difference(set_mem_keys)
			tab = "mem"
		}
		for snap_id := range diff.Iter() {
			Printf("%s missing %s\n", tab, snap_id.(string))
		}
		return keys_equal
	}

	// compare values
	compare_equals := true
	for db_key, db_value := range db_snapshots {
		mem_value := mem_snapshots[db_key]
		if db_value != mem_value {
			Printf("db  %s %s %s %s\n", db_value.snap_time, db_value.snap_host, db_value.snap_fsys, db_value.root)
			Printf("mem %s %s %s %s\n", mem_value.snap_time, mem_value.snap_host, mem_value.snap_fsys, mem_value.root)
			compare_equals = false
		}
	}
	return compare_equals
}

func readAllTablesAndCounts(db_conn *sql.DB, table_counts map[string]int) error {
	// get table names
  sql := "SELECT tbl_name FROM sqlite_master WHERE type = 'table'"
  rows, err := db_conn.Query(sql)
  if err != nil {
    Printf("Query error is %v\n", err)
    return err
  }

	// get tabe names
  for rows.Next() {
		var tbl_name string
		err = rows.Scan(&tbl_name)
		if err != nil {
			Printf("Scan error is %v\n", err)
			return err
		}
		table_counts[tbl_name] = 0
  }
  rows.Close()

  // get counts per table
  for tbl_name := range table_counts {
		sql := "SELECT count(*) FROM " + tbl_name
		row, err := db_conn.Query(sql)
		if err != nil {
			Printf("Query error for %s is %v\n", sql, err)
			return err
		}
		row.Next()
		var count int
		err = row.Scan(&count)
		if err != nil {
			Printf("Scan error is %v\n", err)
			return err
		}
		table_counts[tbl_name] = count
	}
	return nil
}

// load the database rows for table index_repo into memory
func ReadIndexRepoTable(db_conn *sql.DB,
db_aggregate DBAggregate) (map[restic.ID]DBIndexRepoRecord, error) {

	// store results in db_index_repo
	db_index_repo := make(map[restic.ID]DBIndexRepoRecord, db_aggregate.table_counts["index_repo"])
	rows, err := db_conn.Query("SELECT * FROM index_repo")
	if err != nil {
		Printf("db_verify:query index_repo err %v\n", err)
		return nil, err
	}

	// collect rows
	for rows.Next() {
		/* columns are:
			id INTEGER PRIMARY KEY,           -- the primary key
			idd VARCHAR(64) NOT NULL,         -- the idd, UNIQUE INDEX
			idd_size INTEGER NOT NULL,        -- idd size
			index_type VARCHAR(4) NOT NULL,   -- type tree / data (might need INDEX)
			id_pack_id INTEGER NOT NULL,      -- pack_id (needs INDEX)
		*/
		var id 				int
		var idd 	string
		var idd_size int
		var index_type string
		var id_pack_id int

		err = rows.Scan(&id, &idd, &idd_size, &index_type, &id_pack_id)
		if err != nil {
			Printf("db_verify:query failed %v\n", err)
			return nil, err
		}

		// convert idd to ID
		idd_as_ID, err := restic.ParseID(idd)
		if err != nil {
			Printf("Parse failed for %s %v\n", idd, err)
			return nil, err
		}
		db_index_repo[idd_as_ID] = DBIndexRepoRecord{idd: idd_as_ID,
			idd_size: idd_size, index_type: index_type, id_pack_id:id_pack_id}
	}
	rows.Close()
	return db_index_repo, nil
}

func check_db_index_repo(db_index_repo map[restic.ID]DBIndexRepoRecord,
db_aggregate DBAggregate, repositoryData *RepositoryData) bool {
	// step 1: build comparison table from memory
	/* index_handle is a map[restic.ID]Index_Handle, wit
	 Index_Handle{Type: blob.Type, size: blob.Length,
			pack_index: repositoryData.blob_to_index[blob.PackID],
			blob_index: repositoryData.blob_to_index[blob.ID]}

		type DBIndexRepoRecord struct {
			idd 				restic.ID
			idd_size 		int
			index_type 	string
			id_pack_id	int // ptr to packfiles table, no validation here
		}
			*/
	mem_repo_index_map := make(map[restic.ID]DBIndexRepoRecord, db_aggregate.table_counts["index_repo"])
	for id, data := range repositoryData.index_handle {
		var index_type string
		if data.Type == restic.TreeBlob {
			index_type = "tree"
		} else {
			index_type = "data"
		}
		mem_repo_index_map[id] = DBIndexRepoRecord{idd: id, idd_size: int(data.size),
			index_type: index_type, id_pack_id: 0}
	}

	//build key sets
	set_db_keys := mapset.NewSet()
	set_mem_keys := mapset.NewSet()
	for key := range db_index_repo {
		set_db_keys.Add(key)
	}
	for key := range mem_repo_index_map {
		set_mem_keys.Add(key)
	}

	// compare keys
	// compare keys being equal
	keys_equal := set_mem_keys.Equal(set_db_keys)
	if !keys_equal {
		diff := mapset.NewSet()
		tab := ""
		len_db_set  := set_db_keys.Cardinality()
		len_mem_set := set_mem_keys.Cardinality()
		Printf("keys don't match\n")
		Printf("db_snapshots  %5d\n", len_db_set)
		Printf("mem_snapshots %5d\n", len_mem_set)
		if len_mem_set > len_db_set {
			diff = set_mem_keys.Difference(set_db_keys)
			tab = "db "
		} else if len_db_set > len_mem_set {
			diff = set_db_keys.Difference(set_mem_keys)
			tab = "mem"
		}
		for key := range diff.Iter() {
			Printf("%s missing %s\n", tab, key.(string))
		}
		return keys_equal
	}

	// compare table values - ignore id_pack_id
	compare_equals := true
	for db_key, db_value := range db_index_repo {
		mem_value := mem_repo_index_map[db_key]
		if db_value.idd_size != mem_value.idd_size || db_value.index_type != mem_value.index_type {
			Printf("db  %7d %s\n", db_value.idd_size, db_value.idd_size)
			Printf("mem %7d %s\n", mem_value.idd_size, mem_value.idd_size)
			compare_equals = false
		}
	}
	return compare_equals
}
