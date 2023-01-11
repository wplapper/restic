package main

// compile with "go run build.go -tags debug""
// run with DEBUG_LOG=/home/wplapper/restic/debug.log fully-qualified-name/restic -r <repo> overview

// ref to onedrive: rclone:onedrive:restic_backups

import (
	// system
	"fmt"
	"golang.org/x/sync/errgroup"
	"strings"
	"time"

	//argparse
	"github.com/spf13/cobra"

	// sqlx for sqlite3
	"github.com/jmoiron/sqlx"

	// restic library
	"github.com/wplapper/restic/library/restic"
	"github.com/wplapper/restic/library/sqlite"
)

var cmdDBAdd = &cobra.Command{
	Use:   "db_add_record [flags]",
	Short: "add records to SQLite database",
	Long: `add records to SQLite database.

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
	DisableAutoGenTag: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runDBAdd(globalOptions, args)
	},
}

// initialize new memory maps
func InitNewcomers() *Newcomers {
	var new_comers Newcomers
	new_comers.Mem_snapshots  = make(map[string]SnapshotRecordMem)
	new_comers.Mem_index_repo = make(map[restic.IntID]*IndexRepoRecordMem)
	new_comers.Mem_packfiles  = make(map[restic.IntID]*PackfilesRecordMem)
	new_comers.Mem_names      = make(map[string]*NamesRecordMem)

	new_comers.Mem_idd_file = make(map[CompIddFile]*IddFileRecordMem)
	new_comers.Mem_meta_dir = make(map[CompMetaDir]*MetaDirRecordMem)
	new_comers.Mem_contents = make(map[CompContents]*ContentsRecordMem)
	new_comers.Mem_dir_children = make(map[CompDirChildren]*DirChildrenMem)
	return &new_comers
}

// setup local flags for the current command
func init() {
	cmdRoot.AddCommand(cmdDBAdd)
	flags := cmdDBAdd.Flags()
	flags.BoolVarP(&dbOptions.echo, 	  "echo", "E", false, "echo database operations to stdout")
	flags.BoolVarP(&dbOptions.rollback, "rollback", "R", false, "ROLLBACK database operations")
	flags.BoolVarP(&dbOptions.timing,   "timing", "T", false, "produce timings")
	flags.StringVarP(&dbOptions.altDB,  "DB", "", "", "aternative database name")
}

type StdForAll func(GlobalOptions, *RepositoryData, *DBAggregate, *Newcomers) error
type StdDbRead func(*sqlx.Tx, *DBAggregate) error
type process_functions struct {
	read_func    StdDbRead
	for_all_func StdForAll
}

// runDBAdd adds new rows to the database
func runDBAdd(gopts GlobalOptions, args []string) error {
	var (
		repositoryData = init_repositoryData() // is a *RepositoryData
		newComers = InitNewcomers()
		db_name string
		ok bool
	)

	start := time.Now()
	db_aggregate.repositoryData = repositoryData
	EMPTY_NODE_ID = restic.Hash([]byte("{\"nodes\":[]}\n"))
	// need access to verbose option etc
	gOptions = gopts

	// step 1: open repository
	repo, err := OpenRepository(gopts)
	if err != nil {
		return err
	}
	Printf("Repository is %s\n", gopts.Repo)
	if dbOptions.timing {
		timeMessage("%-30s %10.1f seconds\n", "open repository",
			time.Now().Sub(start).Seconds())
	}

	repositoryData.snaps, err = GatherAllSnapshots(gopts, repo)
	if err != nil {
		return err
	}
	repositoryData.snap_map = make(map[string]*restic.Snapshot)
	for _, sn := range repositoryData.snaps {
		repositoryData.snap_map[sn.ID().Str()] = sn
	}
	if dbOptions.timing {
		timeMessage("%-30s %10.1f seconds\n", "gather snapshots",
			time.Now().Sub(start).Seconds())
	}

	// step 2: manage Index Records
	if err = HandleIndexRecords(gopts, repo, repositoryData); err != nil {
		return err
	}
	if dbOptions.timing {
		timeMessage("%-30s %10.1f seconds\n", "read index records",
			time.Now().Sub(start).Seconds())
	}

	// step 3: collect all snapshot related information
	GatherAllRepoData(gopts, repo, repositoryData)
	if dbOptions.timing {
		timeMessage("%-30s %10.1f seconds\n", "GatherAllRepoData",
			time.Now().Sub(start).Seconds())
	}

	// step 4.1: get database name
	if dbOptions.altDB != "" {
		db_name = dbOptions.altDB
	} else {
		db_name, ok = DATABASE_NAMES[gopts.Repo]
		if !ok {
			Printf("database name for repo %s is missing!\n", gopts.Repo)
			return nil
		}
	}

	// step 4.2: open selected database
	db_conn, err := sqlite.OpenDatabase(db_name, true, 1, true)
	if err != nil {
		Printf("db_add_record: OpenDatabase failed, error is %v\n", err)
		return err
	}
	db_aggregate.db_conn = db_conn

	// get the the highest id for each TABLE
	err = sqlite.Get_all_high_ids()
	if err != nil {
		Printf("db_add_record: Could not get Get_all_high_ids, error is %v\n", err)
		return err
	}
	/*if dbOptions.timing {
		timeMessage("%-30s %10.1f seconds\n", "After initial DB", time.Now().Sub(start).Seconds())
	}*/

	// BEGIN TRANSACTION
	tx, err := db_aggregate.db_conn.Beginx()
	if err != nil {
		Printf("Can't start transaction. Error is %v\n", err)
		return err
	}
	db_aggregate.tx = tx
	Printf("BEGIN TRANSACTION\n")

	// read three database tables in parallel
	wg, _ := errgroup.WithContext(gopts.ctx)
	wg.Go(func() error { return ReadSnapshotTable(tx, &db_aggregate) })
	wg.Go(func() error { return ReadNamesTable(tx, &db_aggregate) })
	wg.Go(func() error { return ReadPackfilesTable(tx, &db_aggregate) })
	res := wg.Wait()
	if res != nil {
		Printf("READ error processing group 1. Error is %v\n", res)
		return res
	}

	// Index Repo needs to run by itself since it depends om packfiles
	err = ReadIndexRepoTable(tx, &db_aggregate)
	if err != nil {
		Printf("Error processing ReadIndexRepoTable. Error is %v\n", err)
		return err
	}
	if dbOptions.timing {
		timeMessage("%-30s %10.1f seconds\n", "After reading 4 tables",
			time.Now().Sub(start).Seconds())
	}

	err1 := ForAllSnapShots(gopts,   repositoryData, &db_aggregate, newComers)
	err2 := ForAllPackfiles(gopts,   repositoryData, &db_aggregate, newComers)
	err3 := ForAllNames(gopts,       repositoryData, &db_aggregate, newComers)
	err4 := ForAllIndexRepo(gopts,   repositoryData, &db_aggregate, newComers)
	var errs = []error{err1, err2, err3, err4, nil}
	for ix, err := range errs {
		if err != nil && ix != 4 {
			Printf("Error in processing function %d\n", ix + 1)
			errs[4] = err
		}
	}
	if errs[4] != nil {
		return errs[4]
	}
	if dbOptions.timing {
		timeMessage("%-30s %10.1f seconds\n", "After new rows for 4 tables",
			time.Now().Sub(start).Seconds())
	}

	// the main reason for sequential processing is the amount of memory these
	// functions create, henceforce sequential process and resetting the memory
	// for the database tables afterwards
	var process_list = []process_functions{
		{ReadMetaDirTable,  ForAllMetaDir},
		{ReadIddFileTable,  ForAllIddFile},
		{ReadContentsTable, ForAllContents},
		{ReadDirChildrenTable, ForAllDirChildren},
		{ReadDirNameIdTable, ForAllDirNameIDs},
		{ReadFullnameTable, ForAllFullname},
		{ReadDirPathIdTable, ForAllDirPathIDs},
	}
	for _, actual := range process_list {
		// read database table
		err := actual.read_func(tx, &db_aggregate)
		if err != nil {
			return err
		}

		// find new rows
		err = actual.for_all_func(gopts, repositoryData, &db_aggregate, newComers)
		if err != nil {
			return err
		}
	}
	if dbOptions.timing {
		timeMessage("%-30s %10.1f seconds\n", "3 dependent tables, read&new",
			time.Now().Sub(start).Seconds())
	}

	// reset database tables
	db_aggregate.Table_snapshots = nil
	db_aggregate.Table_names = nil
	db_aggregate.Table_packfiles = nil
	db_aggregate.Table_index_repo = nil
	db_aggregate.Table_meta_dir = nil
	db_aggregate.Table_idd_file = nil
	db_aggregate.Table_contents = nil
	db_aggregate.Table_dir_children = nil
	db_aggregate.Table_dir_name_id = nil
	//db_aggregate.Table_dir_path_id = nil
	//db_aggregate.Table_fullname = nil

	CreateBlobSummary(&db_aggregate, repositoryData, newComers)

	// finale: INSERT new records
	err = CommitNewRecords(&db_aggregate, repositoryData, newComers, gopts)
	if err != nil {
		return err
	}
	if dbOptions.timing {
		timeMessage("%-30s %10.1f seconds\n", "COMMIT/ROLLBACK", time.Now().Sub(start).Seconds())
	}
	return nil
}

// CommitNewRecords goes over all the Sets created before and INSERTs the
// newly found data into the database
// because of my lack of generic programming capabilities in Go I have
// to do every TABLE by hand
func CommitNewRecords(db_aggregate *DBAggregate, repositoryData *RepositoryData,
	newComers *Newcomers, gopts GlobalOptions) error {
	column_names, err := GetColumnNames(db_aggregate.db_conn)
	if err != nil {
		return err
	}

	changes_made := false
	tx := db_aggregate.tx
	err1 := InsTab("snapshots",   newComers.Mem_snapshots,  tx, column_names, &changes_made)
	err2 := InsTab("packfiles",   newComers.Mem_packfiles,  tx, column_names, &changes_made)
	err3 := InsTab("index_repo",  newComers.Mem_index_repo, tx, column_names, &changes_made)
	err4 := InsTab("names",       newComers.Mem_names,      tx, column_names, &changes_made)
	err5 := InsTab("meta_dir",    newComers.Mem_meta_dir,   tx, column_names, &changes_made)
	err6 := InsTab("idd_file",    newComers.Mem_idd_file,   tx, column_names, &changes_made)
	err7 := InsTab("contents",    newComers.Mem_contents,   tx, column_names, &changes_made)
	err8 := InsTab("dir_children",newComers.Mem_dir_children,tx, column_names, &changes_made)
	err9 := InsTab("dir_name_id", newComers.Mem_dir_name_id,tx, column_names, &changes_made)
	err10 := InsTab("fullname",   newComers.Mem_fullname,   tx, column_names, &changes_made)
	err11 := InsTab("dir_path_id",newComers.Mem_dir_path_id,tx, column_names, &changes_made)

	// collect possible errors
	var errs = []error{err1, err2, err3, err4, err5, err6, err7, err8, err9, err10, err11, nil}
	for ix, err := range errs {
		if err != nil && ix != 11 {
			Printf("Error in processing function %d\n", ix + 1)
			errs[11] = err
		}
	}
	if errs[11] != nil {
		return errs[11]
	}

	// update timestamp
	if len(db_aggregate.Table_snapshots) > 0 {
		err = db_update_timestamp(db_aggregate.Table_snapshots, tx)
	} else {
		err = db_update_timestamp(newComers.Mem_snapshots, tx)
	}
	if err != nil {
		Printf("update timestamp failed: error is %v\n", err)
	}

	// COMMIT or ROLLBACK transaction
	if !dbOptions.rollback && changes_made {
		err = tx.Commit()
		if err != nil {
			Printf("COMMIT error %v\n")
			return err
		}
		Printf("COMMIT\n")
	} else {
		tx.Rollback()
		Printf("ROLLBACK\n")
	}
	return nil
}

func InsertTable[K comparable, V any] (tbl_name string, mem_table map[K]V,
	tx *sqlx.Tx, column_names map[string][]string, changes_made *bool) error {

	if len(mem_table) == 0 {
		return nil
	}
	Printf("INSERT INTO %s with %6d rows.\n", tbl_name, len(mem_table))

	// build INSERT statement - bulk INSERT is the only SQL statement which
	// does not need a loop over all new rows
	column_list := column_names[tbl_name]
	value_list := make([]string, len(column_list))
	for ix, name := range column_list {
		value_list[ix] = ":" + name
	}

	t_insert := make([]V, len(mem_table))
	ix := 0
	for _, data := range mem_table {
		t_insert[ix] = data
		ix++
	}

	sql := fmt.Sprintf("INSERT INTO %s(%s) VALUES(%s)", tbl_name,
		strings.Join(column_list, ","), strings.Join(value_list, ","))
	if dbOptions.echo {
		Printf("%s\n", sql)
	}

	// do the INSERT
	// the splitting into segments could a bit more dynamic
	// its is limited by the product 'number-of-columns' * 'rows_inserted'
	const OFFSET = 4000 // max 8 columns * 4000 = 32000 < 32k
	for offset := 0; offset < len(t_insert); offset += OFFSET {
		max := len(t_insert)
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
		count, _ := r.RowsAffected()
		if count > 0 {
			*changes_made = true
		}
	}
	Printf("%7d rows inserted into table %s\n", len(t_insert), tbl_name)
	return nil
}

// generic funtion to INSERT new data into the database
func InsTab[K comparable, V any](table_name string, mem_map map[K]V,
	tx *sqlx.Tx, column_names map[string][]string, changes_made *bool) error {
	//filter_new(mem_map, "new")
	err := InsertTable(table_name, mem_map, tx, column_names, changes_made)
	if table_name != "snapshots" {
		mem_map = nil
	}
	return err
}

func CreateBlobSummary(db_aggregate *DBAggregate, repositoryData *RepositoryData,
newComers *Newcomers) {
	// create blob summary
	for key, data := range newComers.Mem_index_repo {
		if data.Status != "new" {
			delete(newComers.Mem_index_repo, key)
		}
	}
	sum_data_blobs   := 0
	sum_meta_blobs   := 0
	count_meta_blobs := 0
	count_data_blobs := 0
	for blob_int := range newComers.Mem_index_repo {
		blob := repositoryData.index_to_blob[blob_int]
		ih := repositoryData.index_handle[blob]
		typ := ih.Type.String()[0:1]
		if typ == "t" {
			sum_meta_blobs += ih.size
			count_meta_blobs++
		} else if typ == "d" {
			sum_data_blobs += ih.size
			count_data_blobs++
		}
	}
	Printf("\n*** Summary of new data in database ***\n")
	Printf("type   count       size\n")
	Printf("%s %7d %10.3f MiB\n", "meta", count_meta_blobs, float64(sum_meta_blobs)/ONE_MEG)
	Printf("%s %7d %10.3f MiB\n", "data", count_data_blobs, float64(sum_data_blobs)/ONE_MEG)
	Printf("%s %7d %10.3f MiB\n", "sum ", count_data_blobs+count_meta_blobs,
		float64(sum_data_blobs+sum_meta_blobs)/ONE_MEG)
	Printf("\n")
}

// manage timestamp table (with one row)
func db_update_timestamp(Table_snapshots map[string]SnapshotRecordMem, tx *sqlx.Tx) error {
	var (
		max_time = ""
		max_snap_time time.Time
		restic_updated time.Time
	)
	for _, data := range Table_snapshots {
		if data.Snap_time > max_time {
			max_time = data.Snap_time
		}
	}
	//  convert youngest time from snapshots
	if max_time != "" {
		max_snap_time, err := time.Parse("2006-01-02 15:04:05", max_time)
		if err != nil {
			Printf("GetMaxSnaptime: error parsing time %v\n", err)
			panic("GetMaxSnaptime: error parsing time")
		}
		_ = max_snap_time
	}

	// get restic_updated time from timestamp
	sql := "SELECT restic_updated FROM timestamp WHERE id = 1"
	err := tx.Get(&restic_updated, sql)
	if err != nil {
		// there is no row in timestamp, create one
		now := time.Now()
		now_str := now.String()[:19]
		sql2 := `INSERT INTO timestamp(id,restic_updated,database_updated,ts_created)
               VALUES(:id,:restic_updated,:database_updated,:ts_created)`
		_, err := tx.Exec(sql2, 1, now_str, now_str, now_str)
		if err != nil {
			fmt.Printf("Can't INSERT INTO timestamp. Error is %v\n", err)
			return err
		}
		restic_updated = now
	}

	if max_time != "" && restic_updated.Before(max_snap_time) {
		sql = `UPDATE timestamp SET database_updated =
              (SELECT datetime('now', 'localtime')),
              restic_updated = :restic_updated WHERE id = 1`
		_, err := tx.Exec(sql, max_snap_time.String()[:19])
		if err != nil {
			Printf("UPDATE timestamp failed 1: %v\n", err)
			return err
		}
	} else {
		sql := `UPDATE timestamp SET database_updated =
               (SELECT datetime('now', 'localtime')) WHERE id = 1`
		_, err := tx.Exec(sql)
		if err != nil {
			Printf("UPDATE timestamp failed 2: %v\n", err)
			return err
		}
	}
	return nil
}
