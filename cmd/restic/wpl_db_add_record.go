package main

// add new records to database

import (
	// system
	"context"
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
	Use:   "db-add-record [flags]",
	Short: "add records to SQLite database",
	Long: `add records to SQLite database.

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
	DisableAutoGenTag: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runDBAdd(cmd.Context(), cmd, globalOptions, args)
	},
}

// setup local flags for the current command
func init() {
	cmdRoot.AddCommand(cmdDBAdd)
	f := cmdDBAdd.Flags()
	f.BoolVarP(&dbOptions.echo, "echo", "E", false, "echo database operations to stdout")
	f.BoolVarP(&dbOptions.timing, "timing", "T", false, "produce timings")
	f.BoolVarP(&dbOptions.memory_use, "memory", "M", false, "show memory use")
	f.BoolVarP(&dbOptions.rollback, "rollback", "R", false, "ROLLBACK database operations")
}

type StdForAll func(GlobalOptions, context.Context, *RepositoryData, *DBAggregate, *Newcomers) error
type StdDbRead func(*sqlx.Tx, *DBAggregate) error
type process_functions struct {
	read_func    StdDbRead
	for_all_func StdForAll
}

// runDBAdd: adds new rows to the database
func runDBAdd(ctx context.Context, cmd *cobra.Command, gopts GlobalOptions, args []string) error {
	var (
		repositoryData RepositoryData
		newComers      = InitNewcomers()
		db_name        string
	)

	start := time.Now()
	init_repositoryData(&repositoryData)
	db_aggregate.repositoryData = &repositoryData
	// need access to verbose option etc
	gOptions = gopts

	// step 1: open repository
	repo, err := OpenRepository(ctx, gopts)
	if err != nil {
		return err
	}
	Printf("Repository is %s\n", gopts.Repo)
	if dbOptions.timing {
		timeMessage(dbOptions.memory_use, "%-30s %10.1f seconds\n", "open repository",
			time.Now().Sub(start).Seconds())
	}

	err = gather_base_data_repo(repo, gopts, ctx, &repositoryData, dbOptions.timing)
	if err != nil {
		return err
	}

	// step 4.1: get database name
	db_name, err = database_via_cache(repo, ctx)
	if err != nil {
		Printf("db_verify: could not copy database from backend %v\n", err)
		return err
	}

	// step 4.2: open selected database
	db_conn, err := sqlite.OpenDatabase(db_name, dbOptions.echo, gopts.Verbose, true)
	if err != nil {
		Printf("db_add_record: OpenDatabase failed, error is %v\n", err)
		return err
	}
	db_aggregate.db_conn = db_conn
	Printf("database name is %s\n", db_name)

	// get the the highest id for each TABLE (for INSERTs later)
	err = sqlite.Get_all_high_ids()
	if err != nil {
		Printf("db_add_record: Could not get Get_all_high_ids, error is %v\n", err)
		return err
	}

	changed, err := run_db_add_record(ctx, cmd, gopts, repo, &repositoryData, &db_aggregate,
		newComers, start)
	if err != nil {
		return err
	}

	// reset database tables
	db_aggregate.Table_snapshots = nil
	db_aggregate.Table_names = nil
	db_aggregate.Table_packfiles = nil
	db_aggregate.Table_index_repo = nil
	db_aggregate.Table_meta_dir = nil
	db_aggregate.Table_idd_file = nil
	db_aggregate.Table_contents = nil
	db_aggregate.Table_dir_path_id = nil
	db_aggregate.Table_fullname = nil

	// database must be written back to backend
	if changed {
		err = write_back_database(db_name, repo, ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func run_db_add_record(ctx context.Context, cmd *cobra.Command, gopts GlobalOptions,
	repo restic.Repository, repositoryData *RepositoryData, db_aggregate *DBAggregate,
	newComers *Newcomers, start time.Time) (bool, error) {
	// BEGIN TRANSACTION
	tx, err := db_aggregate.db_conn.Beginx()
	if err != nil {
		Printf("Can't start transaction. Error is %v\n", err)
		return false, err
	}
	db_aggregate.tx = tx
	Printf("BEGIN TRANSACTION\n")
	if dbOptions.timing {
		timeMessage(dbOptions.memory_use, "%-30s %10.1f seconds\n", "Begin Transaction",
			time.Now().Sub(start).Seconds())
	}

	// read three database tables in parallel
	wg, _ := errgroup.WithContext(ctx)
	wg.Go(func() error { return ReadSnapshotTable(tx, db_aggregate) })
	wg.Go(func() error { return ReadNamesTable(tx, db_aggregate) })
	wg.Go(func() error { return ReadPackfilesTable(tx, db_aggregate) })
	res := wg.Wait()
	if res != nil {
		Printf("READ error processing group 1. Error is %v\n", res)
		return false, res
	}

	// Index Repo needs to run by itself since it depends on packfiles
	err = ReadIndexRepoTable(tx, db_aggregate)
	if err != nil {
		Printf("Error processing ReadIndexRepoTable. Error is %v\n", err)
		return false, err
	}
	if dbOptions.timing {
		timeMessage(dbOptions.memory_use, "%-30s %10.1f seconds\n",
			"After reading 4 tables", time.Now().Sub(start).Seconds())
	}

	// sequential: get new rows
	err1 := ForAllSnapShots(gopts, ctx, repositoryData, db_aggregate, newComers)
	err2 := ForAllPackfiles(gopts, ctx, repositoryData, db_aggregate, newComers)
	err3 := ForAllNames(gopts, ctx, repositoryData, db_aggregate, newComers)
	err4 := ForAllIndexRepo(gopts, ctx, repositoryData, db_aggregate, newComers)
	var errs = []error{err1, err2, err3, err4, nil}
	for ix, err := range errs {
		if err != nil && ix != 4 {
			Printf("Error in processing function %d\n", ix+1)
			errs[4] = err
		}
	}
	if errs[4] != nil {
		return false, errs[4]
	}

	if dbOptions.timing {
		timeMessage(dbOptions.memory_use, "%-30s %10.1f seconds\n",
			"After new rows for 4 tables", time.Now().Sub(start).Seconds())
	}

	// the main reason for sequential processing is the amount of memory these
	// functions create, henceforce sequential process and resetting the memory
	// for the database tables afterwards
	var process_list = []process_functions{
		{ReadMetaDirTable, ForAllMetaDir},
		{ReadIddFileTable, ForAllIddFile},
		{ReadContentsTable, ForAllContents},
		{ReadFullnameTable, ForAllFullname},
		{ReadDirPathIdTable, ForAllDirPathIDs},
	}
	for _, actual := range process_list {
		// read database table
		err := actual.read_func(tx, db_aggregate)
		if err != nil {
			return false, err
		}

		// find new rows
		err = actual.for_all_func(gopts, ctx, repositoryData, db_aggregate, newComers)
		if err != nil {
			return false, err
		}
	}
	if dbOptions.timing {
		timeMessage(dbOptions.memory_use, "%-30s %10.1f seconds\n",
			"8 dependent tables: read&new", time.Now().Sub(start).Seconds())
	}

	CreateBlobSummary(db_aggregate, repositoryData, newComers)

	// finale: INSERT new records
	changed, err := CommitNewRecords(db_aggregate, repositoryData, newComers, gopts)
	if err != nil {
		return false, err
	}
	if dbOptions.timing {
		timeMessage(dbOptions.memory_use, "%-30s %10.1f seconds\n", "COMMIT/ROLLBACK",
			time.Now().Sub(start).Seconds())
	}
	return changed, nil
}

// CommitNewRecords goes over all the Sets created before and INSERTs the
// newly found data into the database
// because of my lack of generic programming capabilities in Go I have
// to do every TABLE by hand
func CommitNewRecords(db_aggregate *DBAggregate, repositoryData *RepositoryData,
	newComers *Newcomers, gopts GlobalOptions) (bool, error) {
	column_names, err := GetColumnNames(db_aggregate.db_conn)
	if err != nil {
		return false, err
	}

	changes_made := false
	tx := db_aggregate.tx
	err1 := InsertTable("snapshots", newComers.Mem_snapshots, tx, column_names, &changes_made)
	err2 := InsertTable("packfiles", newComers.Mem_packfiles, tx, column_names, &changes_made)
	err3 := InsertTable("index_repo", newComers.Mem_index_repo, tx, column_names, &changes_made)
	err4 := InsertTable("names", newComers.Mem_names, tx, column_names, &changes_made)
	err5 := InsertTableSlice("meta_dir", newComers.Mem_meta_dir_slice, tx, column_names, &changes_made)
	err6 := InsertTableSlice("idd_file", newComers.Mem_idd_file_slice, tx, column_names, &changes_made)
	err7 := InsertTableSlice("contents", newComers.Mem_contents_slice, tx, column_names, &changes_made)
	err8 := InsertTable("fullname", newComers.Mem_fullname, tx, column_names, &changes_made)
	err9 := InsertTableSlice("dir_path_id", newComers.Mem_dir_path_id_sl, tx, column_names, &changes_made)

	// reset
	newComers.Mem_packfiles = nil
	newComers.Mem_index_repo = nil
	newComers.Mem_names = nil
	//newComers.Mem_meta_dir = nil
	//newComers.Mem_idd_file = nil
	//newComers.Mem_contents = nil
	newComers.Mem_fullname = nil
	//newComers.Mem_dir_path_id = nil

	// collect possible errors
	var errs = []error{err1, err2, err3, err4, err5, err6, err7, err8, err9, nil}
	for ix, err := range errs {
		if err != nil && ix != 9 {
			Printf("Error in processing function %d\n", ix+1)
			errs[9] = err
		}
	}
	if errs[9] != nil {
		return false, errs[9]
	}

	// update timestamp
	if changes_made {
		if len(db_aggregate.Table_snapshots) > 0 {
			err = db_update_timestamp(db_aggregate.Table_snapshots, tx, &changes_made)
		} else {
			err = db_update_timestamp(newComers.Mem_snapshots, tx, &changes_made)
		}
		if err != nil {
			Printf("update timestamp failed: error is %v\n", err)
		}
	}

	// COMMIT or ROLLBACK transaction
	if !dbOptions.rollback && changes_made {
		err = tx.Commit()
		if err != nil {
			Printf("COMMIT error %v\n", err)
			return false, err
		}
		Printf("COMMIT\n")
	} else {
		tx.Rollback()
		Printf("ROLLBACK\n")
		changes_made = false
	}
	return changes_made, nil
}

func InsertTable[KEY comparable, MEM any](tbl_name string, mem_table map[KEY]MEM,
	tx *sqlx.Tx, column_names map[string][]string, changes_made *bool) error {

	if len(mem_table) == 0 {
		return nil
	}
	Printf("INSERT INTO %-15s with %6d rows.\n", tbl_name, len(mem_table))

	// build INSERT statement - bulk INSERT is the only SQL statement which
	// does not need a loop over all new rows
	column_list := column_names[tbl_name]

	// copy all (new) rows from 'mem_table' (values) to 't_insert'
	t_insert := make([]MEM, 0, len(mem_table))
	for _, data := range mem_table {
		t_insert  = append(t_insert, data)
	}

	sql := fmt.Sprintf("INSERT INTO %s(%s) VALUES(:%s)", tbl_name,
		strings.Join(column_list, ","), strings.Join(column_list, ",:"))
	if dbOptions.echo {
		Printf("%s %s\n", time.Now().Format("2006-01-02 15:04:05.000"), sql)
	}


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

func InsertTableSlice[MEM any](tbl_name string, mem_table []MEM,
	tx *sqlx.Tx, column_names map[string][]string, changes_made *bool) error {

	if len(mem_table) == 0 {
		return nil
	}
	Printf("INSERT INTO %-15s with %6d rows.\n", tbl_name, len(mem_table))

	// build INSERT statement - bulk INSERT is the only SQL statement which
	// does not need a loop over all new rows
	column_list := column_names[tbl_name]
	sql := fmt.Sprintf("INSERT INTO %s(%s) VALUES(:%s)", tbl_name,
		strings.Join(column_list, ","), strings.Join(column_list, ",:"))
	if dbOptions.echo {
		Printf("%s %s\n", time.Now().Format("2006-01-02 15:04:05.000"), sql)
	}


	// PREPAREd statement do not work here, since the number of columns to
	// be inserted is dependent on the table to be inserted into.

	// do the INSERT
	// the splitting into segments could a bit more dynamic
	// its is limited by the product 'number-of-columns' * 'rows_inserted'
	const OFFSET = 4000 // max 8 columns * 4000 = 32000 < 32k
	max := len(mem_table)
	var count int64
	for offset := 0; offset < len(mem_table); offset += OFFSET {
		if max > OFFSET {
			max = OFFSET
		}
		if offset+max > len(mem_table) {
			max = len(mem_table) - offset
		}
		r, err := tx.NamedExec(sql, mem_table[offset:offset+max])
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

func CreateBlobSummary(db_aggregate *DBAggregate, repositoryData *RepositoryData,
	newComers *Newcomers) {
	// create blob summary
	sum_data_blobs := 0
	sum_meta_blobs := 0
	count_meta_blobs := 0
	count_data_blobs := 0
	for blob_int := range newComers.Mem_index_repo {
		blob := repositoryData.IndexToBlob[blob_int]
		ih := repositoryData.IndexHandle[blob]
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
	Printf("%s %7d %10.3f MiB\n", "sum ", count_data_blobs + count_meta_blobs,
		float64(sum_data_blobs + sum_meta_blobs)/ONE_MEG)
	Printf("\n")
}

// manage timestamp table (contains exactly one row)
func db_update_timestamp(Table_snapshots map[string]SnapshotRecordMem,
	tx *sqlx.Tx, changes_made *bool) error {
	var (
		max_time       = ""
		max_snap_time  time.Time
		restic_updated time.Time
		err            error
	)

	// find youngest snapshot
	for _, data := range Table_snapshots {
		if data.Snap_time > max_time {
			max_time = data.Snap_time
		}
	}

	//  convert time from youngest snapshot
	if max_time != "" {
		max_snap_time, err = time.Parse("2006-01-02 15:04:05", max_time)
		if err != nil {
			Printf("GetMaxSnaptime: error parsing time %v\n", err)
			panic("GetMaxSnaptime: error parsing time")
		}
	}

	// get restic_updated time from timestamp
	sqll := "SELECT restic_updated FROM timestamp WHERE id = 1"
	err = tx.Get(&restic_updated, sqll)
	if err != nil {
		// there is no row in timestamp, create one
		now := time.Now()
		now_str := now.String()[:19]
		Printf("No timestamp record found at %s, create one\n", now_str)

		sql2 := `INSERT INTO timestamp(id,restic_updated,database_updated,ts_created)
							 VALUES(:id, :restic_updated, :database_updated, :ts_created)`
		tx.MustExec(sql2, 1, max_time, now_str, now_str)
		if dbOptions.echo {
			Printf("%s %s\n", time.Now().Format("2006-01-02 15:04:05.000"), sql2)
		}
		restic_updated = now
	}

	Printf("restic_updated %s\n", restic_updated.String()[:19])
	Printf("max_snap_time  %s\n", max_time)
	// restic_updated < max_snap_time
	if max_time != "" && restic_updated.Before(max_snap_time) {
		sqll = `UPDATE timestamp SET database_updated =
							(SELECT datetime('now', 'localtime')),
							restic_updated = :restic_updated WHERE id = 1`
		tx.MustExec(sqll, max_time)
		if dbOptions.echo {
			Printf("%s %s\n", time.Now().Format("2006-01-02 15:04:05.000"), sqll)
		}
		Printf("UPDATE timestamp.database_updated + restic_updated\n")
	} else {
		sqll := `UPDATE timestamp SET database_updated =
							 (SELECT datetime('now', 'localtime')) WHERE id = 1`
		tx.MustExec(sqll)
		Printf("UPDATE timestamp.database_updated\n")
		sqll = "SELECT database_updated FROM timestamp WHERE id = 1"
		_ = tx.Get(&restic_updated, sqll)
		if dbOptions.echo {
			Printf("%s %s\n", time.Now().Format("2006-01-02 15:04:05.000"), sqll)
		}
		Printf("database updated %s\n", restic_updated.String()[:19])
	}
	*changes_made = true
	return nil
}
