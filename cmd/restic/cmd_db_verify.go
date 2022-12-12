package main

// compile with "go run build.go -tags debug""
// run with DEBUG_LOG=/home/wplapper/restic/debug.log fully-qualified-name/restic -r <repo> overview

// ref to onedrive: rclone:onedrive:restic_backups

/* The companion commands db_verify, db_add_record and db_rem_record manage the
 * relationship between the restic repository and the SQLite database.
 * db_add_record adds new records to the database, whereas db_rem_record removes
 * old stale records which cannot be longer found in the repository.
 * db_verify makes sure that the two systems are in sync and indicates
 * differences but does not make any changes on its own.
 *
 * The developed code tries to use many concepts of Go to minimise repeating
 * bits of code all over the place. The database has seven major tables which
 * look very similar, but represent different parts of the repository.
 *
 * So the following more recent Go concepts have been used:
 * Generics.
 *
 * In order to keep memory consumption low, the following comparisons are not
 * full two ways compares, rather than checkimg if all database records are
 * still reflected in the repository.
 */

import (
	// system
	"errors"
	"golang.org/x/sync/errgroup"
	"sort"
	"time"
	"fmt"

	//argparse
	"github.com/spf13/cobra"

	// sets
	//"github.com/wplapper/restic/library/mapset"

	// sqlx for sqlite3
	//"github.com/jmoiron/sqlx"

	// restic library
	"github.com/wplapper/restic/library/restic"
	"github.com/wplapper/restic/library/sqlite"
)

var cmdDBVerify = &cobra.Command{
	Use:   "db_verify [flags]",
	Short: "compare SQLite database with repository",
	Long: `compare SQLite database with repository.

EXIT STATUS
===========

Exit Status is 0 if the command was successful, and non-zero if there was any error.
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
	flags.StringVarP(&dbOptions.altDB, "DB", "", "", "aternative database name")
}

func runDBVerify(gopts GlobalOptions, args []string) error {

	// step 0: setup global stuff
	start := time.Now()
	_ = start
	repositoryData = init_repositoryData() // is a *RepositoryData
	db_aggregate.repositoryData = repositoryData
	EMPTY_NODE_ID = restic.Hash([]byte("{\"nodes\":[]}\n"))
	newComers = InitNewcomers()
	gOptions = gopts
	var db_name string
	var ok bool

	// step 0: open repository
	repo, err := OpenRepository(gopts)
	if err != nil {
		return err
	}
	Printf("Repository is %s\n", gopts.Repo) //repo.cfg.ID)
	//timeMessage("%-30s %10.1f seconds\n", "open repository", time.Now().Sub(start).Seconds())

	// step 1: gather the snapshot information
	repositoryData.snaps, err = GatherAllSnapshots(gopts, repo)
	if err != nil {
		return err
	}
	repositoryData.snap_map = make(map[string]*restic.Snapshot)
	for _, sn := range repositoryData.snaps {
		repositoryData.snap_map[sn.ID().Str()] = sn
	}

	// step 2: manage Index Records
	start = time.Now()
	if err = HandleIndexRecords(gopts, repo, repositoryData); err != nil {
		return err
	}
	//timeMessage("%-30s %10.1f seconds\n", "read index records", time.Now().Sub(start).Seconds())

	// step 3: collect all repository related information
	start = time.Now()
	GatherAllRepoData(gopts, repo, repositoryData)
	timeMessage("%-30s %10.1f seconds\n", "GatherAllRepoData (sum)", time.Now().Sub(start).Seconds())
	PrintMemUsage()

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

	// step 4.2: open selected database -- XXX more generic verbose & echo options
	db_conn, err := sqlite.OpenDatabase(db_name, true, 1, true)
	if err != nil {
		Printf("db_verify: OpenDatabase failed, error is %v\n", err)
		return err
	}
	db_aggregate.db_conn = db_conn

	// step 5: BEGIN TRANSACTION
	start = time.Now()
	tx, err := (db_aggregate).db_conn.Beginx()
	if err != nil {
		Printf("Cant start transaction. Error is %v\n", err)
		return err
	}
	db_aggregate.tx = tx
	Printf("BEGIN TRANSACTION\n")

	// step 6.1: gather counts for all tables in database
	names_and_counts := make(map[string]int)
	if err = readAllTablesAndCounts(db_conn, names_and_counts); err != nil {
		Printf("readAllTablesAndCounts error is %v\n", err)
		return err
	}
	db_aggregate.table_counts = names_and_counts

	// step 6.2: sort table names
	tbl_names_sorted := make([]string, len(names_and_counts))
	ix := 0
	for tbl_name := range names_and_counts {
		tbl_names_sorted[ix] = tbl_name
		ix++
	}
	sort.Strings(tbl_names_sorted)

	// step 6.3: print table counts
	for _, tbl_name := range tbl_names_sorted {
		if len(tbl_name) >= 9 && tbl_name[:9] == "timestamp" {
			continue
		}
		count := names_and_counts[tbl_name]
		Printf("%-25s %8d\n", tbl_name, count)
	}

	// step 7: READ database
	// READ database tables: the first three tables can be read parallel
	// we need the tables 'snapshots', 'index_repo', 'packfiles' and 'names'
	// in memory, because of the back pointer links (foreign keys)
	wg, _ := errgroup.WithContext(gopts.ctx)
	wg.Go(func() error { return ReadSnapshotTable(tx, &db_aggregate) })
	wg.Go(func() error { return ReadNamesTable(tx, &db_aggregate) })
	wg.Go(func() error { return ReadPackfilesTable(tx, &db_aggregate) })
	res := wg.Wait()
	if res != nil {
		Printf("Error processing group 1. Error is %v\n", res)
		return res
	}

	err = ReadIndexRepoTable(tx, &db_aggregate)
	if err != nil {
		Printf("Error processing ReadIndexRepoTable. Error is %v\n", err)
		return err
	}

	timeMessage("%-30s %10.1f seconds\n", "After READ 4 tables",
		time.Now().Sub(start).Seconds())
	PrintMemUsage()

	// step 8: compare database and repository
	var r1, r2, r3, r4, r5, r6, r7 bool
	GenericCompare("snapshots", &db_aggregate, check_db_snapshots_v2, repositoryData, newComers, &r1)
	if !r1 {
		return errors.New("Snapshots mismatch!")
	}

	GenericCompare("index_repo", &db_aggregate, check_db_index_repo_v2,repositoryData, newComers, &r2)
	GenericCompare("packfiles",  &db_aggregate, check_db_packfiles_v2,repositoryData, newComers, &r3)
	GenericCompare("names", 		 &db_aggregate, check_db_names_v2, 		repositoryData, newComers, &r4)
	GenericCompare("meta_dir", 	 &db_aggregate, check_db_meta_dir_v2, repositoryData, newComers, &r5)
	GenericCompare("idd_file", 	 &db_aggregate, check_db_idd_file_v2, repositoryData, newComers, &r6)
	GenericCompare("contents", 	 &db_aggregate, check_db_contents_v2, repositoryData, newComers, &r7)

	// collect summary
	var flags = []bool{r2, r3, r4, r5, r6, r7}
	total := true
	for ix, which := range flags {
		if !which {
			Printf("check %d failed!\n", ix + 2)
			total = false
		}
	}
	if total {
		Printf("*** all tables compare equal ***\n")
	} else {
		Printf("*** some tables fail to compare! ***\n")
	}
	timeMessage("%-30s %10.1f seconds\n", "Compare all tables",
		time.Now().Sub(start).Seconds())
	PrintMemUsage()

	// step 9: check foreign key relationship
	CheckForeignKeys(&db_aggregate, repositoryData)
	return nil
}

func CheckForeignKeys(db_aggregate *DBAggregate, repositoryData *RepositoryData) bool {
	type ForeignKeys struct {
		check_table  string
		column_name  string
		ref_table    string // the implied column is always "Id" for the ref_table
	}
	var check_tables = []ForeignKeys{
		{"meta_dir", "Id_snap_id", "snapshots"},
		{"meta_dir", "Id_idd", "index_repo"},
		{"idd_file", "Id_blob", "index_repo"},
		{"contents", "Id_data_idd", "index_repo"},
		{"contents", "Id_blob", "index_repo"},
		{"idd_file", "Id_name", "names"},
		{"index_repo", "Id_pack_id", "packfiles"}}

	Printf("\n*** Check Foreign Key relationship ***\n")
	all_good := true
	for _, action := range check_tables {
		check_table := true
		sql := fmt.Sprintf(`SELECT %s.%s FROM %s
  LEFT OUTER JOIN %s ON %s.%s = %s.id WHERE %s.id IS NULL`,
			action.check_table, action.column_name, action.check_table, action.ref_table,
			action.check_table, action.column_name, action.ref_table, action.ref_table)

		if dbOptions.echo {
			Printf("%s\n", sql)
		}
		rows, err := db_aggregate.db_conn.Queryx(sql)
		if err != nil {
			Printf("Error in sql %s, error is %v\n", sql, err)
			check_table = false
		}

		count_rows := 0
		for rows.Next() {
			var row RemoveTable
			err = rows.StructScan(&row)
			if err != nil {
				Printf("CheckForeinKeys:StructScan for sql %s failed %v\n", sql, err)
				check_table = false
			}
			if count_rows < 10 {
				Printf("Id=%6d in %s is not related to %s\n", row.Id,
					action.check_table, action.ref_table)
			}
			check_table = false
			if !check_table {
				all_good = false
			}
		}
	}

	if all_good {
		Printf("All foreign key checks are OK!\n")
	} else {
		Printf("Some foreign key checks failed!\n")
	}
	return all_good
}


// call generic comparator
type ComparatorFunc func(*DBAggregate, *RepositoryData, *Newcomers) bool

func GenericCompare(table_name string, db_aggregate *DBAggregate, comparator ComparatorFunc,
	repositoryData *RepositoryData, newComers *Newcomers, return_value *bool) error {
	*return_value = comparator(db_aggregate, repositoryData, newComers)
	if !*return_value {
		Printf("checking table %s FAIL\n", table_name)
	}
	return nil
}
