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
 * bits of code all over the place. The database has ten major tables which
 * look very similar, but represent different parts of the repository.
 *
 * So the following more recent Go concepts have been used:
 * Generics.
 *
 * In order to keep memory consumption low, the following comparisons are not
 * full two ways compares, rather than checkimg if all database records are
 * still reflected in the repository.
 * Only the table 'snapshots' is checked both ways.
 */

import (
	// system
	"context"
	"errors"
	"fmt"
	"golang.org/x/sync/errgroup"
	"sort"
	"time"

	//argparse
	"github.com/spf13/cobra"

	// sqlite
	"github.com/jmoiron/sqlx"

	// restic library
	"github.com/wplapper/restic/library/restic"
	"github.com/wplapper/restic/library/sqlite"
)

type DBOptions struct {
	echo bool
	timing bool
	memory_use bool
	print_count_tables bool
}
var dbOptions DBOptions

type DBAggregate struct {
	repositoryData    *RepositoryData
	db_conn           *sqlx.DB
	tx                *sqlx.Tx
	table_counts      map[string]int // count of all tables
	// the database tables - memory representation
	Table_snapshots   map[string]SnapshotRecordMem
	Table_index_repo  map[IntID]*IndexRepoRecordMem
	Table_packfiles   map[IntID]*PackfilesRecordMem
	Table_names       map[string]*NamesRecordMem
	Table_meta_dir    map[CompMetaDir]*MetaDirRecordMem
	Table_idd_file    map[CompIddFile]*IddFileRecordMem
	Table_contents    map[CompContents]*ContentsRecordMem
	Table_dir_path_id map[IntID]*DirPathIdMem
	Table_fullname    map[string]*FullnameMem
	// other tables reference these tables via FOREIGN KEY
	pk_snapshots      map[int]string
	pk_index_repo     map[int]IntID
	pk_fullname       map[int]string
}

var cmdDBVerify = &cobra.Command{
	Use:   "db-verify [flags]",
	Short: "compare SQLite database with repository",
	Long: `compare SQLite database with repository.

EXIT STATUS
===========

Exit Status is 0 if the command was successful, and non-zero if there was any error.
`,
	DisableAutoGenTag: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runDBVerify(cmd.Context(), cmd, globalOptions, args)
	},
}

func init() {
	cmdRoot.AddCommand(cmdDBVerify)
	f := cmdDBVerify.Flags()
	f.BoolVarP(&dbOptions.echo, "echo", "E", false, "echo database operations to stdout")
	f.BoolVarP(&dbOptions.timing, "timing", "T", false, "produce timings")
	f.BoolVarP(&dbOptions.memory_use, "memory", "M", false, "produce memory usage")
	f.BoolVarP(&dbOptions.print_count_tables, "print-c", "C", false, "print table counts")
}

func runDBVerify(ctx context.Context, cmd *cobra.Command, gopts GlobalOptions, args []string) error {

	// step 0: setup global stuff
	start := time.Now()
	var repositoryData RepositoryData
	init_repositoryData(&repositoryData)
	db_aggregate.repositoryData = &repositoryData
	gOptions = gopts

	var (
		db_name string
		repo    restic.Repository
		db_aggregate DBAggregate
		newComers *Newcomers
	)
	newComers = InitNewcomers()

	// step 1: open repository
	repo, err := OpenRepository(ctx, gopts)
	if err != nil {
		return err
	}
	Verboseff("Repository is %s\n", globalOptions.Repo)
	if dbOptions.timing {
		timeMessage(dbOptions.memory_use, "%-30s %10.1f seconds\n", "open repository",
			time.Now().Sub(start).Seconds())
	}

	err = gather_base_data_repo(repo, gopts, ctx, &repositoryData, dbOptions.timing)
	if err != nil {
		return err
	}

	db_name, err = database_via_cache(repo, ctx)
	if err != nil {
		Printf("db_verify: could not copy database from backend %v\n", err)
		return err
	}

	// step 4.2: open selected database --
	db_conn, err := sqlite.OpenDatabase(db_name, dbOptions.echo, gopts.Verbose, true)
	if err != nil {
		Printf("db_verify: OpenDatabase failed, error is %v\n", err)
		return err
	}

	db_aggregate.db_conn = db_conn
	err = run_db_verify(ctx, cmd, gopts, repo, &repositoryData, &db_aggregate,
		newComers, start, dbOptions.timing, dbOptions.memory_use)
	if err != nil {
		return err
	}
	return nil
}

// run_db_verify runs the heart of the verify process
func run_db_verify(ctx context.Context, cmd *cobra.Command, gopts GlobalOptions,
	repo restic.Repository, repositoryData *RepositoryData, db_aggregate *DBAggregate,
	newComers *Newcomers, start time.Time, timing bool, memory_use bool) error {
	// step 5: BEGIN TRANSACTION
	tx, err := (db_aggregate).db_conn.Beginx()
	if err != nil {
		Printf("Cant start transaction. Error is %v\n", err)
		return err
	}

	names_and_counts := make(map[string]int)
	// get count from all tables
	//readAllTablesAndCounts(tx, names_and_counts)
	// sort table names ascending, map meesed the ordering up
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
		Printf("%-25s %8d\n", tbl_name, names_and_counts[tbl_name])
	}
	if dbOptions.print_count_tables {
		tx.Rollback()
		return nil
	}

	db_aggregate.tx = tx
	if dbOptions.timing {
		timeMessage(dbOptions.memory_use, "%-30s %10.1f seconds\n",
			"After initial DB", time.Now().Sub(start).Seconds())
	}

	// step 7: READ database
	// READ database tables: the first three tables can be read parallel
	// we need the tables 'snapshots', 'index_repo', 'packfiles' and 'names'
	// in memory, because of the back pointer links (foreign keys)
	if timing {
		timeMessage(memory_use, "%-30s %10.1f seconds\n", "start read 3 tables paralled",
			time.Now().Sub(start).Seconds())
	}

	wg, _ := errgroup.WithContext(ctx)
	wg.Go(func() error { return ReadSnapshotTable(tx, db_aggregate) })
	wg.Go(func() error { return ReadNamesTable(tx, db_aggregate) })
	wg.Go(func() error { return ReadPackfilesTable(tx, db_aggregate) })
	res := wg.Wait()
	if res != nil {
		Printf("Error processing group 1. Error is %v\n", res)
		return res
	}
	if timing {
		timeMessage(memory_use, "%-30s %10.1f seconds\n", "ended read 3 tables paralled",
			time.Now().Sub(start).Seconds())
	}

	err = ReadIndexRepoTable(tx, db_aggregate)
	if err != nil {
		Printf("Error processing ReadIndexRepoTable. Error is %v\n", err)
		return err
	}

	if timing {
		timeMessage(memory_use, "%-30s %10.1f seconds\n",
			"After READ 4 tables", time.Now().Sub(start).Seconds())
	}

	// step 8: compare database and repository
	var r1, r2, r3, r4, r5, r6, r7 bool
	GenericCompare("snapshots", db_aggregate, check_db_snapshots_v2, repositoryData, &r1)
	if !r1 {
		return errors.New("Snapshots mismatch!")
	}

	GenericCompare("index_repo", db_aggregate, check_db_index_repo_v2, repositoryData, &r2)
	GenericCompare("packfiles", db_aggregate, check_db_packfiles_v2, repositoryData, &r3)
	GenericCompare("names",    db_aggregate, check_db_names_v2, repositoryData, &r4)
	GenericCompare("meta_dir", db_aggregate, check_db_meta_dir_v2, repositoryData, &r5)
	GenericCompare("idd_file", db_aggregate, check_db_idd_file_v2, repositoryData, &r6)
	GenericCompare("contents", db_aggregate, check_db_contents_v2, repositoryData, &r7)
	// the following tables are missing: fullpath and dir_path_id
	// however the mapping is NOT unique!
	if timing {
		timeMessage(memory_use, "%-30s %10.1f seconds\n",
			"GenericCompare 7 tables", time.Now().Sub(start).Seconds())
	}

	// collect summary
	var flags = []bool{r2, r3, r4, r5, r6, r7}
	total := true
	for ix, which := range flags {
		if !which {
			Printf("check %d failed!\n", ix+2)
			total = false
		}
	}
	if total {
		Printf("*** all tables compare equal ***\n")
	} else {
		Printf("*** some tables fail to compare! ***\n")
	}
	if timing {
		timeMessage(memory_use, "%-30s %10.1f seconds\n",
			"Compare all tables", time.Now().Sub(start).Seconds())
	}

	// step 9: check foreign key relationship
	CheckForeignKeys(db_aggregate, repositoryData)
	tx.Rollback()
	return nil
}

func CheckForeignKeys(db_aggregate *DBAggregate, repositoryData *RepositoryData) bool {
	type ForeignKeys struct {
		check_table string
		column_name string
		ref_table   string // the implied column is always "Id" for the ref_table
	}
	var check_tables = []ForeignKeys{
		{"meta_dir", "Id_snap_id", "snapshots"},
		{"meta_dir", "Id_idd", "index_repo"},
		{"idd_file", "Id_blob", "index_repo"},
		{"contents", "Id_data_idd", "index_repo"},
		{"contents", "Id_blob", "index_repo"},
		{"idd_file", "Id_name", "names"},
		{"index_repo", "Id_pack_id", "packfiles"},
		{"dir_path_id", "Id", "index_repo"},
		{"dir_path_id", "Id_pathname", "fullname"},
	}

	Printf("\n*** Check Foreign Key relationship ***\n")
	all_good := true
	for _, action := range check_tables {
		check_table := true
		sql := fmt.Sprintf(`SELECT DISTINCT %s.%s AS id FROM %s
	LEFT OUTER JOIN %s ON %s.%s = %s.id WHERE %s.id IS NULL`,
			action.check_table, action.column_name, action.check_table, action.ref_table,
			action.check_table, action.column_name, action.ref_table, action.ref_table)

		if dbOptions.echo {
			Printf("%s %s\n", time.Now().Format("2006-01-02 15:04:05.000"), sql)
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
				return false
			}
			if count_rows < 10 {
				Printf("Id=%6d in %s is not related to %s\n", row.Id,
					action.check_table, action.ref_table)
			}
			count_rows++
			check_table = false
		}
		rows.Close()
		if !check_table {
			all_good = false
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
type ComparatorFunc func(*DBAggregate, *RepositoryData) bool

func GenericCompare(table_name string, db_aggregate *DBAggregate, comparator ComparatorFunc,
	repositoryData *RepositoryData, return_value *bool) error {
	*return_value = comparator(db_aggregate, repositoryData)
	if !*return_value {
		Printf("checking table %s FAIL\n", table_name)
	}
	return nil
}

type Newcomers struct {
	// the containers of various memory tables
	Mem_snapshots   map[string]SnapshotRecordMem
	Mem_index_repo  map[IntID]*IndexRepoRecordMem
	Mem_packfiles   map[IntID]*PackfilesRecordMem
	Mem_names       map[string]*NamesRecordMem
	Mem_idd_file    map[CompIddFile]*IddFileRecordMem
	Mem_meta_dir    map[CompMetaDir]*MetaDirRecordMem
	Mem_contents    map[CompContents]*ContentsRecordMem
	Mem_dir_path_id map[IntID]*DirPathIdMem
	Mem_fullname    map[string]*FullnameMem
}

// initialize new memory maps
func InitNewcomers() *Newcomers {
	var new_comers Newcomers
	new_comers.Mem_snapshots = make(map[string]SnapshotRecordMem)
	new_comers.Mem_index_repo = make(map[IntID]*IndexRepoRecordMem)
	new_comers.Mem_packfiles = make(map[IntID]*PackfilesRecordMem)
	new_comers.Mem_names = make(map[string]*NamesRecordMem)
	new_comers.Mem_idd_file = make(map[CompIddFile]*IddFileRecordMem)
	new_comers.Mem_meta_dir = make(map[CompMetaDir]*MetaDirRecordMem)
	new_comers.Mem_contents = make(map[CompContents]*ContentsRecordMem)
	new_comers.Mem_dir_path_id = make(map[IntID]*DirPathIdMem)
	new_comers.Mem_fullname = make(map[string]*FullnameMem)
	return &new_comers
}
