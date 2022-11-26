package main

// compile with "go run build.go -tags debug""
// run with DEBUG_LOG=/home/wplapper/restic/debug.log fully-qualified-name/restic -r <repo> overview

// ref to onedrive: rclone:onedrive:restic_backups

/* The companion commands db_verify, db_add_record and db_rem_record manage the
 * relationship between the restic repository and the SQLite database.
 * db_add_record adds new records to the database, whereas db_rem_record removes
 * old stale records which cannot be longer found in the repository.
 * db_verify makes sure that the two systems are in sync nad indicates
 * differences but does not make any changes on its own.
 *
 * The developed code tries to use mnay concepts of Go to minimise repeating
 * bits of code all over the place. The database has seven major tables which
 * look very similar, but represent different parts of the repository
 *
 * So the following more recnt Go concepts have been used:
 * Reflection and Generics.
 */
import (
	// system
	"errors"
	"golang.org/x/sync/errgroup"
	"reflect"
	"sort"
	"time"
	//"regexp"

	//argparse
	"github.com/spf13/cobra"

	// sets
	"github.com/wplapper/restic/library/mapset"

	// sqlx for sqlite3
	//"github.com/jmoiron/sqlx"

	// restic library
	"github.com/wplapper/restic/library/restic"
	"github.com/wplapper/restic/library/sqlite"
)

// system wide variables
var db_aggregate DBAggregate
var dbOptions DBOptions
var newComers *Newcomers
//var repositoryData *RepositoryData

// map repo to database - really a const, but not according to the Go gospel
var DATABASE_NAMES = map[string]string{
	// master
	"/media/mount-points/Backup-ext4-Mate/restic_master":  "/media/mount-points/home/wplapper/restic/db/restic-master_nfs.db",
	"/media/mount-points/Backup-ext4-Mate/restic_master/": "/media/mount-points/home/wplapper/restic/db/restic-master_nfs.db",

	// onedrive
	"rclone:onedrive:restic_backups": "/media/mount-points/home/wplapper/restic/db/restic-onedrive.db",

	// most
	"/media/mount-points/Backup-ext4-Mate/restic_most":  "/media/mount-points/home/wplapper/restic/db/restic-most_nfs.db",
	"/media/mount-points/Backup-ext4-Mate/restic_most/": "/media/mount-points/home/wplapper/restic/db/restic-most_nfs.db",

	// data
	"/media/wplapper/internal-fast/restic_Data":  "/home/wplapper/restic/db/XPS-restic-data_nfs.db",
	"/media/wplapper/internal-fast/restic_Data/": "/home/wplapper/restic/db/XPS-restic-data_nfs.db",

	// test
	"/media/wplapper/internal-fast/restic_test":  "/home/wplapper/restic/db/XPS-restic-test.db",
	"/media/wplapper/internal-fast/restic_test/": "/home/wplapper/restic/db/XPS-restic-test.db"}

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

	// step 1: open repository
	repo, err := OpenRepository(gopts)
	if err != nil {
		return err
	}
	Printf("Repository is %s\n", gopts.Repo) //repo.cfg.ID)
	//timeMessage("%-30s %10.1f seconds\n", "open repository", time.Now().Sub(start).Seconds())
	/*config, err := restic.LoadConfig(gopts.ctx, repo)
	if err != nil {
		return err
	}
	Printf("config %+v\n", config.ID)*/

	repositoryData.snaps, err = GatherAllSnapshots(gopts, repo)
	if err != nil {
		return err
	}

	// step 2: manage Index Records
	start = time.Now()
	if err = HandleIndexRecords(gopts, repo, repositoryData); err != nil {
		return err
	}
	//timeMessage("%-30s %10.1f seconds\n", "read index records", time.Now().Sub(start).Seconds())

	// step 3: collect all snapshot related information
	start = time.Now()
	GatherAllRepoData(gopts, repo, repositoryData)
	//timeMessage("%-30s %10.1f seconds\n", "GatherAllRepoData (sum)", time.Now().Sub(start).Seconds())

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

	// gather counts for all tables in database
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

	// the first three tables can go parallel
	wg, _ := errgroup.WithContext(gopts.ctx)
	wg.Go(func() error { return ReadSnapshotTable(db_conn, &db_aggregate) })
	wg.Go(func() error { return ReadNamesTable(db_conn, &db_aggregate) })
	wg.Go(func() error { return ReadPackfilesTable(db_conn, &db_aggregate) })
	res := wg.Wait()
	if res != nil {
		Printf("Esrror processing group 1. Error is %v\n", res)
		return res
	}

	err = ReadIndexRepoTable(db_conn, &db_aggregate)
	if err != nil {
		Printf("Error processing ReadIndexRepoTable. Error is %v\n", err)
		return err
	}

	// the last three tables can be read in parallel, since they dont depend on one another
	wg1, _ := errgroup.WithContext(gopts.ctx)
	wg1.Go(func() error { return ReadMetaDirTable(db_conn, &db_aggregate) })
	wg1.Go(func() error { return ReadIddFileTable(db_conn, &db_aggregate) })
	wg1.Go(func() error { return ReadContentsTable(db_conn, &db_aggregate) })
	res = wg1.Wait()
	if res != nil {
		Printf("Error processing group 2. Error is %v\n", res)
		return res
	}

	wg, _ = errgroup.WithContext(gopts.ctx)
	//_ = wg
	var r1 bool
	var r2 bool
	var r3 bool
	var r4 bool
	var r5 bool
	var r6 bool
	var r7 bool
	GenericCompare("snapshots", &db_aggregate, check_db_snapshots, repositoryData, newComers, &r1)
	if !r1 {
		Printf("Snapshots mismatch!\n")
		return errors.New("Snapshots mismatch!")
	}

	wg, _ = errgroup.WithContext(gopts.ctx)
	wg.Go(func() error {
		return GenericCompare("index_repo", &db_aggregate, check_db_index_repo, repositoryData, newComers, &r2)
	})
	wg.Go(func() error {
		return GenericCompare("packfiles", &db_aggregate, check_db_packfiles, repositoryData, newComers, &r3)
	})
	wg.Go(func() error {
		return GenericCompare("contents", &db_aggregate, check_db_contents, repositoryData, newComers, &r4)
	})
	wg.Go(func() error {
		return GenericCompare("meta_dir", &db_aggregate, check_db_meta_dir, repositoryData, newComers, &r5)
	})
	wg.Go(func() error {
		return GenericCompare("names", &db_aggregate, check_db_names, repositoryData, newComers, &r6)
	})
	wg.Go(func() error {
		return GenericCompare("idd_file", &db_aggregate, check_db_idd_file, repositoryData, newComers, &r7)
	})
	res = wg.Wait()
	if res != nil {
		Printf("Error processing group 1. Error is %v\n", res)
		return res
	}

	// collect summary
	var flags = []bool{r1, r2, r3, r4, r5, r6, r7}
	total := true
	for ix, which := range flags {
		if !which {
			Printf("check %d failed!\n", ix)
			total = false
		}
	}
	if total {
		Printf("*** all tables compare equal ***\n")
	} else {
		Printf("*** some tables fail to compare! ***\n")
	}
	// check foreign key relationship
	CheckForeignKeys(&db_aggregate, repositoryData)
	return nil
}

func CheckForeignKeys(db_aggregate *DBAggregate, repositoryData *RepositoryData) bool {
	type ForeignKeys struct {
		check_table  string
		column_name  string
		group_number int
		ref_table    string
	}
	var check_tables = []ForeignKeys{
		{"meta_dir", "Id_snap_id", 2, "snapshots"},
		{"meta_dir", "Id_idd", 3, "index_repo"},
		{"idd_file", "Id_blob", 3, "index_repo"},
		{"contents", "Id_data_idd", 3, "index_repo"},
		{"contents", "Id_blob", 3, "index_repo"},
		{"idd_file", "Id_name", 4, "names"},
		{"index_repo", "Id_pack_id", 5, "packfiles"}}

	Printf("\n*** Check Foreign Key relationship ***\n")
	dyna_table := reflect.ValueOf(db_aggregate).Elem()
	previous_group := 0
	ref_table_keys1 := mapset.NewSet[int]()
	all_good := true

	// loop over all ForeignKeys relationships
	for _, entry := range check_tables {
		table_good := true
		current_group := entry.group_number
		if current_group != previous_group {
			ref_table_keys1 = BuildReferenceSet(db_aggregate, entry.ref_table)
			previous_group = current_group
		}

		// regex to extract key and value Type
		//re := regexp.MustCompile(`^map\[([A-Za-z0-9_.]+)\]([A-Za-z0-9_.]+)$`)
		//res := re.FindStringSubmatch(type_string)
		//Printf("result of regex key='%s' values='%s'\n", res[1], res[2])

		var data interface{}
		Printf("checking %s.%s vs %s.id\n", entry.check_table, entry.column_name, entry.ref_table)
		iter := dyna_table.FieldByName("Table_" + entry.check_table).MapRange()
		print_count := 0
		for iter.Next() {
			raw_data := iter.Value()

			// empty interface, knows about Type of the data in the interface
			raw_interface := raw_data.Interface()
			switch typ := raw_interface.(type) {
			// same raw data, but with Type assertion
			case IndexRepoRecordMem:
				data = raw_interface.(IndexRepoRecordMem)
			case *IndexRepoRecordMem:
				data = raw_interface.(*IndexRepoRecordMem)
			case *MetaDirRecordMem:
				data = raw_interface.(*MetaDirRecordMem)
			case *IddFileRecordMem:
				data = raw_interface.(*IddFileRecordMem)
			case *ContentsRecordMem:
				data = raw_interface.(*ContentsRecordMem)
			default:
				Printf("Wrong Type %+v \n", typ)
				panic("Wrong Type -- loop 2")
			}

			// extract id-data from <check_table>.<column_name>, convert to int64.
			// ere we use 'data' to get another reflection, which allows us to
			// dynamically choose the column which we want to compare
			to_be_checked := reflect.ValueOf(data).Elem().FieldByName(entry.column_name).Int()
			// convert from int64 to int
			if !ref_table_keys1.Contains(int(to_be_checked)) {
				table_good = false
				all_good = false
				if print_count < 10 {
					Printf("%s.%s[%6d] not in %s.id\n", entry.check_table, entry.column_name,
						to_be_checked, entry.ref_table)
				}
			}
		}
		if !table_good {
			Printf("checking %s.%s vs %s.id\n", entry.check_table, entry.column_name, entry.ref_table)
			Printf("foreign key constraints for %-15s %v\n", entry.check_table, table_good)
		}
	}
	ref_table_keys1.Clear()
	if all_good {
		Printf("All foreign key constraints are OK!\n")
	}
	return all_good
}

// utility function - compare_keys
// compare the keys of the equvalent memory and database table,
// test of equalness and return result
func CompareKeys[K comparable, V1 any, V2 any](table_name string, db map[K]V1, mem map[K]V2) bool {
	// define sets for the keys
	set_db_keys := mapset.NewSet[K]()
	set_mem_keys := mapset.NewSet[K]()
	for key := range db {
		set_db_keys.Add(key)
	}
	for key := range mem {
		set_mem_keys.Add(key)
	}
	return set_mem_keys.Equal(set_db_keys)
}

// compare the keys of the equivalent memory and database table,
// return the difference between memory and database
func NewMemoryKeys[K comparable, V1 any, V2 any](db map[K]V1, mem map[K]V2) mapset.Set[K] {
	// define sets for the keys
	set_db_keys := mapset.NewSet[K]()
	set_mem_keys := mapset.NewSet[K]()
	for key := range db {
		set_db_keys.Add(key)
	}
	for key := range mem {
		set_mem_keys.Add(key)
	}
	return set_mem_keys.Difference(set_db_keys)
}

// compare the keys of the equivalent memory and database table,
// return the difference between database and memory
func OldDBKeys[K comparable, V1 any, V2 any](db map[K]V1, mem map[K]V2) mapset.Set[K] {
	// define sets for the keys
	set_db_keys := mapset.NewSet[K]()
	set_mem_keys := mapset.NewSet[K]()
	for key := range db {
		set_db_keys.Add(key)
	}
	for key := range mem {
		set_mem_keys.Add(key)
	}
	return set_db_keys.Difference(set_mem_keys)
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

// build mapset.Set based on table_name, use Id as field reference
func BuildReferenceSet(db_aggregate *DBAggregate, table_name string) mapset.Set[int] {
	// reflect on db_aggregate.Table_<table_name>
	dyna_table := reflect.ValueOf(db_aggregate).Elem()
	ref_table := dyna_table.FieldByName("Table_" + table_name)
	iter := ref_table.MapRange()
	reference_set := mapset.NewSet[int]()

	var blob_number int
	for iter.Next() {
		// the iter.Value().Interface() knows what the actual Type is!
		// but it is typeless, so it has to be recast into an actual type!
		// so in the switch we go from reflect.Value() back to the Types of the database tables

		raw_interface := iter.Value().Interface()
		switch typ := raw_interface.(type) {
		case *IndexRepoRecordMem:
			blob_number = raw_interface.(*IndexRepoRecordMem).Id
		case *MetaDirRecordMem:
			blob_number = raw_interface.(*MetaDirRecordMem).Id
		case *IddFileRecordMem:
			blob_number = raw_interface.(*IddFileRecordMem).Id
		case *ContentsRecordMem:
			blob_number = raw_interface.(*ContentsRecordMem).Id
		case *SnapshotRecordMem:
			blob_number = raw_interface.(*SnapshotRecordMem).Id
			//Printf("blob_number is %3d\n", blob_number)
		case *NamesRecordMem:
			blob_number = raw_interface.(*NamesRecordMem).Id
		case *PackfilesRecordMem:
			blob_number = raw_interface.(*PackfilesRecordMem).Id
		default:
			Printf("Wrong Type %+v\n", typ)
			panic("Wrong Type -- loop 1")
		}
		reference_set.Add(blob_number)
	}
	return reference_set
}
