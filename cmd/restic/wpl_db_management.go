package main

/* The companion commands db_verify, db_add_record and db_rem_record manage the
 * relationship between the restic repository and the SQLite database.
 * db_add_record adds new records to the database, whereas db_rem_record removes
 * old stale records which cannot be longer found in the repository.
 * db_verify makes sure that the databasenand the repository are in sync.
 * It indicates differences but does not make any changes on its own.
 *
 * The developed code tries to use many concepts of Go to minimise repeating
 * bits of code all over the place. The database has ten major tables which
 * look very similar, but represent different parts of the repository.
 *
 * So the following more recent Go concepts have been used:
 * Generics.
 *
 * In order to keep memory consumption low, the following comparisons are not
 * full two ways compares, rather than checking if all database records are
 * still reflected in the repository.
 * Only the table 'snapshots' is checked both ways.
 */

import (
	// system
	"context"
	"os"
	"time"

	//argparse
	"github.com/spf13/cobra"

	// restic library
	"github.com/wplapper/restic/library/cache"
	"github.com/wplapper/restic/library/sqlite"
)

// this structure should live in wpl_db_management
type DBOptions struct {
	echo               bool
	print_count_tables bool
	db_name            string // for copying internal restic.db to accessable target
	rollback           bool
	timing             bool
	fullpath           bool
	memory_use         bool
	check              bool
	//rebuild_index      bool
}
var dbOptions DBOptions

// the following types represent database tables and their contents
// in the database and in memory
type SnapshotRecordMem struct {
	Id           int
	Snap_id      string // == UNIQUE
	Snap_time    string
	Snap_host    string
	Snap_fsys    string
	Id_snap_root string // tree root
	Status       string
}

type IndexRepoRecordMem struct {
	Id         int
	Idd        string // == UNIQUE
	Idd_size   int
	Index_type string
	Id_pack_id int // back pointer to packfiles
	Status     string
}

type NamesRecordMem struct {
	Id     int
	Name   string // == UNIQUE
	Status string
}

type MetaDirRecordMem struct {
	Id         int
	Id_snap_id int // map back to snapshots
	Id_idd     int // map back to index_repo
	// tuple (Id_snap_id, Id_idd) == UNIQUE
	Status string
}

type ContentsRecordMem struct {
	Id          int
	Id_data_idd int // map back to index_repo.id (data)
	Id_blob     int // map back to index_repo.id (owning directory / meta_blob)
	Position    int // with triple (Id_blob, Position, Offset) == UNIQUE
	Offset      int
	Status      string
}

type IddFileRecordMem struct {
	Id       int
	Id_blob  int // back pointer to index_repo
	Position int
	Id_name  int
	Size     int
	Inode    int64
	Mtime    string
	Type     string
	Status   string
}

type PackfilesRecordMem struct {
	Id          int
	Packfile_id string // == UNIQUE
	Status      string
}

type FullnameMem struct {
	Id       int
	Pathname string // not UNIQUE
	Status   string
}

type DirPathIdMem struct {
	Id          int
	Id_pathname int // == UNIQUE
	Status      string
}

type TimeStamp struct {
	Id               int
	Restic_updated   time.Time
	Database_updated time.Time
	Ts_created       time.Time
}

// Composite indices for maps
type CompMetaDir struct {
	// composite index on MetaDirRecordMem
	snap_id   string
	meta_blob IntID
}

type CompIddFile struct {
	meta_blob IntID
	position  int
}

type CompContents struct {
	meta_blob IntID
	position  int
	offset    int
}

type RemoveTable struct {
	Id int
}

type UpdateTable_index_repo struct {
	Id         int
	Id_pack_id int
}

type Newcomers struct {
	// the containers of various memory tables
	Mem_snapshots       map[string]SnapshotRecordMem
	Mem_index_repo      map[IntID]*IndexRepoRecordMem
	Mem_packfiles       map[IntID]*PackfilesRecordMem
	Mem_names           map[string]*NamesRecordMem
	//Mem_idd_file        map[CompIddFile]*IddFileRecordMem
	//Mem_meta_dir        map[CompMetaDir]*MetaDirRecordMem
	//Mem_contents        map[CompContents]*ContentsRecordMem
	//Mem_dir_path_id     map[IntID]*DirPathIdMem
	Mem_fullname        map[string]*FullnameMem
	Mem_idd_file_slice []*IddFileRecordMem
	Mem_meta_dir_slice []*MetaDirRecordMem
	Mem_contents_slice []*ContentsRecordMem
	Mem_dir_path_id_sl []*DirPathIdMem
}

var cmdDBManage = &cobra.Command{
	Use:   "db-manage [flags]",
	Short: "run db_add_record, db_rem_record, db_verify in sequence",
	Long: `run db_add_record, db_rem_record, db_verify in sequence.

EXIT STATUS
===========

Exit Status is 0 if the command was successful, and non-zero if there was any error.
`,
	DisableAutoGenTag: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runDBManage(cmd.Context(), cmd, globalOptions, args)
	},
}

func init() {
	cmdRoot.AddCommand(cmdDBManage)
	f := cmdDBManage.Flags()
	f.BoolVarP(&dbOptions.echo, "echo", "E", false, "echo database operations to stdout")
	f.BoolVarP(&dbOptions.timing, "timing", "T", false, "produce timings")
	f.BoolVarP(&dbOptions.memory_use, "memory", "M", false, "show memory use")
	f.BoolVarP(&dbOptions.rollback, "rollback", "R", false, "ROLLBACK database operations")
	f.StringVarP(&dbOptions.db_name, "copy-db", "C", "", "copy database to <copy-db> with read access")
}

// runDBManage: run db_add_record, db_rem_record and db_verify in sequence
func runDBManage(ctx context.Context, cmd *cobra.Command, gopts GlobalOptions,
	args []string) error {

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
	if dbOptions.timing {
		timeMessage(dbOptions.memory_use, "%-30s %10.1f seconds\n", "open repository",
			time.Now().Sub(start).Seconds())
	}

	// get database name from within the repository at wpl/restic.db
	db_name, err = database_via_cache(repo, ctx)
	if err != nil {
		Printf("db_verify: could not copy database from backend %v\n", err)
		return err
	}

	if dbOptions.db_name != "" {
		Verbosef("copy restic database to %s\n", dbOptions.db_name)

		// get cache configuration
		tcache, _ := cache.DefaultDir()
		db_name := tcache + "/" + repo.Config().ID + "/wpl/restic.db"
		// read database file
		database_buffer, err := os.ReadFile(db_name)
		if err != nil {
			Printf("db_manage: could not read database from cache %v\n", err)
			return err
		}

		// write it to named location
		err = os.WriteFile(dbOptions.db_name, database_buffer, 0644)
		if err != nil {
			Printf("db_manage: could not write database from cache to %s %v\n",
				db_name, err)
			return err
		}
		return nil
	}

	err = gather_base_data_repo(repo, gopts, ctx, &repositoryData, dbOptions.timing)
	if err != nil {
		return err
	}

	// step 4.2: open selected database
	db_conn, err := sqlite.OpenDatabase(db_name, dbOptions.echo, gopts.Verbose, true)
	if err != nil {
		Printf("db_manage: OpenDatabase failed, error is %v\n", err)
		return err
	}
	db_aggregate.db_conn = db_conn

	// get the the highest id for each TABLE
	err = sqlite.Get_all_high_ids()
	if err != nil {
		Printf("db_add_record: Could not get Get_all_high_ids, error is %v\n", err)
		return err
	}

	// db_add_record
	Verbosef("\n*** run_db_add_record ***\n")
	changedA, err := run_db_add_record(ctx, cmd, gopts, repo, &repositoryData, &db_aggregate,
		newComers, start)
	if err != nil {
		return err
	}

	//db_rem_record
	Verbosef("\n*** run_db_rem_record ***\n")
	changedR, err := run_db_rem_record(ctx, cmd, gopts, repo, &repositoryData,
		&db_aggregate, newComers, start)
	if err != nil {
		return err
	}

	if changedA || changedR {
		err = write_back_database(db_name, repo, ctx)
		if err != nil {
			return err
		}
	}

	// db_verify
	Verbosef("\n*** run_verify\n")
	err = run_db_verify(ctx, cmd, gopts, repo, &repositoryData, &db_aggregate,
		newComers, start, dbOptions.timing, dbOptions.memory_use)
	if err != nil {
		return err
	}
	return nil
}

// initialize new memory maps
func InitNewcomers() *Newcomers {
	var new_comers Newcomers
	new_comers.Mem_snapshots = make(map[string]SnapshotRecordMem)
	new_comers.Mem_index_repo = make(map[IntID]*IndexRepoRecordMem)
	new_comers.Mem_packfiles = make(map[IntID]*PackfilesRecordMem)
	new_comers.Mem_names = make(map[string]*NamesRecordMem)
	//new_comers.Mem_idd_file = make(map[CompIddFile]*IddFileRecordMem)
	//new_comers.Mem_meta_dir = make(map[CompMetaDir]*MetaDirRecordMem)
	//new_comers.Mem_contents = make(map[CompContents]*ContentsRecordMem)
	//new_comers.Mem_dir_path_id = make(map[IntID]*DirPathIdMem)
	new_comers.Mem_fullname = make(map[string]*FullnameMem)
	new_comers.Mem_idd_file_slice = make([]*IddFileRecordMem, 0)
	new_comers.Mem_meta_dir_slice = make([]*MetaDirRecordMem, 0)
	new_comers.Mem_contents_slice = make([]*ContentsRecordMem, 0)
	new_comers.Mem_dir_path_id_sl = make([]*DirPathIdMem, 0)
	return &new_comers
}
