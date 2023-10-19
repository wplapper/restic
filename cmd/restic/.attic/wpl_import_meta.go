package main

import (
	// system
	"context"
	"os"
	"io"
	"errors"
	"fmt"
	"bufio"

	//argparse
	"github.com/spf13/cobra"

	// restic library
	"github.com/wplapper/restic/library/restic"

	// sqlite
	"github.com/wplapper/restic/library/sqlite"

	//sets and queues
	"github.com/deckarep/golang-set/v2"
)

var cmdImportMeta = &cobra.Command{
	Use:   "wpl-import [flags]",
	Short: "import metadata from plaintext directory structure",
	Long: `import metadata from plaintext directory structure.

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
	DisableAutoGenTag: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runImportMeta(cmd.Context(), cmd, globalOptions, args)
	},
}

type ImportMetaOptions struct {
	Repo string
	Echo  bool
	New   bool
}
var importMetaOptions ImportMetaOptions

func init() {
	cmdRoot.AddCommand(cmdImportMeta)
	f := cmdImportMeta.Flags()
	f.BoolVarP(&importMetaOptions.Echo, "echo", "E", false, "echo database internals")
	f.BoolVarP(&importMetaOptions.New, "new", "N", false, "start with fresh database")
}

func runImportMeta(ctx context.Context, cmd *cobra.Command, gopts GlobalOptions, args []string) error {
	var err error
	var repoUnpackedName string

	// need to open the repository in order to access the database
	repo, err := OpenRepository(ctx, gopts)
	if err != nil {
		return err
	}
	defer repo.Close()
	Verboseff("Repository is %s\n", globalOptions.Repo)

	// check for given repo name
	repoLongName := map_repo_names(gopts.Repo)
	if len(args) > 0  {
		dirname := args[0]
		fileInfo, err := os.Stat(dirname)
		if err == nil && ! fileInfo.IsDir() {
			Printf("Export name exists, but not a directory %s\n", dirname)
			return errors.New("export name exists, but not a directory. Aborting!")
		}
		repoUnpackedName = args[0]
	} else if repoLongName != gopts.Repo {
		repoUnpackedName = fmt.Sprintf("/home/wplapper/restic/.repositoryExports/%s-new", gopts.Repo)
	} else {
		Printf("No name given for export directory. Aborting!\n")
		return errors.New("no name for export directory. Aborting!")
	}

	importDirectory := repoUnpackedName
	info, err := os.Stat(importDirectory)
	if err != nil {
		Printf("Import directory %s does not exist. Abort!\n", importDirectory)
		return errors.New("Import directory does not exist. Abort!")
	}

	if ! info.IsDir() {
		Printf("%s is not a directory. Abort!\n", importDirectory)
		return errors.New("First argument is not a directory. Abort!")
	}

	for _, basename := range []string{"all_snapshots", "all_packfiles", "all_index_info", "config"} {
		filename := importDirectory + "/" + basename
		_, err := os.Stat(filename)
		if err != nil {
			Printf("File %s does not exist. Abort!\n", filename)
			return errors.New(fmt.Sprintf("File %s does not exist. Abort!", basename))
		}
	}


	// verify that we opened the correct repository
	handle, err := os.Open(importDirectory + "/config")
	if err != nil {
		Printf("Can't open config file %s. Aborting! - error is '%v'\n",
			importDirectory + "/config", err)
		return errors.New("Can't open import config file")
	}
  defer handle.Close()

	scanner := bufio.NewScanner(handle)
	scanner.Scan()
	cfg := scanner.Text()
	config := repo.Config()
	if cfg != config.ID {
		Printf("Configuration mismatch - wrong repository!\n")
		Printf("having %s\n", config.ID[:12])
		Printf("wanted %s\n", cfg[:12])
		return errors.New("Configuration mismatch - wrong repository!")
	}

	// access the database
	// step 4.1: get database name
	db_name, err := database_via_cache(repo, ctx)
	if err != nil {
		Printf("db_verify: could not copy database from backend %v\n", err)
		return err
	}

	//if importMetaOptions.New {
	os.Remove(db_name)
	//}

	// step 4.2: open selected database
	db_conn, err := sqlite.OpenDatabase(db_name, true, gopts.Verbose, true)
	if err != nil {
		Printf("OpenDatabase failed, error is %v\n", err)
		return err
	}
	Printf("database name is %s\n", db_name)

	tx, err := db_conn.Beginx()
	if err != nil {
		Printf("Can't start transaction. Error is %v\n", err)
		return err
	}

	changes_made := false
	PrintTableCounts(tx)
	table_column_names, err := GetColumnNames(tx)

	// gather table data
	SnapshotsTable, err := ProcessSnaphotsTable(tx, importDirectory + "/all_snapshots",
		table_column_names,	&changes_made)
	if err != nil {
		return err
	}
	//ManageDeleteRows(tx, SnapshotsTable, "snapshots",  &changes_made)

	PackfilesTable, err := ProcessPackfilesTable(tx, importDirectory + "/all_packfiles",
		table_column_names, &changes_made)
	if err != nil {
		return err
	}

	IndexRepoTable, reverseIndexRepo, err := ProcessIndexRecordsTable(tx, importDirectory + "/all_index_info",
		table_column_names,	&changes_made, PackfilesTable)
	if err != nil {
		return err
	}

	metaDataAll, err := ProcessMetaDataDetails(tx, importDirectory + "/metadata",
		table_column_names, &changes_made, IndexRepoTable)
	if err != nil {
		return err
	}

	// make DirectoryMap
	children, directoryNamesMapUnq := MakeChildrenMapUnique(metaDataAll, IndexRepoTable)
	metaDirMap := BuildMetaDirMap(SnapshotsTable, IndexRepoTable, children)

	_, err = ProcessMetaDirTable(tx, table_column_names, &changes_made,
		metaDirMap, SnapshotsTable)
	if err != nil {
		return err
	}

	_, err = ProcessContentsTable(tx, table_column_names, &changes_made, IndexRepoTable,
		reverseIndexRepo, metaDataAll)
	if err != nil {
		return err
	}

	namesTable, err := ProcessNamesTable(tx, table_column_names, &changes_made, metaDataAll)
	if err != nil {
		return err
	}

	//fileDataTable, err := ProcessFileDataTable(tx, table_column_names, &changes_made, namesTable, metaDataAll)
	_, err = ProcessFileDataTable(tx, table_column_names, &changes_made, namesTable, metaDataAll)
	if err != nil {
		return err
	}

	fullpath := MakeFullPath(children, directoryNamesMapUnq, SnapshotsTable, IndexRepoTable,
		metaDataAll)

	//_, pathDirTable, err := ProcessFullNameTable(tx, table_column_names, &changes_made, fullpath, children)
	_, _, err = ProcessFullNameTable(tx, table_column_names, &changes_made, fullpath, children)
	if err != nil {
		return err
	}

	/* DELETE FROM tables
	ManageDeleteRows(tx, PackfilesTable, "packfiles",  &changes_made)
	ManageDeleteRows(tx, IndexRepoTable, "index_repo", &changes_made)
	//ManageDeleteRows(tx, metaDirTable,   "meta_dir",   &changes_made)
	ManageDeleteRows(tx, namesTable,     "names",      &changes_made)
	//ManageDeleteRows(tx, contentsTable,  "contents",   &changes_made)
	ManageDeleteRows(tx, fileDataTable,  "idd_file",   &changes_made)
	ManageDeleteRows(tx, pathDirTable,   "dir_path_id", &changes_made)
	ManageDeleteRows(tx, IndexRepoTable, "index_repo", &changes_made)
	*/

	// consistency check
	res := CheckForeignKeys(tx, importMetaOptions.Echo)
	Printf("Consistency check %v\n", res)

	if changes_made {
		err = tx.Commit()
		if err != nil {
			Printf("COMMIT error %v\n", err)
			return err
		}
		Printf("COMMIT\n")

		err = write_back_database(db_name, repo, ctx)
		if err != nil {
			Printf("write_back_database failed - error is %v\n", err)
			return err
		}

		// copy cached database to /home/wplapper/restic/db/<repo-name>.db
		filename := fmt.Sprintf("/home/wplapper/restic/db/%s.db", gopts.Repo)
		Printf("database filename is %s\n", filename)
		source, err := os.Open(db_name)
		if err != nil {
			return err
		}
		defer source.Close()

		destination, err := os.Create(filename)
		if err != nil {
			return err
		}
		defer destination.Close()

    _, err = io.Copy(destination, source)
		if err != nil {
			return err
		}
	} else {
		tx.Rollback()
		Printf("ROLLBACK\n")
		changes_made = false
	}
	return nil
}

func BuildMetaDirMap(SnapshotsTable  map[string]SnapshotRecordMem,
IndexRepoTable map[string]IndexRepoRecordMem,
children map[IntID]mapset.Set[IntID]) (metaDirMap map[string]mapset.Set[IntID]) {

	// MetaDirMap snap_id -> set of all participating meta blobs
	metaDirMap = map[string]mapset.Set[IntID]{}
	for snap_id, sn := range SnapshotsTable {
		treeRoot := sn.Snap_root
		treeRootRow, ok := IndexRepoTable[treeRoot]
		if ! ok {
			Printf("Internal inconsistency in root %s for snap %s. Aborting!\n", snap_id, treeRoot[:12])
			panic("Internal inconsistency for snap. Aborting!")
		}

		metaDirMap[snap_id] = TopologyStructure(IntID(treeRootRow.Id),
			children)
	}
	return metaDirMap
}

// build a topology structure for one snapshot
// the function relies on 'children' being initialized properly
func MakeFullPath(children map[IntID]mapset.Set[IntID],
directoryNamesMapUnq map[IntID]map[IntID]string, SnapshotsTable map[string]SnapshotRecordMem,
IndexRepoTable map[string]IndexRepoRecordMem, metaDataAll map[IntID]*restic.Tree) (fullpath map[IntID]string) {

	// initialize
	fullpath = map[IntID]string{}
	initials := mapset.NewSet[IntID]()

	for _, row := range SnapshotsTable {
		blob := row.Snap_root
		row2, ok := IndexRepoTable[blob]
		if ! ok {
			Printf("Internal inconsistency for snap root %s. Aborting!\n", blob[:12])
			panic("Internal inconsistency for snap root. Aborting!")
		}
		initials.Add(IntID(IntID(row2.Id)))
		fullpath[IntID(row2.Id)] = "/"
	}

	dfs_res := dfs(children, initials)
	for _, parent := range dfs_res {
		if parent == EMPTY_NODE_ID_TRANSLATED { continue }
		for child := range children[parent].Iter() {
			if child == EMPTY_NODE_ID_TRANSLATED { continue }
			if _, ok := fullpath[child]; ! ok {
				if fullpath[parent] == "/" {
					fullpath[child] = "/" + directoryNamesMapUnq[child][parent]
				} else {
					fullpath[child] = fullpath[parent] + "/" + directoryNamesMapUnq[child][parent]
				}
			}
		}
	}

	// rename roots of snaps
	for snap_id, row := range SnapshotsTable {
		row2 := IndexRepoTable[row.Snap_root]
		fullpath[IntID(row2.Id)] = fmt.Sprintf("/ (root of %s)", snap_id)
	}

	initials = nil // for GC
	return fullpath
}

func MakeChildrenMapUnique(metaDataAll map[IntID]*restic.Tree,
IndexRepoTable map[string]IndexRepoRecordMem) (childrenMap map[IntID]mapset.Set[IntID],
directoryNamesMap map[IntID]map[IntID]string) {

	// setup empty maps
	childrenMap = map[IntID]mapset.Set[IntID]{}
	directoryNamesMap = map[IntID]map[IntID]string{}

	for parent_int, tree := range metaDataAll {
		// setup new empty map for 'parent_int'
		childrenMap[parent_int] = mapset.NewSet[IntID]()
		for _, node := range tree.Nodes {
			if node.Type == "dir" {
				child := node.Subtree.String()
				child_row, ok := IndexRepoTable[child]
				if ! ok {
					Printf("internal inconsistency for child %s\n", child[:12])
					panic("internal inconsistency for child")
				}
				if IntID(child_row.Id) == EMPTY_NODE_ID_TRANSLATED { continue }

				child_int := IntID(child_row.Id)
				childrenMap[parent_int].Add(child_int)
				_, ok = directoryNamesMap[child_int]; if ! ok {
					directoryNamesMap[child_int] = map[IntID]string{}
				}
				directoryNamesMap[child_int][parent_int] = node.Name
			}
		}
	}
	return childrenMap, directoryNamesMap
}

