package main

import (
	// system
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"io"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	//argparse
	"github.com/spf13/cobra"

	// restic library
	"github.com/wplapper/restic/library/restic"
	"github.com/wplapper/restic/library/repository"

	// sets
	"github.com/deckarep/golang-set/v2"
)

type ExportOptions struct {
	Debug int
}
var exportOptions ExportOptions

var cmdCreateSql = &cobra.Command{
	Use:   "wpl-create-sql [flags]",
	Short: "create sql statements for a SQLite database to be generated",
	Long: `create sql statements for a SQLite database to be generated.

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
	DisableAutoGenTag: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runCreateSql(cmd.Context(), cmd, globalOptions)
	},
}

func init() {
	cmdRoot.AddCommand(cmdCreateSql)
	f := cmdCreateSql.Flags()
	f.IntVarP(&exportOptions.Debug, "debug", "D", 0, "debug options")
}

type Driver struct {
	Stdin io.WriteCloser
	Debug bool
}

func runCreateSql(ctx context.Context, cmd *cobra.Command, gopts GlobalOptions) error {

	var (
		repositoryData RepositoryData
		err error
		startTime        time.Time
	)

	// startup
	startTime = time.Now()
	gOptions = gopts
	init_repositoryData(&repositoryData)

	// step 1: open repository
	repo, err := OpenRepository(ctx, gopts)
	if err != nil { return err }
	Verboseff("Repository is %s\n", gopts.Repo)
	Verboseff("%-35s %7.1f seconds\n", "repository open",
		time.Since(startTime).Seconds())

	// step 2: gather the base information
	err = gather_base_data_repo(repo, gopts, ctx, &repositoryData,
		repo_details_options.Timing)
	if err != nil {
		return err
	}
	Verboseff("%-35s %7.1f seconds\n", "gather base data",
		time.Since(startTime).Seconds())

	dataBaseFilename := fmt.Sprintf("/home/wplapper/restic/db/%s.db",
		filepath.Base(gopts.Repo))
	_, err = os.Create(dataBaseFilename)
	if err != nil {
		Printf	("Cant Create database - reason '%v'\n", err)
		return err
	}

	err = os.Chown(dataBaseFilename, 1000, 1000)
	if err != nil {
		Printf("Cant Chown database - reason '%v'\n", err)
		return err
	}

	scmd := exec.Command("/usr/bin/sqlite3", dataBaseFilename)
	Stdin, err := scmd.StdinPipe()
	if err != nil {
		Printf("Cant Create Pipe to STDIN - reason '%v'\n", err)
		return err
	}

	go func() {
		defer Stdin.Close()
		driver := &Driver{Stdin, false}
		GatherDatabaseData(ctx, repo, driver, &repositoryData, startTime)
	}()

	out, err := scmd.CombinedOutput()
	if err != nil {
		Printf("CombinedOutput failed - reason '%v'\n", err)
		Printf("%s\n", out)
		return err
	}
	if out != nil {
		Printf("%s\n", out)
	}
	return nil
}

func GatherDatabaseData(ctx context.Context, repo *repository.Repository,
driver *Driver, repositoryData *RepositoryData, startTime time.Time) (err error) {
	/*
	 * sequence of events is as follows:
	 * the following sequential text files will be written
	 * 00-startup
	 * 01-create_tables
	 * 02-snapshots
	 * 03-packfiles
	 * 04-index_repo
	 * 05-names
	 * metaBlobSorter
	 * 06-meta_data_storage
	 * 07-contents
	 * 08-children
	 * 09-fullname
	 * 10-dir_path
	 * 11-snap_blobs
	 * 12-create_index
	 * 13-finale

	 * 00-startup is `PRAGMA foreign_keys=OFF;BEGIN EXCLUSIVE TRANSACTION;`
	 * each of the files 02-xx ... 11-xx will contain
	 * INSERT INTO TABLE ttt VALUES(...);
	 * 13-finale is `COMMIT;VACCUM;`
	 */

	// get all snapshots
	//snapSlice := repositoryData.Snaps
	step_00_startup(driver, startTime)
	step_01_createTables(driver, startTime)
	snapMap := step_02_snapshots(driver, repositoryData, startTime) // 02-snapshots
	step_03_04_ix_files(driver, repositoryData, startTime) // 03-packfiles, 04-index_repo
	namesMap:= step_05_names(driver, repositoryData, startTime)
	metaBlobs := MetaBlobSorter(repositoryData)
	_ = metaBlobs
	step_06_meta_data_storage(ctx, repo, driver, repositoryData, namesMap, metaBlobs, startTime)
	step_07_contents(driver, repositoryData, metaBlobs, startTime)
	step_08_children(driver, repositoryData, startTime)
	step_09_10_fullname_dirpath(driver, repositoryData, metaBlobs, startTime)
	step_11_snap_blobs(driver, repositoryData, snapMap, metaBlobs, startTime)
	step_12_createIndex(driver, startTime)
	step_13_finale(driver, startTime)

	Verboseff("%-35s %7.1f seconds\n", "GatherDatabaseData",
		time.Since(startTime).Seconds())
	return nil
}

// start the export file
func step_00_startup(tx *Driver, start time.Time) {
	io.WriteString(tx.Stdin, "PRAGMA foreign_keys=OFF;\n")
	io.WriteString(tx.Stdin, "BEGIN EXCLUSIVE TRANSACTION;\n")
}

// and CREATE TABLEs
func step_01_createTables(tx *Driver, start time.Time) {
	for _, sql := range CreateTablesSlice {
		io.WriteString(tx.Stdin, sql + ";\n\n")
	}
}

// gather data about snapshots
func step_02_snapshots(tx *Driver, repositoryData *RepositoryData,
startTime time.Time) (snapMap map[restic.ID]int) {

	/*
	type SnapshotRow struct {
		Id        int
		Snap_id   []byte
		Snap_time string
		Snap_host string
		Snap_fsys string
		Snap_root []byte
	}
	*/
	//snapSlice = []SnapshotRow{}
	high_id := 1
	snapMap = map[restic.ID]int{}
	// repositoryData.Snaps is already sorted
	for _, sn := range repositoryData.Snaps {
		line := fmt.Sprintf(
		  "INSERT INTO snapshots VALUES(%d,X'%s','%s','%s','%s',X'%s');\n",
			high_id,
			hex.EncodeToString(sn.ID()[:]),
			sn.Time.Format(time.DateTime),
			sn.Hostname, sn.Paths[0],
			hex.EncodeToString(sn.Tree[:]))
			io.WriteString(tx.Stdin, line)
		snapMap[*(sn.ID())] = high_id
		high_id++
	}
	return snapMap
}

type PackfileRecordRow struct {
	Id          int
	Packfile_id []byte
	Type        string
}

// gather information about packfiles and the index structure
func step_03_04_ix_files(tx *Driver, repositoryData *RepositoryData,
start time.Time) {

	// gather all packfile IDs
	packMap := map[restic.ID]PackfileRecordRow{}
	packSet := mapset.NewSet[int]()
	for _, ih := range repositoryData.IndexHandle {
		if packSet.Add(int(ih.pack_index)) {
			pack_ID := repositoryData.IndexToBlob[ih.pack_index]
			row := PackfileRecordRow{Id: int(ih.pack_index), Packfile_id: pack_ID[:], Type: ih.Type.String()}
			packMap[pack_ID] = row
		}
	}

	// convert to Slice and sort
	packSlice := packSet.ToSlice()
	sort.Ints(packSlice)

	for _, pack_index := range packSlice {
		pack_ID := repositoryData.IndexToBlob[IntID(pack_index)]
		row := packMap[pack_ID]
		line := fmt.Sprintf("INSERT INTO packfiles VALUES(%d,X'%s','%s');\n",
			pack_index, hex.EncodeToString(pack_ID[:]), row.Type)
		io.WriteString(tx.Stdin, line)
	}
	packMap = nil
	packSet = nil
	Verboseff("%-35s %7.1f seconds\n", "INSERT INTO packfiles",
		time.Since(start).Seconds())

	/*
	type IndexRepoRecordRow struct {
		Id                 IntID
		Blob               []byte // == UNIQUE
		Type               string
		Length             int
		UncompressedLength int
		Pack__id           int // back pointer to packfiles
	}
	*/

	blobSet := mapset.NewSet[int]()
	for _, ih := range repositoryData.IndexHandle {
		blobSet.Add(int(ih.blob_index))
	}

	// convert to Slice and sort
	blobSlice := blobSet.ToSlice()
	sort.Ints(blobSlice)

	for _, blob_index := range blobSlice {
		blob_ID := repositoryData.IndexToBlob[IntID(blob_index)]
		ih := repositoryData.IndexHandle[blob_ID]

		line := fmt.Sprintf("INSERT INTO index_repo VALUES(%d,X'%s','%s',%d,%d,%d);\n",
			blob_index, hex.EncodeToString(blob_ID[:]), ih.Type.String(),
			ih.size, ih.UncompressedLength,
			ih.pack_index)
		io.WriteString(tx.Stdin, line)
	}

	Verboseff("%-35s %7.1f seconds\n", "INSERT INTO index_repo",
		time.Since(start).Seconds())
}


// collect all names from repository
func step_05_names(tx *Driver, repositoryData *RepositoryData,
start time.Time) (namesMap map[string]int) {

	// gather names
	namesSet := mapset.NewSet[string]()
	for _, fileList := range repositoryData.DirectoryMap {
		for _, node := range fileList {
			name := node.name
			if strings.Index(name, "'") >= 0 {
				name = strings.ReplaceAll(name, "'", "''")
			}
			namesSet.Add(name)
		}
	}

	// convert Set to Slice for sorting
	namesSlice := namesSet.ToSlice()
	sort.Strings(namesSlice)

	high_id := 1
	namesMap = map[string]int{}
	for _, name := range namesSlice {
		line := fmt.Sprintf("INSERT INTO names VALUES(%d,'%s');\n", high_id, name)
		io.WriteString(tx.Stdin, line)
		namesMap[name] = high_id
		high_id++
	}

	Verboseff("%-35s %7.1f seconds\n", "INSERT INTO names",
		time.Since(start).Seconds())
	return namesMap
}

// create a Slice of sorted metaBlobs
func MetaBlobSorter(repositoryData *RepositoryData) (metaBlobs []IntID) {
	metaBlobs = []IntID{}
	for _, ih := range repositoryData.IndexHandle {
		if ih.Type == restic.TreeBlob {
			metaBlobs = append(metaBlobs, ih.blob_index)
		}
	}
	sort.Slice(metaBlobs, func (i,j int) bool {
		return metaBlobs[i] < metaBlobs[j]
	})
	return metaBlobs
}

func step_06_meta_data_storage(ctx context.Context, repo *repository.Repository,
tx *Driver, repositoryData *RepositoryData, namesMap map[string]int,
metaBlobs []IntID, start time.Time) (err error) {

	/*
	type MetaDataStorageRow struct {
		Id       IntID // ptr to 'index_repo' TABLE
		Position int   // offset in nodes list
		Name__id int   // ptr to 'names' TABLE
		// copied from original Node:
		Type        string
		Mode        os.FileMode
		Mtime       string
		Inode       uint64
		DeviceID    uint64
		Size        uint64
		Links       uint64
		Subtree__id IntID
		LinkTarget  string
	}
	*/
	// we have to reload all tree records, try sequentially first
	for blob, ih := range repositoryData.IndexHandle {
		metaBlobIntID := repositoryData.BlobToIndex[blob]
		if ih.Type == restic.TreeBlob {
			tree, err := restic.LoadTree(ctx, repo, blob)
			if err != nil {
				Printf("LoadTree returned %v\n", err)
				return err
			}

			for position, node := range tree.Nodes {
				var size, links, subtree__id, linkTarget string
				// check for possible NULL entries
				if node.Size == 0 {
					size = "NULL"
				} else {
					size = fmt.Sprintf("%d", node.Size)
				}
				if node.Links == 0 {
					links = "NULL"
				} else {
					links = fmt.Sprintf("%d", node.Links)
				}
				if node.Type == "dir" {
					subtree__id = fmt.Sprintf("%d", repositoryData.BlobToIndex[*node.Subtree])
				} else {
					subtree__id = "NULL"
				}
				if node.LinkTarget == "" {
					linkTarget = "NULL"
				} else {
					linkTarget = node.LinkTarget
					if strings.Index(linkTarget, "'") >= 0 {
						linkTarget = strings.ReplaceAll(linkTarget, "'", "''")
					}
					linkTarget = fmt.Sprintf("'%s'", linkTarget)
				}

				line := fmt.Sprintf("INSERT INTO meta_data_store VALUES(%d,%d,%d,'%s',%d,'%s',%d,%d,%s,%s,%s,%s);\n",
					metaBlobIntID, position, namesMap[node.Name], node.Type, node.Mode, node.ModTime.Format(time.DateTime),
					node.Inode, node.DeviceID, size, links, subtree__id, linkTarget)
				io.WriteString(tx.Stdin, line)
			}
		}
	}

	Verboseff("%-35s %7.1f seconds\n", "INSERT INTO meta_data_store",
		time.Since(start).Seconds())
	return nil
}

// collect all names from repository
func step_07_contents(tx *Driver, repositoryData *RepositoryData,
metaBlobs []IntID, start time.Time) {

	/*
	type ContentsRecordRow struct {
		Blob__id IntID // map back to index_repo.id (owning directory / meta_blob)
		Position int   // with triple (Id_blob, Position, Offset) == UNIQUE
		Offset   int
		Data__id IntID // map back to index_repo.id (data)
	}
	*/
	for _, meta_blob := range metaBlobs {
		for position, node := range repositoryData.DirectoryMap[meta_blob] {
			for offset, cont := range(node.content) {
				line := fmt.Sprintf("INSERT INTO contents VALUES(%d,%d,%d,%d);\n",
					meta_blob, position, offset, cont)
				io.WriteString(tx.Stdin, line)
			}
		}
	}

	Verboseff("%-35s %7.1f seconds\n", "INSERT INTO contents",
		time.Since(start).Seconds())
}

func step_08_children(tx *Driver, repositoryData *RepositoryData, start time.Time) {
// children is 'map[IntID]mapset.Set[IntID]'

	children := CreateAllChildren(repositoryData)
	parentSet := mapset.NewSet[int]()
	for parent := range children {
		parentSet.Add(int(parent))
	}
	// convert to Slice and sort
	parentSlice := parentSet.ToSlice()
	sort.Ints(parentSlice)

	for _, parent := range parentSlice {
		childSet := children[IntID(parent)]
		// convert to Slice and sort
		childSlice := childSet.ToSlice()
		sort.Slice(childSlice, func (i, j int) bool{
			return childSlice[i] < childSlice[j]
		})

		for _, child := range childSlice {
			line := fmt.Sprintf("INSERT INTO children VALUES(%d,%d);\n",
				int(parent), int(child))
			io.WriteString(tx.Stdin, line)
		}
	}

	Verboseff("%-35s %7.1f seconds\n", "INSERT INTO children",
		time.Since(start).Seconds())
}

// TABLEs fullname and dir_path, derived from repositoryData.FullPath
func step_09_10_fullname_dirpath(tx *Driver, repositoryData *RepositoryData,
metaBlobs []IntID, start time.Time) {

	// reverseMap is a 'map[string]mapset.Set[IntID]'
	reverseMap := CreateReverseFullpath(repositoryData)

	// extract all fullnames
	fullnameSlice := []string{}
	for fullname := range reverseMap {
		if strings.Index(fullname, "'") >= 0 {
			fullname = strings.ReplaceAll(fullname, "'", "''")
		}
		fullnameSlice = append(fullnameSlice, fullname)
	}
	sort.Strings(fullnameSlice)

	fullnameMap := map[string]int{}
	for ix, fullname := range fullnameSlice {
		fullnameMap[fullname] = ix + 1
		line := fmt.Sprintf("INSERT INTO fullname VALUES(%d,'%s');\n", ix + 1, fullname)
		io.WriteString(tx.Stdin, line)
	}

	/*
	type DirPathRow struct {
		Id           IntID
		Fullname__id int
	}
	*/
 	for _, meta_blob := range metaBlobs {
		fullname, ok := repositoryData.FullPath[meta_blob]
		if !ok {
			continue
		}
		fullname__id := fullnameMap[fullname]
		line := fmt.Sprintf("INSERT INTO dir_path VALUES(%d,%d);\n",
			int(meta_blob), fullname__id)
		io.WriteString(tx.Stdin, line)
	}

	Verboseff("%-35s %7.1f seconds\n", "INSERT INTO fullname and dir_path",
		time.Since(start).Seconds())
}

// TABLE snap_blobs, derived from repositoryData.MetaDirMap
func step_11_snap_blobs(tx *Driver, repositoryData *RepositoryData, snapMap map[restic.ID]int,
metaBlobs []IntID, start time.Time) {

	for _, sn := range repositoryData.Snaps {
		snap_id := snapMap[*sn.ID()]
		id_ptr := Ptr2ID(*sn.ID(), repositoryData)
		blobSet := repositoryData.MetaDirMap[id_ptr]
		blobSlice := blobSet.ToSlice()
		sort.Slice(blobSlice, func (i,j int) bool {
			return blobSlice[i] < blobSlice[j]
		})
		for _, meta_blob := range blobSlice {
			line := fmt.Sprintf("INSERT INTO snap_blobs VALUES(%d,%d);\n",
				int(snap_id), int(meta_blob))
			io.WriteString(tx.Stdin, line)
		}
	}

	Verboseff("%-35s %7.1f seconds\n", "INSERT INTO snap_blobs",
		time.Since(start).Seconds())
}

// create all INDEX structures after all data have been inserted
func step_12_createIndex(tx *Driver, start time.Time) {
	for _, sql := range CreateIndexSlice {
		io.WriteString(tx.Stdin, sql + ";\n")
	}
}

// database export final steps
func step_13_finale(tx *Driver, start time.Time) {
	io.WriteString(tx.Stdin, "COMMIT;\n")
	io.WriteString(tx.Stdin, "VACUUM;\n")
}

// table definitions
var CreateTablesSlice = []string{
`CREATE TABLE snapshots (
  id INTEGER PRIMARY KEY,             -- ID of snapshots row
  snap_id BLOB NOT NULL,              -- snap ID, UNIQUE INDEX
  snap_time TEXT NOT NULL,            -- time of snap, truncated to seconds
  snap_host TEXT NOT NULL,            -- host wich is to be backep up
  snap_fsys TEXT NOT NULL,            -- filesystem to be backup up
  snap_root BLOB NOT NULL             --  the root of the snap
)`,

`CREATE TABLE packfiles (
  id INTEGER PRIMARY KEY,             -- the primary key
  packfile_id BLOB NOT NULL,          -- the packfile ID, UNIQUE INDEX, as []byte, len 32
  type TEXT                           -- type of packfile data/tree
)`,

`CREATE TABLE index_repo (
  id INTEGER PRIMARY KEY,             -- the primary key
  blob BLOB NOT NULL,                 -- the idd, UNIQUE INDEX, as []byte, len 32
  type TEXT NOT NULL,                 -- type tree / data
  length INTEGER NOT NULL,            -- length of blob
  uncompressedlength INTEGER NOT NULL,-- uncompressed length
  pack__id INTEGER NOT NULL,          -- back ptr to packflies, INDEX
  FOREIGN KEY(pack__id)               REFERENCES packfiles(id)
)`,

`CREATE TABLE names (
  id INTEGER PRIMARY KEY,             -- the primary key
  name TEXT                           -- all names collected from restic system
)`,

`CREATE TABLE meta_data_store (
  blob__id INTEGER NOT NULL,          -- back ptr to index_repo PRIMARY KEY, part 1
  position INTEGER NOT NULL,          -- offset into slice      PRIMARY KEY, part 2
  name__id INTEGER NOT NULL,          -- pointer to name
  type TEXT NOT NULL,
  mode INTEGER NOT NULL,
  mtime TEXT NOT NULL,
  inode INTEGER NOT NULL,
  deviceid INTEGER NOT NULL,
  size INTEGER,                       -- can be NULL for dir,       symlink
  links INTEGER,                      -- can be NULL for dir
  subtree__id INTEGER,                -- can be NULL for      file, symlink
  linktarget TEXT,                    -- can be NULL for dir, file

  PRIMARY KEY (blob__id, position),   -- the composite PRIMARY KEY
  FOREIGN KEY(blob__id)               REFERENCES index_repo(id)
  FOREIGN KEY(name__id)               REFERENCES name(id)
) WITHOUT ROWID`,

`CREATE TABLE contents (
  blob__id INTEGER NOT NULL,          -- ptr to idd_file, needs INDEX
  position INTEGER NOT NULL,          -- position in idd_file
  offset   INTEGER NOT NULL,          -- the offset of the contents list
  data__id INTEGER NOT NULL,          -- ptr to data_idd, INDEX
  PRIMARY KEY(blob__id, position, offset),
  FOREIGN KEY(data__id)               REFERENCES index_repo(id),
  FOREIGN KEY(blob__id)               REFERENCES index_repo(id)
) WITHOUT ROWID`,

`CREATE TABLE children (
    parent__id INTEGER NOT NULL,      -- parent entry
    child__id  INTEGER NOT NULL,      -- child  entry

    PRIMARY KEY(parent__id, child__id),
    FOREIGN KEY(parent__id)           REFERENCES index_repo(id),
    FOREIGN KEY(child__id)            REFERENCES index_repo(id)
) WITHOUT ROWID`,

`CREATE TABLE fullname (
    id INTEGER PRIMARY KEY,           -- the primary key, GENERIC ascending
    pathname TEXT NOT NULL            -- full pathname of directory UNIQUE INDEX
)`,

`CREATE TABLE dir_path (
    id INTEGER PRIMARY KEY,           -- the primary key
    fullname__id INTEGER NOT NULL,    -- ptr to "fullname", INDEX
    FOREIGN KEY(fullname__id)         REFERENCES fullname(id),
    FOREIGN KEY(id)                   REFERENCES index_repo(id)
)`,

`CREATE TABLE snap_blobs (
  snap__id INTEGER NOT NULL,          -- the snap_id , INDEX
  blob__id INTEGER NOT NULL,          -- the idd pointer, INDEX
  PRIMARY KEY(snap__id, blob__id),
  FOREIGN KEY(snap__id)               REFERENCES snaphots(id),
  FOREIGN KEY(blob__id)               REFERENCES index_repo(id)
) WITHOUT ROWID`,
}

var CreateIndexSlice = []string{
	// UNIQUE INDEX - alternate primary key
	`CREATE UNIQUE INDEX ux_ss_snap_id  ON snapshots(snap_id)`,
	`CREATE UNIQUE INDEX ux_ixr_blob    ON index_repo(blob)`,
	`CREATE UNIQUE INDEX ux_pack_pack   ON packfiles(packfile_id)`,
	`CREATE UNIQUE INDEX ux_name_name   ON names(name)`,
	`CREATE UNIQUE INDEX ux_fname_path  ON fullname(pathname)`,
	// normal INDEX
	`CREATE        INDEX ix_ixr_pack    ON index_repo(pack__id)`,
	`CREATE        INDEX ix_ixr_type    ON index_repo(type)`,
	`CREATE        INDEX ix_cont_blob   ON contents(blob__id)`,
	`CREATE        INDEX ix_cont_data   ON contents(data__id)`,
	`CREATE        INDEX ix_sb_snap     ON snap_blobs(snap__id)`,
	`CREATE        INDEX ix_sb_blob     ON snap_blobs(blob__id)`,
	`CREATE        INDEX ix_child_paren ON children(parent__id)`,
	`CREATE        INDEX ix_child_child ON children(child__id)`,
	`CREATE        INDEX ix_mds_blob    ON meta_data_store(blob__id)`,
	`CREATE        INDEX ix_mds_type    ON meta_data_store(type)`,
	`CREATE        INDEX ix_mds_inode   ON meta_data_store(inode)`,
	`CREATE        INDEX ix_mds_subtree ON meta_data_store(subtree__id)`,
}
