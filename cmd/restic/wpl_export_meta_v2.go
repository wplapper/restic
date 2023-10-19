package main

import (
	// system
	"context"
	"os"
	"fmt"
	"golang.org/x/sync/errgroup"
	"sync"
	"runtime"
	"time"
	"sort"
	"encoding/hex"
	"path/filepath"

	//argparse
	"github.com/spf13/cobra"

	// restic library
	"github.com/wplapper/restic/library/restic"
	"github.com/wplapper/restic/library/repository"

  //"database/sql"
  "github.com/jmoiron/sqlx"
  _ "github.com/mattn/go-sqlite3"

  // JSON decode
	"encoding/json"
  "github.com/tidwall/gjson"
  "github.com/tidwall/sjson"

	// sets
	"github.com/deckarep/golang-set/v2"
)

var cmdExportMeta = &cobra.Command{
	Use:   "wpl-export",
	Short: "export metadata of repo to database",
	Long: `export metadata of repo to database.

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
	DisableAutoGenTag: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runExportMeta(cmd.Context(), cmd, globalOptions, args)
	},
}

func init() {
	cmdRoot.AddCommand(cmdExportMeta)
}

type IndexRepoRecordRaw struct {
	Id                 IntID
	Blob               []byte // == UNIQUE
	Type               string
	Length             int
	UncompressedLength int
	Pack__id           int // back pointer to packfiles
}

type PackfileRecordRaw struct {
	Id 					int
	Packfile_id []byte
	Type        string
}

type MetaDataStorage struct {
	Id       IntID    // ptr to index_repo
	Position int
	Name__id int
	Metadata string
}

type ContentsRecordRaw struct {
	Data__id    IntID // map back to index_repo.id (data)
	Blob__id    IntID // map back to index_repo.id (owning directory / meta_blob)
	Position    int // with triple (Id_blob, Position, Offset) == UNIQUE
	Offset      int
}

// Node is a file, directory or other item in a backup.
type Node struct {
	Name       string      `json:"name"`
	Type       string      `json:"type"`
	Mode       os.FileMode `json:"mode,omitempty"`
	ModTime    time.Time   `json:"mtime,omitempty"`
	AccessTime time.Time   `json:"atime,omitempty"`
	ChangeTime time.Time   `json:"ctime,omitempty"`
	UID        uint32      `json:"uid"`
	GID        uint32      `json:"gid"`
	User       string      `json:"user,omitempty"`
	Group      string      `json:"group,omitempty"`
	Inode      uint64      `json:"inode,omitempty"`
	DeviceID   uint64      `json:"device_id,omitempty"` // device id of the file, stat.st_dev
	Size       uint64      `json:"size,omitempty"`
	Links      uint64      `json:"links,omitempty"`
	LinkTarget string      `json:"linktarget,omitempty"`
	// implicitly base64-encoded field. Only used while encoding, `linktarget_raw` will overwrite LinkTarget if present.
	// This allows storing arbitrary byte-sequences, which are possible as symlink targets on unix systems,
	// as LinkTarget without breaking backwards-compatibility.
	// Must only be set of the linktarget cannot be encoded as valid utf8.
	//LinkTargetRaw      []byte              `json:"linktarget_raw,omitempty"`
	//ExtendedAttributes []ExtendedAttribute `json:"extended_attributes,omitempty"`
	Device             uint64              `json:"device,omitempty"` // in case of Type == "dev", stat.st_rdev
	//Content            IDs                 `json:"content"`
	Subtree            uint32                 `json:"subtree,omitempty"`

	//Error string `json:"error,omitempty"`
	//Path string `json:"-"`
}

// NodeShort is a file, directory or symlink in a backup.
type NodeShort struct {
	Type       string      `json:"type"`
	Mode       os.FileMode `json:"mode,omitempty"`
	ModTime    time.Time   `json:"mtime,omitempty"`
	Inode      uint64      `json:"inode,omitempty"`
	DeviceID   uint64      `json:"device_id,omitempty"` // device id of the file, stat.st_dev
	Device     uint64      `json:"device,omitempty"` // in case of Type == "dev", stat.st_rdev
	Size       uint64      `json:"size,omitempty"`
	Links      uint64      `json:"links,omitempty"`
	LinkTarget string      `json:"linktarget,omitempty"`
	Subtree    uint32      `json:"subtree,omitempty"`
}

type SnapshotRaw struct{
	Id        int
	Snap_id   []byte
	Snap_time string
	Snap_host string
	Snap_fsys string
	Snap_root []byte
}

type FullnameRow struct {
	Id       int
	Pathname string
}

type DirPathRow struct {
	Id           IntID
	Fullname__id int
}

type MetaDirEntry struct {
	Snap__id IntID
	Blob__id IntID
}

type ChildrenRow struct {
	Parent__id IntID
	Child__id  IntID
}

type CompIndex struct {
	MetaBlob__id IntID
	Position     int
}

type NamesRow struct {
	Id   int
	Name string
}

// convert all the metadata into a SQLite database
func runExportMeta(ctx context.Context, cmd *cobra.Command, gopts GlobalOptions, args []string) error {
	var (
		err              error
		repositoryData   RepositoryData
		database_name    string
	)

	init_repositoryData(&repositoryData)

	// step: open repository
	repo, err := OpenRepository(ctx, gopts)
	if err != nil {
		return err
	}
	defer repo.Close()
	Verboseff("Repository is %s\n", globalOptions.Repo)

	database_name = fmt.Sprintf("/home/wplapper/restic/db/%s.db", filepath.Base(gopts.Repo))
	os.Create(database_name)

	// open database
  db_conn := sqlx.MustOpen("sqlite3", database_name)
  if err != nil {
		Printf("Can't open database %s - reason %v", database_name, err)
		return err
  }
  Verboseff("database %s open\n", database_name)

	// start TRANSACTION
	tx, err := db_conn.Beginx()
	if err != nil {
		Printf("Can't start transaction. Error is %v\n", err)
		return err
	}

	// CREATE TABLE ...
	create_tables := []string{
  `CREATE TABLE snapshots (
    id INTEGER PRIMARY KEY,      -- ID of table row
    snap_id TEXT NOT NULL,       -- snap ID, UNIQUE INDEX
    snap_time TEXT NOT NULL,     -- time of snap
    snap_host TEXT NOT NULL,     -- host wich is to be backep up
    snap_fsys TEXT NOT NULL,     -- filesystem to be backup up
    snap_root TEXT NOT NULL      --  the root of the snap
  )`,

  `CREATE TABLE packfiles_raw (
    -- needed for the relationship between packfiles and blobs
    id INTEGER PRIMARY KEY,    -- the primary key
    packfile_id BLOB NOT NULL, -- the packfile ID, UNIQUE INDEX, as []byte, len 32
    type TEXT                  -- type of packfile data/tree
  )`,

  `CREATE TABLE index_repo (
    -- maintains the contents of the index/ * files
    id INTEGER PRIMARY KEY,             -- the primary key
    blob BLOB NOT NULL,                 -- the idd, UNIQUE INDEX, as []byte, len 32
    type TEXT NOT NULL,                 -- type tree / data
    length INTEGER NOT NULL,            -- length of blob
    uncompressedlength INTEGER NOT NULL,-- uncompressed length
    pack__id INTEGER NOT NULL,          -- back ptr to packflies, INDEX
    FOREIGN KEY(pack__id)               REFERENCES packfiles(id)
  )`,

	`CREATE TABLE meta_data_storage (
    id INTEGER NOT NULL,           -- back ptr to index_repo PRIMARY KEY, part 1
    position INTEGER NOT NULL,     -- offset into slice      PRIMARY KEY, part 2
    name__id INTEGER NOT NULL,     -- pointer to name
    metadata TEXT,                 -- the data
    PRIMARY KEY (id, position),    -- the composite PRIMARY KEY
    FOREIGN KEY(id)       REFERENCES index_repo(id)
    FOREIGN KEY(name__id) REFERENCES name(id)
  ) WITHOUT ROWID`,

  `CREATE TABLE contents (
    -- id INTEGER PRIMARY KEY,             -- the primary key
    data__id INTEGER NOT NULL,          -- ptr to data_idd, INDEX
    blob__id  INTEGER NOT NULL,         -- ptr to idd_file, needs INDEX
    position INTEGER NOT NULL,          -- position in idd_file
    offset   INTEGER NOT NULL,          -- the offset of the contents list
    -- the triple (id_blob, position, offset) is a UNIQUE INDEX
    PRIMARY KEY(blob__id, position, offset),
    FOREIGN KEY(data__id)               REFERENCES index_repo(id),
    FOREIGN KEY(blob__id)               REFERENCES index_repo(id)
  ) WITHOUT ROWID`,

  `CREATE TABLE names (
    id INTEGER PRIMARY KEY,             -- the primary key
    name TEXT                           -- all names collected from restic system
  )`,

  `CREATE TABLE meta_dir (
    -- many to many relationship table between snaps and directory idds
    snap__id INTEGER NOT NULL,          -- the snap_id , INDEX
    blob__id INTEGER NOT NULL,          -- the idd pointer, INDEX
    PRIMARY KEY(snap__id, blob__id),
    FOREIGN KEY(snap__id)     REFERENCES snaphots(id),
    FOREIGN KEY(blob__id)     REFERENCES index_repo(id)
  ) WITHOUT ROWID`,

  `CREATE TABLE children (
      parent__id INTEGER NOT NULL,      -- parent entry
      child__id  INTEGER NOT NULL,      -- child  entry
      FOREIGN KEY(parent__id) REFERENCES index_repo(id),
      FOREIGN KEY(child__id)  REFERENCES index_repo(id),
      PRIMARY KEY(parent__id, child__id)
  ) WITHOUT ROWID`,

  `CREATE TABLE fullname (
      id INTEGER PRIMARY KEY,           -- the primary key, GENERIC ascending
      pathname TEXT NOT NULL            -- full pathname of directory UNIQUE INDEX
  )`,

  `CREATE TABLE dir_path_id (
      id INTEGER PRIMARY KEY,           -- the primary key
      fullname__id INTEGER NOT NULL,    -- ptr to "fullname", INDEX
      FOREIGN KEY(fullname__id)         REFERENCES fullname(id),
      FOREIGN KEY(id)                   REFERENCES index_repo(id)
  )`,
	}

	for _, create_sql := range create_tables {
		tx.MustExec(create_sql)
	}

	// get all snapshots
	snaps, err := ProcessSnapshots(ctx, repo, tx)
	if err != nil {
		Printf("ProcessSnapshots failed - reason %v\n", err)
		return err
	}

	// read all index files
	indexMap, err := ProcessIndexFiles(ctx, repo, tx)
	if err != nil {
		Printf("ProcessIndexFiles failed - reason %v\n", err)
		return err
	}

	// read a blob metadata
	metadataMap, namesReverse, err := ProcessMetaDataRaw(ctx, repo, tx, indexMap)
	if err != nil {
		Printf("ProcessMetaData failed - reason %v\n", err)
		return err
	}

	// create more tables
	ProcessMetaDirTable(tx, snaps, indexMap, metadataMap, namesReverse)

	// CREATE some INDEX records in the database
	createIndex := []string{
		`CREATE UNIQUE INDEX ux_ss_snap_id  ON snapshots(snap_id)`,
		`CREATE UNIQUE INDEX ux_ixr_blob    ON index_repo(blob)`,
		`CREATE UNIQUE INDEX ux_pack_pack   ON packfiles_raw(packfile_id)`,
		`CREATE UNIQUE INDEX ux_name_name   ON names(name)`,
		`CREATE UNIQUE INDEX ux_fname_path  ON fullname(pathname)`,
		`CREATE        INDEX ix_ixr_pack    ON index_repo(pack__id)`,
		`CREATE        INDEX ix_cont_data   ON contents(data__id)`,
		`CREATE        INDEX ix_md_blob     ON meta_dir(blob__id)`,
		`CREATE        INDEX ix_child_child ON children(child__id)`,
	}
	for _, create_index := range createIndex {
		tx.MustExec(create_index)
	}

	// COMMIT
	err = tx.Commit()
	if err != nil {
		Printf("Can't COMMIT. Error is %v\n", err)
		return err
	}

	// compress database
	db_conn.MustExec("VACUUM")

	// and close database
	db_conn.Close()
	return nil
}

// gather data about snapshots
func ProcessSnapshots(ctx context.Context, repo *repository.Repository, tx *sqlx.Tx) (snaps []SnapshotRaw, err error) {

	snaps = []SnapshotRaw{}
	high_id := 1
	repo.List(ctx, restic.SnapshotFile, func(id restic.ID, size int64) error {
		sn, err := restic.LoadSnapshot(ctx, repo, id)
		if err != nil {
			Printf("restic.SnapshotFile.skip loading snap record %s! - reason: %v\n", id, err)
			return err
		}

		snaps = append(snaps, SnapshotRaw{Snap_id: id[:], Snap_host: sn.Hostname,
			Snap_time: sn.Time.String()[:19], Snap_fsys: sn.Paths[0], Snap_root: sn.Tree[:],
		})
		high_id++
		return nil
	})

	// sort by time
	sort.Slice(snaps, func(i, j int) bool {
		return snaps[i].Snap_time < snaps[j].Snap_time
	})

	insert_sql := `INSERT INTO snapshots(snap_id, snap_time, snap_host, snap_fsys, snap_root)
    VALUES(:snap_id, :snap_time, :snap_host, :snap_fsys, :snap_root)`
	result, err := tx.NamedExec(insert_sql, snaps)
	if err != nil {
		Printf("INSERT snapshots_raw failed - reason %v\n", err)
		return nil, err
	}

	count, _ := result.RowsAffected()
	Verboseff("INSERT %-18s %7d rows\n", "snapshots_raw", count)
	return snaps, nil
}

// gather information about packfiles and the index structure
func ProcessIndexFiles(ctx context.Context, repo *repository.Repository, tx *sqlx.Tx) (
// return:
indexMap map[restic.ID]IntID, err error) {

	if err = repo.LoadIndex(ctx); err != nil {
		Printf("repo.LoadIndex - failed. Error is %v\n", err)
		return nil, err
	}

	// gather all packfile IDs
	high_id := 1
	insert_pack_table := []PackfileRecordRaw{}
	insert_map := map[restic.ID]PackfileRecordRaw{}
	repo.Index().Each(ctx, func(blob restic.PackedBlob) {
		if _, ok := insert_map[blob.PackID]; ! ok {
			row := PackfileRecordRaw{
				Id: high_id, Packfile_id: blob.PackID[:], Type: blob.Type.String(),
			}
			insert_pack_table = append(insert_pack_table,row)
			insert_map[blob.PackID] = row
			high_id++
		}
	})

	err = GenericInsert[PackfileRecordRaw](tx, "packfiles_raw", insert_pack_table,
		"INSERT INTO packfiles_raw(id, packfile_id, type) VALUES(:id, :packfile_id, :type)", 2)
	if err != nil {
		return nil, err
	}

	insert_index_table := []IndexRepoRecordRaw{}
	indexMap = map[restic.ID]IntID{}
	high_id2 := IntID(1)
	repo.Index().Each(ctx, func(blob restic.PackedBlob) {
		// extract index info
		row := IndexRepoRecordRaw{
			Id: high_id2,
			Blob: blob.ID[:],
			Type: blob.Type.String(),
			Length: int(blob.Length),
			UncompressedLength: int(blob.UncompressedLength),
			Pack__id: insert_map[blob.PackID].Id,
		}
		insert_index_table = append(insert_index_table, row)
		indexMap[blob.ID] = high_id2
		high_id2++
	})

	err = GenericInsert[IndexRepoRecordRaw](tx, "index_repo", insert_index_table,
		`INSERT INTO index_repo(id, blob, type, length, uncompressedlength, pack__id)
       VALUES(:id, :blob, :type, :length, :uncompressedlength, :pack__id)`, 6)
	if err != nil {
		return nil, err
	}
	return indexMap, nil
}

// gather all metablobs from Index
func GetMetaBlobSlice(ctx context.Context, repo *repository.Repository) []restic.ID {
	metaBlobSlice := []restic.ID{}
	repo.Index().Each(ctx, func(blob restic.PackedBlob) {
		if blob.Type == restic.TreeBlob {
			metaBlobSlice = append(metaBlobSlice, blob.ID)
		}
	})
	return metaBlobSlice
}

// auxiliary function to deliver meta blobs from the index to ForAllMyTrees
// for parallel processing
func SpoutMetaBlobs(metaBlobSlice []restic.ID, fn func(id restic.ID) error) error {
	for _, blob_ID := range metaBlobSlice {
		fn(blob_ID)
	}
	return nil
}

// read metadata json strings for all meta blobs
func ProcessMetaDataRaw(ctx context.Context, repo *repository.Repository, tx *sqlx.Tx,
indexMap map[restic.ID]IntID) (
// return
metaDataMap map[CompIndex]MetaDataStorage, namesReverse map[int]string, err error) {

	metaDataMap    = map[CompIndex]MetaDataStorage{}
	metaBlobSlice := GetMetaBlobSlice(ctx, repo)

	var m sync.Mutex
	// protect MetaDataStorage from parallel processing
	metaDataNodes := []MetaDataStorage{}
	contentsNodes := []ContentsRecordRaw{}
	namesMap      := map[string]int{}
	name_ctr      := 1

	wg, ctx := errgroup.WithContext(ctx)
	chan_tree_blob := make(chan restic.ID)

	wg.Go(func() error {
		defer close(chan_tree_blob)

		// this callback function get fed the 'id'
		return SpoutMetaBlobs(metaBlobSlice, func(id restic.ID) error {
			select {
			case <-ctx.Done():
				return nil
			case chan_tree_blob <- id:
				return nil
			}
			return nil
		})
	})

	// a worker receives a metablob ID from chan_tree_blob, loads the JSON data
	// and runs fn with id, the snapshot and the error
	// we split off contents and store the results in 'contentsNodes',
	// to be INSERTed INTO 'contents' later.
	worker := func() error {
		for id := range chan_tree_blob {
			json_buf, err := repo.LoadBlob(ctx, restic.TreeBlob, id, nil)
			if err != nil {
				Printf("LoadBlob returned %v\n", err)
				return err
			}

			ix := indexMap[id]
			// split the nodes into a slice
			for position, node := range gjson.GetBytes(json_buf, "nodes").Array() {
				name    := gjson.Get(node.String(), "name").String() // files only
				content := gjson.Get(node.String(), "content") // files only
				subtree := gjson.Get(node.String(), "subtree") // dir only

				cmpix := CompIndex{ix, position}
				var row MetaDataStorage
				var name__id int

				// make sure that we have exclusive control over 'metaDataNodes' and 'contentsNodes'
				m.Lock()
				name__id, ok := namesMap[name]; if !ok {
					namesMap[name] = name_ctr
					name__id = name_ctr
					name_ctr++
				}

				if subtree.String() != "" {
					meta__id, _ := restic.ParseID(subtree.Str)
					sub_ID := indexMap[meta__id]

					// and replace
					replaced, _ := sjson.Set(node.String(), "subtree", sub_ID)
					replaced, _  = sjson.Delete(replaced, "content")
					replaced, _  = sjson.Delete(replaced, "name")
					row = MetaDataStorage{ix, position, name__id, MakeShortNode(replaced)}
				} else if content.String() != "" {
					for offset, cont := range content.Array() {
						// convert cont to restic.ID
						data__id, _ := restic.ParseID(cont.String())
						contentsNodes = append(contentsNodes, ContentsRecordRaw{
							Data__id: indexMap[data__id], Blob__id: ix, Position: position, Offset: offset})
					}

					contents_removed, _ := sjson.Delete(node.String(), "content")
					contents_removed, _  = sjson.Delete(contents_removed, "name")
					row = MetaDataStorage{ix, position, name__id, MakeShortNode(contents_removed)}
				} else {
					name_removed, _  := sjson.Delete(node.String(), "name")
					row = MetaDataStorage{ix, position, name__id, MakeShortNode(name_removed)}
				}
				metaDataNodes = append(metaDataNodes, row)
				metaDataMap[cmpix] = row
				m.Unlock()
			}
		}
		return nil
	}

	// start all these parallel workers
	max_parallel := int(repo.Connections()) + runtime.GOMAXPROCS(0)
	for i := 0; i < max_parallel; i++ {
		wg.Go(worker)
	}
	res := wg.Wait()
	if res != nil {
		Printf("waitgroup Wait failed - error is %v\n", res)
		return nil, nil, res
	}
	// === finished parallel work ==============================================//

	// names collector
	namesSorted := []NamesRow{}
	namesReverse = map[int]string{}
	for name, index := range namesMap {
		namesSorted = append(namesSorted, NamesRow{index, name})
		namesReverse[index] = name
	}
 	sort.Slice(namesSorted, func(i,j int) bool {
		return namesSorted[i].Name < namesSorted[j].Name
	})

	// gather names for INSERT
	err = GenericInsert[NamesRow](tx, "names", namesSorted,
		`INSERT INTO names(id, name) VALUES(:id, :name)`, 2)
	if err != nil {
		return nil, nil, err
	}

	err = GenericInsert[MetaDataStorage](tx, "meta_data_storage", metaDataNodes,
		`INSERT INTO meta_data_storage(id, position, name__id, metadata)
    VALUES(:id, :position, :name__id, :metadata)`, 4)
	if err != nil {
		return nil, nil, err
	}

	err = GenericInsert[ContentsRecordRaw](tx, "contents", contentsNodes,
		`INSERT INTO contents(data__id, blob__id, position, offset)
    VALUES(:data__id, :blob__id, :position, :offset)`, 4)
	if err != nil {
		return nil, nil, err
	}
	return metaDataMap, namesReverse, nil
}

// copy selected fields from full Node and create the JSON version of it.
func MakeShortNode(buf string) (result string) {
	node := Node{}
	err := json.Unmarshal([]byte(buf), &node)
	if err != nil {
		panic("Unable to Unmarshal node string")
	}

	nodeShort := NodeShort{
		ModTime: node.ModTime, Size: node.Size, Links: node.Links,
		Type: node.Type, Mode: node.Mode,
		Inode: node.Inode, DeviceID: node.DeviceID, Device: node.Device,
		LinkTarget: node.LinkTarget, Subtree: node.Subtree,
	}
	buf2, err := json.Marshal(nodeShort)
	return string(buf2)
}

// extract data for tables 'children', 'meta_dir', 'fullnames'
func ProcessMetaDirTable(tx *sqlx.Tx, snaps []SnapshotRaw, indexMap map[restic.ID]IntID,
metadataMap map[CompIndex]MetaDataStorage, namesReverse map[int]string) {

	// find out about EMPTY_NODDE_ID
	var EMPTY_NODE_ID_TRANSLATED IntID
	EMPTY_NODE_ID := restic.Hash([]byte("{\"nodes\":[]}\n"))
	EMPTY_NODE_ID_TRANSLATED, ok := indexMap[EMPTY_NODE_ID]
	if !ok {
		EMPTY_NODE_ID_TRANSLATED = IntID(999999999)
	}

	// run through the metadata and find subtrees
	children := map[IntID]mapset.Set[IntID]{EMPTY_NODE_ID_TRANSLATED: mapset.NewSet[IntID]()}
	subDirNames := map[IntID]string{}
	for ckey, jsonData := range metadataMap {
		parent := IntID(ckey.MetaBlob__id)
		if _, ok := children[parent]; ! ok {
			children[parent] = mapset.NewSet[IntID]()
		}

		// extract child node with name
		jsonString := "[" + jsonData.Metadata + "]"
		meta_blob  := jsonData.Name__id
		name       := namesReverse[meta_blob]
		child := IntID(gjson.Get(jsonString, `@this.#(type == "dir").subtree`).Int())
		if child == 0 { continue }

		children[parent].Add(child)
		subDirNames[child] = name
		Printf("")
	}

	// make sure that the 'children' sets are complete
	// should not be necessary, but just in case ...
	for _, childrenSet := range children {
		for child := range childrenSet.Iter() {
			if _, ok := children[child]; ! ok {
				children[child] = mapset.NewSet[IntID]()
			}
		}
	}

	// INSERT INTO children
	// convert 'children' into childrenList
	childrenList := []ChildrenRow{}
	for parent__id, childrenSet := range children {
		for child__id := range childrenSet.Iter() {
			row := ChildrenRow{parent__id, child__id}
			childrenList = append(childrenList, row)
		}
	}

	err := GenericInsert[ChildrenRow](tx, "children", childrenList,
		`INSERT INTO children(parent__id, child__id)
       VALUES(:parent__id, :child__id)`, 2)
	if err != nil {
		return
	}
	childrenList = nil

	// manage snaps
	initials := mapset.NewSet[IntID]()
	snapMap  := map[restic.ID]int{}
	fullnames := map[IntID]string{}
	for ix, sn := range snaps {
		id := restic.ID{}
		copy(id[:], sn.Snap_id)
		snapMap[id] = ix + 1

		// generate initials for dfs
		copy(id[:], sn.Snap_root)
		treeRootInt := IntID(indexMap[id])
		initials.Add(treeRootInt)

		// names for tree roots
		fullnames[treeRootInt] = "/"
	}

	// create full directory names
	dfs_res := dfs(children, initials)
	var newChild string
	for _, meta_blob := range dfs_res {
		for child := range children[meta_blob].Iter() {
			if child == EMPTY_NODE_ID_TRANSLATED { continue }
			if fullnames[meta_blob] == "/" {
			  newChild =  "/" + subDirNames[child]
			} else {
			  newChild = fullnames[meta_blob] + "/" + subDirNames[child]
			}
			fullnames[child] = newChild
		}
	}

	// rename root entries
	for _, sn := range snaps {
		id := restic.ID{}
		copy(id[:], sn.Snap_root)
		treeRootInt := IntID(indexMap[id])
		// new names for tree roots
		fullnames[treeRootInt] = fmt.Sprintf("/ (root of %s)",
			hex.EncodeToString(sn.Snap_id[:4]))
	}

	// find out how many DISTINCT names we have
	fullnameSet := mapset.NewSet[string]()
	for _, fullname := range fullnames {
		fullnameSet.Add(fullname)
	}
	fullnameSlice := fullnameSet.ToSlice()
	sort.Strings(fullnameSlice)
	//Printf("%7d fullnameSlice entries\n", len(fullnameSlice))

	// INSERT INTO fullname
	fullnameTable := []FullnameRow{}
	fullnameMap   := map[string]int{}
	for ix, fullname := range fullnameSlice {
		row := FullnameRow{ix + 1, fullname}
		fullnameTable = append(fullnameTable, row)
		fullnameMap[fullname] = ix + 1
	}
	//Printf("%7d fullnameTable entries\n", len(fullnameTable))

	err = GenericInsert[FullnameRow](tx, "fullname", fullnameTable,
		`INSERT INTO fullname(id, pathname) VALUES(:id, :pathname)`, 2)
	if err != nil {
		return
	}

	// create table entries for 'dir_path_id'
	dirPathTable := []DirPathRow{}
	for blob_index, path := range fullnames {
		ix := fullnameMap[path]
		dirPathTable = append(dirPathTable, DirPathRow{blob_index, ix})
	}

	err = GenericInsert[DirPathRow](tx, "dir_path_id", dirPathTable,
		`INSERT INTO dir_path_id(id, fullname__id) VALUES(:id, :fullname__id)`, 2)
	if err != nil {
		return
	}

	//Verboseff("number of entries in fullnames %7d, DISTICT names %7d\n", len(fullnames),
	//	fullnameSet.Cardinality())

	//  create data for table 'meta_dir'
	metaDirSlice := []MetaDirEntry{}
	for ix, sn := range snaps {
		tree_id := restic.ID{}
		copy(tree_id[:], sn.Snap_root)
		flatSet := TopologyStructure(IntID(indexMap[tree_id]), children)
		snap_id_int := IntID(ix + 1)

		// create metaDirSlice entries
		for meta_blob := range flatSet.Iter() {
			row := MetaDirEntry{snap_id_int, meta_blob}
			metaDirSlice = append(metaDirSlice, row)
		}
	}

	// INSERT INTO meta_dir ... table
	err = GenericInsert[MetaDirEntry](tx, "meta_dir", metaDirSlice,
		`INSERT INTO meta_dir(snap__id, blob__id) VALUES(:snap__id, :blob__id)`, 2)
	if err != nil {
		return
	}
}

// generic INSERT INTO 'tableName'
func GenericInsert[TYPE any](tx *sqlx.Tx, tableName string, tableSlice []TYPE,
insertSql string, numColumns int16) (err error) {

	OFFSET := int(32700 / numColumns)
	max := len(tableSlice)
	full_len := max
	count := int64(0)
	for offset := 0; offset < full_len; offset += OFFSET {
		if max > OFFSET { max = OFFSET }
		if offset + max > full_len { max = full_len - offset }
		r, err := tx.NamedExec(insertSql, tableSlice[offset:offset + max])
		if err != nil {
			Printf("error INSERT %s - error is: %v\n", tableName, err)
			return err
		}
		c, _ := r.RowsAffected()
		count += c
	}
	Verboseff("INSERT %-18s %7d rows\n", tableName, count)
	return nil
}

/*
SELECT json_extract(metadata, '$.type') as mtype, fullname.pathname, name
	from meta_data_storage
	join names       on names.id=meta_data_storage.name__id
	join dir_path_id on dir_path_id.id = meta_data_storage.id
	join fullname    on fullname.id = dir_path_id.fullname__id
	where mtype='dir'
	ORDER BY fullname.pathname, name;
*/

/*
SELECT key FROM json_each('{"name":"PXL_20230609_130849650.jpg","type":"file",
 "mode":420,"mtime":"2023-06-09T13:08:49.650000095+01:00","atime":"2023-06-09T13:08:49.650000095+01:00",
 "ctime":"2023-08-17T14:46:25.908467409+01:00","uid":1001,"gid":132,"user":"frances",
 "group":"theplapperts","inode":15205162,"device_id":66309,"size":6247953,"links":1}');
*/

