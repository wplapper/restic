package main

import (
	// system
	"context"
	"encoding/hex"
	"fmt"
	"golang.org/x/sync/errgroup"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"

	//argparse
	"github.com/spf13/cobra"

	// restic library
	"github.com/wplapper/restic/library/repository"
	"github.com/wplapper/restic/library/restic"

	//"database/sql"
	"database/sql"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"

	// JSON encode / decode
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

type ExportOptions struct {
	Raw bool
}

var export_options ExportOptions

func init() {
	cmdRoot.AddCommand(cmdExportMeta)
	f := cmdExportMeta.Flags()
	f.BoolVarP(&export_options.Raw, "raw", "R", false, "export raw metadata")
}

type SnapshotRow struct {
	Id        int
	Snap_id   []byte
	Snap_time string
	Snap_host string
	Snap_fsys string
	Snap_root []byte
}

type IndexRepoRecordRow struct {
	Id                 IntID
	Blob               []byte // == UNIQUE
	Type               string
	Length             int
	UncompressedLength int
	Pack__id           int // back pointer to packfiles
}

type PackfileRecordRow struct {
	Id          int
	Packfile_id []byte
	Type        string
}

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
	Size        sql.NullInt64
	Links       sql.NullInt64
	Subtree__id sql.NullInt32
	LinkTarget  sql.NullString
}

type MetaDataStorageRaw struct {
	Id       IntID // ptr to 'index_repo' TABLE
	Position int   // offset in nodes list
	Json     string
}

type ContentsRecordRow struct {
	Data__id IntID // map back to index_repo.id (data)
	Blob__id IntID // map back to index_repo.id (owning directory / meta_blob)
	Position int   // with triple (Id_blob, Position, Offset) == UNIQUE
	Offset   int
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
	Device  uint64   `json:"device,omitempty"` // in case of Type == "dev", stat.st_rdev
	Content []string `json:"content"`
	Subtree string   `json:"subtree,omitempty"`
}

// type Nodes []*Node
type Tree struct {
	Nodes []*Node `json:"nodes"`
}

type FullnameRow struct {
	Id       int
	Pathname string
}

type DirPathRow struct {
	Id           IntID
	Fullname__id int
}

type MetaDirRow struct {
	Snap__id IntID
	Blob__id IntID
}

type ChildrenRow struct {
	Parent__id IntID
	Child__id  IntID
}

type NamesRow struct {
	Id   int
	Name string
}

type CompIndex struct {
	MetaBlob__id IntID
	Position     int
}

// convert all the metadata into a SQLite database
func runExportMeta(ctx context.Context, cmd *cobra.Command, gopts GlobalOptions, args []string) error {
	var (
		err           error
		database_name string
	)

	startTime := time.Now()
	// open repository
	repo, err := OpenRepository(ctx, gopts)
	if err != nil {
		return err
	}
	defer repo.Close()
	Verboseff("Repository is %s\n", globalOptions.Repo)
	Verboseff("%-35s %7.1f seconds\n", "repository open", time.Since(startTime).Seconds())

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
	tx := db_conn.MustBegin()
	Verboseff("%-35s %7.1f seconds\n", "transaction open",
		time.Since(startTime).Seconds())

	CreateTables(tx)

	// get all snapshots
	snapSlice, err := ProcessSnapshots(ctx, repo, tx)
	if err != nil {
		return err
	}
	Verboseff("%-35s %7.1f seconds\n", "ProcessSnapshots",
		time.Since(startTime).Seconds())

	// read all index files
	indexMap, err := ProcessIndexFiles(ctx, repo, tx, startTime)
	if err != nil {
		return err
	}

	// read the blob metadata (large)
	metadataMap, namesReverse, err := ProcessMetaData(ctx, repo, tx, indexMap,
		startTime, export_options.Raw)
	if err != nil {
		return err
	}

	// create derived tables
	ProcessMetaDirTable(tx, snapSlice, indexMap, metadataMap, namesReverse)
	Verboseff("%-35s %7.1f seconds\n", "ProcessMetaDirTable",
		time.Since(startTime).Seconds())

	CreateIndices(tx)

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
	Verboseff("%-35s %7.1f seconds\n", "*** finale ***",
		time.Since(startTime).Seconds())
	return nil
}

// gather data about snapshots
func ProcessSnapshots(ctx context.Context, repo *repository.Repository, tx *sqlx.Tx) (
	// return:
	snapSlice []SnapshotRow, err error) {

	snapSlice = []SnapshotRow{}
	high_id := 1
	repo.List(ctx, restic.SnapshotFile, func(id restic.ID, size int64) error {
		sn, err := restic.LoadSnapshot(ctx, repo, id)
		if err != nil {
			Printf("restic.SnapshotFile.skip loading snap record %s! - reason: %v\n", id, err)
			return err
		}

		snapSlice = append(snapSlice, SnapshotRow{Snap_id: id[:], Snap_host: sn.Hostname,
			Snap_time: sn.Time.String()[:19], Snap_fsys: sn.Paths[0], Snap_root: sn.Tree[:],
		})
		high_id++
		return nil
	})

	// sort by time - times are strins here
	sort.Slice(snapSlice, func(i, j int) bool {
		return snapSlice[i].Snap_time < snapSlice[j].Snap_time
	})

	err = GenericInsert[SnapshotRow](tx, "snapshots", snapSlice,
		`INSERT INTO snapshots(snap_id, snap_time, snap_host, snap_fsys, snap_root)
    VALUES(:snap_id, :snap_time, :snap_host, :snap_fsys, :snap_root)`, 5)
	if err != nil {
		return nil, err
	}
	return snapSlice, nil
}

// gather information about packfiles and the index structure
func ProcessIndexFiles(ctx context.Context, repo *repository.Repository, tx *sqlx.Tx, start time.Time) (
	// return:
	indexMap map[restic.ID]IntID, err error) {

	if err = repo.LoadIndex(ctx); err != nil {
		Printf("repo.LoadIndex - failed. Error is %v\n", err)
		return nil, err
	}

	// gather all packfile IDs
	high_id := 1
	insertPackSlice := []PackfileRecordRow{}
	packMap := map[restic.ID]PackfileRecordRow{}
	repo.Index().Each(ctx, func(blob restic.PackedBlob) {
		if _, ok := packMap[blob.PackID]; !ok {
			row := PackfileRecordRow{
				Id: high_id, Packfile_id: blob.PackID[:], Type: blob.Type.String(),
			}
			insertPackSlice = append(insertPackSlice, row)
			packMap[blob.PackID] = row
			high_id++
		}
	})

	err = GenericInsert[PackfileRecordRow](tx, "packfiles", insertPackSlice,
		"INSERT INTO packfiles(id, packfile_id, type) VALUES(:id, :packfile_id, :type)", 2)
	if err != nil {
		return nil, err
	}
	insertPackSlice = nil
	Verboseff("%-35s %7.1f seconds\n", "INSERT INTO packfiles",
		time.Since(start).Seconds())

	insertIndexSlice := []IndexRepoRecordRow{}
	indexMap = map[restic.ID]IntID{}
	high_id2 := IntID(1)
	repo.Index().Each(ctx, func(blob restic.PackedBlob) {
		row := IndexRepoRecordRow{Id: high_id2, Blob: blob.ID[:], Type: blob.Type.String(),
			Length: int(blob.Length), UncompressedLength: int(blob.UncompressedLength),
			Pack__id: packMap[blob.PackID].Id,
		}
		insertIndexSlice = append(insertIndexSlice, row)
		indexMap[blob.ID] = high_id2
		high_id2++
	})

	err = GenericInsert[IndexRepoRecordRow](tx, "index_repo", insertIndexSlice,
		`INSERT INTO index_repo(id, blob, type, length, uncompressedlength, pack__id)
       VALUES(:id, :blob, :type, :length, :uncompressedlength, :pack__id)`, 6)
	if err != nil {
		return nil, err
	}
	Verboseff("%-35s %7.1f seconds\n", "INSERT INTO index_repo",
		time.Since(start).Seconds())
	return indexMap, nil
}

// gather all metablobs from Index, needed for the parallel processing
func GetMetaBlobSlice(ctx context.Context, repo *repository.Repository) (metaBlobSlice []*restic.ID) {
	metaBlobSlice = []*restic.ID{}
	repo.Index().Each(ctx, func(blob restic.PackedBlob) {
		if blob.Type == restic.TreeBlob {
			metaBlobSlice = append(metaBlobSlice, &blob.ID)
		}
	})
	return metaBlobSlice
}

// auxiliary function to deliver meta blobs from the index to ProcessMetaData
// for parallel processing
func SpoutMetaBlobs(metaBlobSlice []*restic.ID, fn func(id restic.ID) error) error {
	for _, blob_ID := range metaBlobSlice {
		fn(*blob_ID)
	}
	return nil
}

// read metadata json strings for all meta blobs
func ProcessMetaData(ctx context.Context, repo *repository.Repository, tx *sqlx.Tx,
	indexMap map[restic.ID]IntID, start time.Time, rawExport bool) (
	// return
	metaDataMap map[CompIndex]MetaDataStorageRow, namesReverse map[int]string, err error) {

	metaDataMap = map[CompIndex]MetaDataStorageRow{}
	metaBlobSlice := GetMetaBlobSlice(ctx, repo)

	// protect 'metaDataNodes', 'contentsNodes', 'namesMap', 'nameCounter' from parallel processing
	var m sync.Mutex
	var m_cont sync.Mutex
	var mRaw sync.Mutex

	metaDataNodes := []MetaDataStorageRow{}
	metaRaw := []MetaDataStorageRaw{}
	contentsNodes := []ContentsRecordRow{}
	namesMap := map[string]int{}
	nameCounter := 1
	// end protected

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
	}) // end of unnamed Go func

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

			if rawExport {
				for position, node := range gjson.GetBytes(json_buf, "nodes").Array() {
					mod, _ := sjson.Delete(node.String(), "content")
					mRaw.Lock()
					metaRaw = append(metaRaw, MetaDataStorageRaw{ix, position, mod})
					mRaw.Unlock()
				}
			}

			// split the nodes list into many node entries
			tree := &Tree{}
			err = json.Unmarshal(json_buf, tree)
			if err != nil {
				Printf("Unable to Unmarshal nodes -reason %v\n", err)
				return err
			}

			for position, node := range tree.Nodes {
				// extract key fields
				name := node.Name
				subtree := node.Subtree // dir only
				content := node.Content // files only
				cmpix := CompIndex{ix, position}

				var (
					name__id   int
					size       sql.NullInt64
					links      sql.NullInt64
					linkTarget sql.NullString
					sub_ID     sql.NullInt32
				)

				if len(content) > 0 {
					//critical contents region
					m_cont.Lock()
					for offset, cont := range content {
						// convert cont to restic.ID
						data__id, _ := restic.ParseID(cont)
						contentsNodes = append(contentsNodes, ContentsRecordRow{
							Data__id: indexMap[data__id], Blob__id: ix, Position: position, Offset: offset})
					}
					m_cont.Unlock()
				}

				if subtree != "" {
					meta__id, _ := restic.ParseID(subtree)
					sub_ID = sql.NullInt32{int32(indexMap[meta__id]), true}
				} else {
					sub_ID = sql.NullInt32{}
				}

				if node.Size == 0 && node.Type != "file" {
					size = sql.NullInt64{}
				} else {
					size = sql.NullInt64{int64(node.Size), true}
				}

				if node.Links == 0 {
					links = sql.NullInt64{}
				} else {
					links = sql.NullInt64{int64(node.Links), true}
				}

				if node.LinkTarget == "" {
					linkTarget = sql.NullString{}
				} else {
					linkTarget = sql.NullString{node.LinkTarget, true}
				}

				//critical metadata region
				m.Lock()
				name__id, ok := namesMap[name]
				if !ok {
					namesMap[name] = nameCounter
					name__id = nameCounter
					nameCounter++
				}

				metaRow := MetaDataStorageRow{
					Id: ix, Position: position, Name__id: name__id, Type: node.Type,
					Mode: node.Mode, Mtime: node.ModTime.String()[:19], Inode: node.Inode,
					DeviceID: node.DeviceID, Size: size, Links: links,
					Subtree__id: sub_ID, LinkTarget: linkTarget,
				}
				metaDataNodes = append(metaDataNodes, metaRow)
				metaDataMap[cmpix] = metaRow
				m.Unlock()
			}
		}
		return nil
	} // end of func 'worker'

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
	Verboseff("%-35s %7.1f seconds\n", "finish parallel", time.Since(start).Seconds())

	// names collector
	namesSorted := []NamesRow{}
	namesReverse = map[int]string{}
	for name, index := range namesMap {
		namesSorted = append(namesSorted, NamesRow{index, name})
		namesReverse[index] = name
	}
	// custom sort by 'name'
	sort.Slice(namesSorted, func(i, j int) bool {
		return namesSorted[i].Name < namesSorted[j].Name
	})
	namesMap = nil

	//  INSERT INTO names
	err = GenericInsert[NamesRow](tx, "names", namesSorted,
		`INSERT INTO names(id, name) VALUES(:id, :name)`, 2)
	if err != nil {
		return nil, nil, err
	}
	namesSorted = nil
	Verboseff("%-35s %7.1f seconds\n", "INSERT INTO names",
		time.Since(start).Seconds())

	err = GenericInsert[MetaDataStorageRow](tx, "meta_data_store", metaDataNodes,
		`INSERT INTO meta_data_store(id, position, name__id, type, mode, mtime, inode,
      deviceid, size, links, subtree__id, linktarget)
    VALUES(:id, :position, :name__id, :type, :mode, :mtime, :inode,
      :deviceid, :size, :links, :subtree__id, :linktarget)`, 12)
	if err != nil {
		return nil, nil, err
	}
	Verboseff("%-35s %7.1f seconds\n", "INSERT INTO meta_data_store",
		time.Since(start).Seconds())

	if len(metaRaw) > 0 {
		err = GenericInsert[MetaDataStorageRaw](tx, "meta_raw", metaRaw,
			`INSERT INTO meta_raw(id, position, json) VALUES(:id, :position, :json)`, 3)
		if err != nil {
			return nil, nil, err
		}
		Verboseff("%-35s %7.1f seconds\n", "INSERT INTO meta_data_store",
			time.Since(start).Seconds())
	}

	// contents
	err = GenericInsert[ContentsRecordRow](tx, "contents", contentsNodes,
		`INSERT INTO contents(data__id, blob__id, position, offset)
    VALUES(:data__id, :blob__id, :position, :offset)`, 4)
	if err != nil {
		return nil, nil, err
	}
	Verboseff("%-35s %7.1f seconds\n", "INSERT INTO contents",
		time.Since(start).Seconds())
	return metaDataMap, namesReverse, nil
}

// extract data for tables 'children', 'snap_blobs', 'fullnames'
func ProcessMetaDirTable(tx *sqlx.Tx, snapSlice []SnapshotRow, indexMap map[restic.ID]IntID,
	metadataMap map[CompIndex]MetaDataStorageRow, namesReverse map[int]string) {

	// find out about EMPTY_NODDE_ID
	var EMPTY_NODE_ID_TRANSLATED IntID
	EMPTY_NODE_ID := restic.Hash([]byte(`{"nodes":[]}` + "\n"))

	//Verboseff("EMPTY_NODE_ID = %s\n", EMPTY_NODE_ID.String())
	EMPTY_NODE_ID_TRANSLATED, ok := indexMap[EMPTY_NODE_ID]
	if !ok {
		//Printf("EMPTY_NODE_ID_TRANSLATED not found!\n")
		EMPTY_NODE_ID_TRANSLATED = IntID(0)
	}
	//Verboseff("EMPTY_NODE_ID_TRANSLATED = %7d\n", EMPTY_NODE_ID_TRANSLATED)

	// run through the metadata and find subtrees
	var err error
	subDirNames := map[IntID]string{}
	children := map[IntID]mapset.Set[IntID]{
		EMPTY_NODE_ID_TRANSLATED: mapset.NewSet[IntID]()}
	for ckey, node := range metadataMap {
		parent := IntID(ckey.MetaBlob__id)
		if _, ok := children[parent]; !ok {
			children[parent] = mapset.NewSet[IntID]()
		}

		// extract child node with name
		meta_blob := node.Name__id
		name := namesReverse[meta_blob]
		childV := node.Subtree__id
		if !childV.Valid {
			continue
		}

		child := IntID(childV.Int32)
		children[parent].Add(child)
		subDirNames[child] = name
	}

	// make sure that the 'children' sets are complete
	// should not be necessary, but just in case ...
	for _, childrenSet := range children {
		for child := range childrenSet.Iter() {
			if _, ok := children[child]; !ok {
				children[child] = mapset.NewSet[IntID]()
			}
		}
	}

	// INSERT INTO children
	// convert 'children' map into a flat childrenSlice
	childrenSlice := []ChildrenRow{}
	for parent__id, childrenSet := range children {
		for child__id := range childrenSet.Iter() {
			row := ChildrenRow{parent__id, child__id}
			childrenSlice = append(childrenSlice, row)
		}
	}

	err = GenericInsert[ChildrenRow](tx, "children", childrenSlice,
		`INSERT INTO children(parent__id, child__id)
       VALUES(:parent__id, :child__id)`, 2)
	if err != nil {
		return
	}
	childrenSlice = nil

	// manage snapSlice, create 'initials' for topological sort
	initials := mapset.NewSet[IntID]()
	snapMap := map[restic.ID]int{}
	fullnames := map[IntID]string{}
	for ix, sn := range snapSlice {
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

	// create full directory names, unsing topo sort
	dfs_res := dfs(children, initials)
	var newChild string
	for _, meta_blob := range dfs_res {
		if meta_blob == EMPTY_NODE_ID_TRANSLATED {
			continue
		}
		for child := range children[meta_blob].Iter() {
			if child == EMPTY_NODE_ID_TRANSLATED {
				continue
			}
			if fullnames[meta_blob] == "/" {
				newChild = "/" + subDirNames[child]
			} else {
				newChild = fullnames[meta_blob] + "/" + subDirNames[child]
			}
			fullnames[child] = newChild
		}
	}

	// rename root entries
	for _, sn := range snapSlice {
		id := restic.ID{}
		copy(id[:], sn.Snap_root)
		fullnames[IntID(indexMap[id])] = fmt.Sprintf("/ (root of %s)",
			hex.EncodeToString(sn.Snap_id[:4]))
	}

	// find out how many DISTINCT names we have
	fullnameSet := mapset.NewSet[string]()
	for _, fullname := range fullnames {
		fullnameSet.Add(fullname)
	}
	fullnameSlice := fullnameSet.ToSlice()
	sort.Strings(fullnameSlice)

	// INSERT INTO fullname
	fullnameTable := []FullnameRow{}
	fullnameMap := map[string]int{}
	for ix, fullname := range fullnameSlice {
		row := FullnameRow{ix + 1, fullname}
		fullnameTable = append(fullnameTable, row)
		fullnameMap[fullname] = ix + 1
	}
	fullnameSlice = nil

	err = GenericInsert[FullnameRow](tx, "fullname", fullnameTable,
		`INSERT INTO fullname(id, pathname) VALUES(:id, :pathname)`, 2)
	if err != nil {
		return
	}
	fullnameTable = nil

	// create table entries for 'dir_path'
	dirPathSlice := []DirPathRow{}
	for blob_index, path := range fullnames {
		dirPathSlice = append(dirPathSlice, DirPathRow{blob_index, fullnameMap[path]})
	}

	err = GenericInsert[DirPathRow](tx, "dir_path", dirPathSlice,
		`INSERT INTO dir_path(id, fullname__id) VALUES(:id, :fullname__id)`, 2)
	if err != nil {
		return
	}
	dirPathSlice = nil

	//  create data for table 'snap_blobs'
	metaDirSlice := []MetaDirRow{}
	for ix, sn := range snapSlice {
		tree_id := restic.ID{}
		copy(tree_id[:], sn.Snap_root)
		flatSet := TopologyStructure(IntID(indexMap[tree_id]), children)
		snap_id_int := IntID(ix + 1)

		// create metaDirSlice entries
		for meta_blob := range flatSet.Iter() {
			row := MetaDirRow{snap_id_int, meta_blob}
			metaDirSlice = append(metaDirSlice, row)
		}
	}

	err = GenericInsert[MetaDirRow](tx, "snap_blobs", metaDirSlice,
		`INSERT INTO snap_blobs(snap__id, blob__id) VALUES(:snap__id, :blob__id)`, 2)
	if err != nil {
		return
	}
}

// generic INSERT INTO 'tableName'
func GenericInsert[TYPE any](tx *sqlx.Tx, tableName string, tableSlice []TYPE,
	insertSql string, numColumns int) (err error) {

	// since SQLite does not allow more than 32k variables (too many SQL variables),
	// we have to split the lot into junks which fulfill the condition
	OFFSET := int(32700 / numColumns)
	max := len(tableSlice)
	full_len := max
	count := int64(0)
	for offset := 0; offset < full_len; offset += OFFSET {
		if max > OFFSET {
			max = OFFSET
		}
		if offset+max > full_len {
			max = full_len - offset
		}
		r, err := tx.NamedExec(insertSql, tableSlice[offset:offset+max])
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
	-- needed for the relationship between packfiles and blobs
	id INTEGER PRIMARY KEY,             -- the primary key
	packfile_id BLOB NOT NULL,          -- the packfile ID, UNIQUE INDEX, as []byte, len 32
	type TEXT                           -- type of packfile data/tree
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

	`CREATE TABLE meta_data_store (
	id INTEGER NOT NULL,                -- back ptr to index_repo PRIMARY KEY, part 1
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

	PRIMARY KEY (id, position),         -- the composite PRIMARY KEY
	FOREIGN KEY(id)                     REFERENCES index_repo(id)
	FOREIGN KEY(name__id)               REFERENCES name(id)
) WITHOUT ROWID`,

	`CREATE TABLE contents (
	-- id INTEGER PRIMARY KEY,          -- the primary key
	data__id INTEGER NOT NULL,          -- ptr to data_idd, INDEX
	blob__id INTEGER NOT NULL,          -- ptr to idd_file, needs INDEX
	position INTEGER NOT NULL,          -- position in idd_file
	offset   INTEGER NOT NULL,          -- the offset of the contents list

	PRIMARY KEY(blob__id, position, offset),
	FOREIGN KEY(data__id)               REFERENCES index_repo(id),
	FOREIGN KEY(blob__id)               REFERENCES index_repo(id)
) WITHOUT ROWID`,

	// derived TABLEs
	`CREATE TABLE names (
	id INTEGER PRIMARY KEY,             -- the primary key
	name TEXT                           -- all names collected from restic system
)`,

	`CREATE TABLE snap_blobs (
	-- many to many relationship table between snaps and directory blobs
	snap__id INTEGER NOT NULL,          -- the snap_id , INDEX
	blob__id INTEGER NOT NULL,          -- the idd pointer, INDEX

	PRIMARY KEY(snap__id, blob__id),
	FOREIGN KEY(snap__id)               REFERENCES snaphots(id),
	FOREIGN KEY(blob__id)               REFERENCES index_repo(id)
) WITHOUT ROWID`,

	`CREATE TABLE children (
		parent__id INTEGER NOT NULL,      -- parent entry
		child__id  INTEGER NOT NULL,      -- child  entry

		FOREIGN KEY(parent__id)           REFERENCES index_repo(id),
		FOREIGN KEY(child__id)            REFERENCES index_repo(id),
		PRIMARY KEY(parent__id, child__id)
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

	`CREATE TABLE meta_raw (
	id INTEGER NOT NULL,                -- back ptr to index_repo PRIMARY KEY, part 1
	position INTEGER NOT NULL,          -- offset into slice      PRIMARY KEY, part 2
	json TEXT,

	PRIMARY KEY (id, position)          -- the composite PRIMARY KEY
 ) WITHOUT ROWID`,
}

// execute CREATE TABLEs ...
func CreateTables(tx *sqlx.Tx) {
	for _, create_sql := range CreateTablesSlice {
		tx.MustExec(create_sql)
	}
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
	`CREATE        INDEX ix_cont_data   ON contents(data__id)`,
	`CREATE        INDEX ix_md_blob     ON snap_blobs(blob__id)`,
	`CREATE        INDEX ix_child_child ON children(child__id)`,
	`CREATE        INDEX ix_mds_type    ON meta_data_store(type)`,
	`CREATE        INDEX ix_mds_inode   ON meta_data_store(inode)`,
	`CREATE        INDEX ix_mds_st_id   ON meta_data_store(subtree__id)`,
}

// execute CREATE some INDEX records for this database
func CreateIndices(tx *sqlx.Tx) {
	for _, create_index := range CreateIndexSlice {
		tx.MustExec(create_index)
	}
}
