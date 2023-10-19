package main

import (
	// system
	"os"
	"errors"
	"strconv"
	"encoding/csv"
	"encoding/json"
	"encoding/hex"
	"io"
	"path/filepath"
	"fmt"
	//"reflect"
	"time"

	// restic library
	"github.com/wplapper/restic/library/restic"

	// sqlite
	"github.com/jmoiron/sqlx"
	"github.com/wplapper/restic/library/sqlite"

	//sets and queues
	"github.com/deckarep/golang-set/v2"
)

func ProcessSnaphotsTable(tx *sqlx.Tx, filename string, table_column_names map[string][]string,
changes_made *bool) (SnapshotsTable map[string]SnapshotRecordMem, err error) {
	SnapshotsTable = map[string]SnapshotRecordMem{}
	// step 1: read from TABLE snapshots
	sql := "SELECT * FROM snapshots"
	rows, err := tx.Queryx(sql)
	if err != nil {
		Printf("Select snap_id. Error in Queryx %v\n", err)
		return nil, err
	}

	for rows.Next() {
		var row SnapshotRecordMem
		err = rows.StructScan(&row)
		if err != nil {
			Printf("select snap_id.StructScan failed %v\n", err)
			return nil, err
		}
		row.Status = DBDELETE
		SnapshotsTable[row.Snap_id] = row
	}

	// step 2: read /all_snapshots
	fd, err := os.Open(filename)
	if err != nil {
		Printf("os.Open %s failed = reason %v\n", filename, err)
		return nil, err
	}
	defer fd.Close()
	csvReader := csv.NewReader(fd)

	highID := sqlite.Get_high_id("snapshots")
	for {
		components, err := csvReader.Read()
		if err == io.EOF { break }
		snap_id  := components[0]
		tree     := components[1]
		hostname := components[2]
		path     := components[3]
		snTime   := components[4]

		snapIDShort := snap_id[:8]
		row, ok := SnapshotsTable[snapIDShort]
		if ok && (row.Status == DBDELETE || row.Status == DBOK) {
			row.Status = DBOK
			SnapshotsTable[snapIDShort] = row
			continue
		}

		// build INSERT slice
		SnapshotsTable[snapIDShort] = SnapshotRecordMem{
			Id: highID,
			Snap_id: snapIDShort,
			Snap_time: snTime,
			Snap_host: hostname,
			Snap_fsys: path,
			Snap_root: tree,
			Status:    DBNEW,
		}
		highID++
	}

	if len(SnapshotsTable) > 0 {
		err = InsertTable("snapshots", SnapshotsTable, tx, table_column_names, changes_made)
		if err != nil {
			Printf("InsertTable(snapshots) %v\n", err)
			return nil, err
		}
	}
	return SnapshotsTable, nil
}

func ProcessPackfilesTable(tx *sqlx.Tx, filename string, table_column_names map[string][]string,
changes_made *bool) (PackfilesTable map[string]PackfilesRecordMem, err error) {
	PackfilesTable = map[string]PackfilesRecordMem{}

	// step 1: read from TABLE packfiles
	sql := "SELECT * FROM packfiles"
	rows, err := tx.Queryx(sql)
	if err != nil {
		Printf("Select snapshots. Error in Queryx %v\n", err)
		return nil, err
	}

	for rows.Next() {
		var row PackfilesRecordMem
		err = rows.StructScan(&row)
		if err != nil {
			Printf("select packfiles.StructScan failed %v\n", err)
			return nil, err
		}
		row.Status = DBDELETE
		PackfilesTable[hex.EncodeToString(row.Packfile_id)] = row
	}

	// step 2: read /all_packfiles
	fd, err := os.Open(filename)
	if err != nil {
		Printf("os.Open %s failed = reason %v\n", filename, err)
		return nil, err
	}
	defer fd.Close()
	csvReader := csv.NewReader(fd)

	highID := sqlite.Get_high_id("packfiles")
	for {
		line, err := csvReader.Read()
		if err == io.EOF { break }

		packfile := line[0]
		row, ok := PackfilesTable[packfile]
		if ok && (row.Status == DBDELETE || row.Status == DBOK) {
			row.Status = DBOK
			PackfilesTable[packfile] = row
			continue
		}

		packfile_bytes, _ := hex.DecodeString(packfile)
		PackfilesTable[packfile] = PackfilesRecordMem{
			Id: highID,
			Packfile_id: packfile_bytes,
			Status: DBNEW,
		}
		highID++
	}

	if len(PackfilesTable) > 0 {
		err = InsertTable("packfiles", PackfilesTable, tx, table_column_names, changes_made)
		if err != nil {
			Printf("InsertTable(packfiles) %v\n", err)
			return nil, err
		}
	}
	return PackfilesTable, nil
}

type IndexUpdate struct {
	Id       int
	Pack__id int
}


func ProcessIndexRecordsTable(tx *sqlx.Tx, filename string, table_column_names map[string][]string,
changes_made *bool, packfiles map[string]PackfilesRecordMem) (
IndexRepoTable map[string]IndexRepoRecordMem, reverseIndexRepo map[int]restic.ID, err error) {

	// initialize return maps
	IndexRepoTable      = map[string]IndexRepoRecordMem{}
	reverseIndexRepo    = map[int]restic.ID{}
	IndexRepoUpdateMap := map[string]IndexUpdate{}

	// step 1: read from TABLE index_repo
	sql := "SELECT * FROM index_repo"
	rows, err := tx.Queryx(sql)
	if err != nil {
		Printf("Select index_repo. Error in Queryx %v\n", err)
		return nil, nil, err
	}

	for rows.Next() {
		var row IndexRepoRecordMem
		err = rows.StructScan(&row)
		if err != nil {
			Printf("select index_repo.StructScan failed %v\n", err)
			return nil, nil, err
		}
		row.Status = DBDELETE
		IndexRepoTable[hex.EncodeToString(row.Blob)] = row

		if len(row.Blob) != 32 {
			Printf("Can't parse blob %s from index_repo.blob %v\n", row.Blob[:12], err)
			panic("Can't parse blob from index_repo.blob")
		}

		blobAsID := restic.ID{}
		copy(blobAsID[:], row.Blob)
		reverseIndexRepo[row.Id] = blobAsID
	}

	// step 2: read /all_index_info
	fd, err := os.Open(filename)
	if err != nil {
		Printf("os.Open %s failed = reason %v\n", filename, err)
		return nil, nil, err
	}
	defer fd.Close()
	csvReader := csv.NewReader(fd)

	// insert new rows
	highID := sqlite.Get_high_id("index_repo")
	for {
		components, err := csvReader.Read()
		if err == io.EOF { break }
		blob := components[0]
		packfile := components[5]
		row, ok := IndexRepoTable[blob]
		if ok && row.Status == DBDELETE {
			newPack__id := packfiles[packfile].Id
			oldPack__id := row.Pack__id
			if newPack__id == oldPack__id {
				// if packfile is identical, every is ok
				row.Status = DBOK
			} else {
				row.Status = DBUPDATE
				IndexRepoUpdateMap[blob] = IndexUpdate{Id: row.Id, Pack__id: packfiles[packfile].Id}
				row.Pack__id = packfiles[packfile].Id
			}
			IndexRepoTable[blob] = row
			continue
		}

		Type := components[1]
		offset,   err1 := strconv.Atoi(components[2])
		length,   err2 := strconv.Atoi(components[3])
		UCLength, err3 := strconv.Atoi(components[4])

		if err1 != nil || err2 != nil || err3 != nil {
			Printf("Conversion error of str->int\n")
			return nil, nil, errors.New("Int Conversion error: offset/length/UClength")
		}

		if _, ok := packfiles[packfile]; ! ok {
			Printf("Back ptr to packfile %s does not exist. Abort!\n", packfile[:12])
			panic("Back ptr to packfile does not exist. Aborting!")
		}

		blob_bytes, _ := hex.DecodeString(blob)
		IndexRepoTable[blob] = IndexRepoRecordMem{
			Id: highID,
			Blob: blob_bytes,
			Type: Type,
			Offset: offset,
			Length: length,
			UncompressedLength: UCLength,
			Pack__id: packfiles[packfile].Id,
			Status: DBNEW,
		}

		blobAsID := restic.ID{}
		if len(blob_bytes) != 32 {
			panic("Internal inconsistency for blob_bytes: length not 32. Aborting!")
		}

		copy(blobAsID[:], blob_bytes)
		reverseIndexRepo[highID] = blobAsID
		highID++
	}

	// deal with EMPTY_NODE_ID
	EMPTY_NODE_ID = restic.Hash([]byte("{\"nodes\":[]}\n"))
	var empty_bytes []byte
	empty_bytes = EMPTY_NODE_ID[:]
	empty, ok := IndexRepoTable[EMPTY_NODE_ID.String()]
	if ! ok {
		row := IndexRepoRecordMem{Id: 999999999, Blob: empty_bytes, Type: "tree",
			Offset: 0, Length: 54, UncompressedLength: 13, Pack__id: 1, Status: DBNEW}
		IndexRepoTable[EMPTY_NODE_ID.String()] = row
		EMPTY_NODE_ID_TRANSLATED = IntID(999999999)
	} else {
		EMPTY_NODE_ID_TRANSLATED = IntID(empty.Id)
	}

	if len(IndexRepoTable) > 0 {
		err = InsertTable("index_repo", IndexRepoTable, tx, table_column_names, changes_made)
		if err != nil {
			Printf("InsertTable(index_repo) %v\n", err)
			return nil, nil, err
		}
	}

	if len(IndexRepoUpdateMap) > 0 {
		err := runIndexRepoUpdate(tx, IndexRepoUpdateMap)
		if err != nil { return nil, nil, err }
		*changes_made = true
	}
	return IndexRepoTable, reverseIndexRepo, nil
}

// big function which processes all Tree elements of the metadata
func ProcessMetaDataDetails(tx *sqlx.Tx, dirname string, table_column_names map[string][]string,
changes_made *bool, IndexRepoTable map[string]IndexRepoRecordMem) (metaDataAll map[IntID]*restic.Tree, err error) {

	type MyTree struct {
		MetaBlob restic.ID `json:"metablob"`
		restic.Tree
	}

	info, err := os.Stat(dirname)
	if err != nil {
		Printf("Can't stat directory %s - reason %v\n", dirname, err)
		return nil, err
	}
	if ! info.IsDir() {
		Printf("%s is not a directory. Aborting!\n", dirname)
		return nil, errors.New("Not a directory. Aborting!")
	}

	metaDataAll = map[IntID]*restic.Tree{}
	files, err := filepath.Glob(dirname + "/[0-9a-f][0-9a-f]/[0-9a-f][0-9a-f]*")
	for _, filename := range files {
		data, err := os.ReadFile(filename)
		if err != nil {
			Printf("Can't os.Read %s - error %v. Aborting\n", filename, err)
			return nil, err
		}

		t := &MyTree{}
		err = json.Unmarshal(data, t)
		if err != nil {
			Printf("Can't Unmarshal data from file %s - error '%+v'. Aborting\n", filename, err)
			return nil, err
		}
		record, ok := IndexRepoTable[t.MetaBlob.String()]
		if ! ok {
			Printf("entry %s not found in IndexRepo. Aborting!\n", t.MetaBlob.String())
			panic("Internal inconsistency. Aborting!")
		}
		metaDataAll[IntID(record.Id)] = &(t.Tree)
	}
	return metaDataAll, nil
}

func ProcessMetaDirTable(tx *sqlx.Tx, table_column_names map[string][]string,
changes_made *bool, metaDirMap map[string]mapset.Set[IntID],
SnapshotsTable map[string]SnapshotRecordMem) (
metaDirTable map[CompMetaDir]MetaDirRecordMem, err error) {
	metaDirTable = map[CompMetaDir]MetaDirRecordMem{}

	// step 1: read mata_dir table
	sql := "SELECT * FROM meta_dir"
	rows, err := tx.Queryx(sql)
	if err != nil {
		Printf("Select meta_dir. Error in Queryx %v\n", err)
		return nil, err
	}

	for rows.Next() {
		var row MetaDirRecordMem
		err = rows.StructScan(&row)
		if err != nil {
			Printf("select meta_dir.StructScan failed %v\n", err)
			return nil, err
		}
		row.Status = DBDELETE
		cmpix := CompMetaDir{row.Snap__id, IntID(row.Blob__id)}
		metaDirTable[cmpix] = row
	}

	// step 2: compare with data from maetadata export as create new rows
	//highID := sqlite.Get_high_id("meta_dir")
	for snap_id, blobSet := range metaDirMap {
		snapRow, ok := SnapshotsTable[snap_id]
		if ! ok {
			Printf("internal inconsistency for snap_id %s. Aborting!\n", snap_id[:12])
			panic("internal inconsistency for snap_id. Aborting!")
		}
		for blob := range blobSet.Iter() {
			cmpix := CompMetaDir{snapRow.Id, blob}
			row, ok := metaDirTable[cmpix]
			if ok && (row.Status == DBDELETE || row.Status == DBOK) {
				row.Status = DBOK
				metaDirTable[cmpix] = row
				continue
			}

			row = MetaDirRecordMem{Status: DBNEW, Snap__id: snapRow.Id, Blob__id: int(blob)}
			metaDirTable[cmpix] = row
		}
	}

	if len(metaDirTable) > 0 {
		err = InsertTable("meta_dir", metaDirTable, tx, table_column_names, changes_made)
		if err != nil {
			Printf("InsertTable(meta_dir) %v\n", err)
			return nil, err
		}
	}
	return metaDirTable, nil
}

func ProcessNamesTable(tx *sqlx.Tx, table_column_names map[string][]string,
changes_made *bool, metaDataAll map[IntID]*restic.Tree) (namesTable map[string]NamesRecordMem, err error) {
	namesTable = map[string]NamesRecordMem{}

	// step 1: read mata_dir table
	sql := "SELECT * FROM names"
	rows, err := tx.Queryx(sql)
	if err != nil {
		Printf("Select names. Error in Queryx %v\n", err)
		return nil, err
	}

	for rows.Next() {
		var row NamesRecordMem
		err = rows.StructScan(&row)
		if err != nil {
			Printf("select names.StructScan failed %v\n", err)
			return nil, err
		}

		row.Status = DBDELETE
		namesTable[row.Name] = row
	}

	// step 2: look for new rows
	highID := sqlite.Get_high_id("names")
	for _, tree := range metaDataAll {
		for _, node := range tree.Nodes {
			name := node.Name
			row, ok := namesTable[name]
			if ok && (row.Status == DBDELETE || row.Status == DBOK) {
				row.Status = DBOK
				namesTable[name] = row
				continue
			}

			namesTable[name] = NamesRecordMem{Status: DBNEW, Id: highID, Name: name}
			highID++
		}
	}

	if len(namesTable) > 0 {
		err = InsertTable("names", namesTable, tx, table_column_names, changes_made)
		if err != nil {
			Printf("InsertTable(names) %v\n", err)
			return nil, err
		}
	}
	return namesTable, nil
}

func ProcessContentsTable(tx *sqlx.Tx, table_column_names map[string][]string,
changes_made *bool, IndexRepoTable map[string]IndexRepoRecordMem,
reverseIndexRepo map[int]restic.ID,
metaDataAll map[IntID]*restic.Tree) (contentsTable map[CompContents]ContentsRecordMem, err error) {
	contentsTable = map[CompContents]ContentsRecordMem{}

	// NOTE: for the time being 'reverseIndexRepo' is not needed
	// keep it for now
	// step 1: read mata_dir table
	sql := "SELECT * FROM contents"
	rows, err := tx.Queryx(sql)
	if err != nil {
		Printf("Select contents. Error in Queryx %v\n", err)
		return nil, err
	}

	for rows.Next() {
		var row ContentsRecordMem
		err = rows.StructScan(&row)
		if err != nil {
			Printf("select contents.StructScan failed %v\n", err)
			return nil, err
		}

		cmpix := CompContents{Blob__id: IntID(row.Blob__id), Position: row.Position,
			Offset: row.Offset}
		row.Status = DBDELETE
		contentsTable[cmpix] = row
	}

	// step 2: look for new rows
	for parent_int, tree := range metaDataAll {
		for position, node := range tree.Nodes {
			if node.Type != "file" || len(node.Content) == 0 { continue }

			for offset, data_blob := range node.Content {
				row_ix_repo, ok := IndexRepoTable[data_blob.String()]
				if ! ok {
					Printf("Internal inconsistency for data_blob %s. Aborting!\n", data_blob[:12])
					panic("Internal inconsistency for data_blob. Aborting!")
				}

				cmpix := CompContents{Blob__id: parent_int, Position: position,	Offset: offset}
				row, ok := contentsTable[cmpix]
				if ok && (row.Status == DBDELETE || row.Status == DBOK) {
					row.Status = DBOK
					contentsTable[cmpix] = row
					continue
				}

				row = ContentsRecordMem{Status: DBNEW, Data__id: row_ix_repo.Id,
					Blob__id: int(parent_int), Position: position, Offset: offset}
				contentsTable[cmpix] = row
			}
		}
	}

	if len(contentsTable) > 0 {
		err = InsertTable("contents", contentsTable, tx, table_column_names, changes_made)
		if err != nil {
			Printf("InsertTable(contents) %v\n", err)
			return nil, err
		}
	}
	return contentsTable, nil
}

func ProcessFileDataTable(tx *sqlx.Tx, table_column_names map[string][]string,
changes_made *bool, namesTable map[string]NamesRecordMem,
metaDataAll map[IntID]*restic.Tree) (fileDataTable map[CompIddFile]IddFileRecordMem, err error) {
	fileDataTable = map[CompIddFile]IddFileRecordMem{}

	// step 1: read idd_file table
	sql := "SELECT * FROM idd_file"
	rows, err := tx.Queryx(sql)
	if err != nil {
		Printf("Select idd_file. Error in Queryx %v\n", err)
		return nil, err
	}

	for rows.Next() {
		var row IddFileRecordMem
		err = rows.StructScan(&row)
		if err != nil {
			Printf("select idd_file.StructScan failed %v\n", err)
			return nil, err
		}

		cmpix := CompIddFile{meta_blob: IntID(row.Blob__id), position: row.Position}
		row.Status = DBDELETE
		fileDataTable[cmpix] = row
	}

	// step 2: look for new rows
	highID := sqlite.Get_high_id("idd_file")
	for parent_int, tree := range metaDataAll {
		for position, node := range tree.Nodes {
			cmpix := CompIddFile{meta_blob: parent_int, position: position}
			row, ok := fileDataTable[cmpix]
			if ok && (row.Status == DBDELETE || row.Status == DBOK) {
				row.Status = DBOK
				fileDataTable[cmpix] = row
				continue
			}
			name_row, ok := namesTable[node.Name]
			if ! ok {
				Printf("Internal inconsistency for name %s. Aborting!\n", node.Name)
				panic("Internal inconsistency for namesTable. Aborting!")
			}

			row = IddFileRecordMem{Status: DBNEW, Id: highID, Blob__id: int(parent_int),
				Position: position, Name__id: name_row.Id, Size: int(node.Size),
				Inode: int64(node.Inode), Mtime: node.ModTime.String()[:19], Type: node.Type}
			fileDataTable[cmpix] = row
			highID++
		}
	}

	if len(fileDataTable) > 0 {
		err = InsertTable("idd_file", fileDataTable, tx, table_column_names, changes_made)
		if err != nil {
			Printf("InsertTable(idd_file) %v\n", err)
			return nil, err
		}
	}
	return fileDataTable, nil
}

func ProcessFullNameTable(tx *sqlx.Tx, table_column_names map[string][]string,
changes_made *bool, fullname map[IntID]string, children map[IntID]mapset.Set[IntID]) (
fullnameTable map[string]FullnameMem, pathDirTable map[int]DirPathIdMem, err error) {

	//  step 1: initialize
	fullnameTable = map[string]FullnameMem{}
	pathDirTable  = map[int]DirPathIdMem{}

	/* since 'fullname' points from a directory ID to a pathname, a lot of
	 * pathsnames will be repeated a few times
	 * In order to normalize this pattern, an intermediate table will be inserted
	 * which collects all the unique names and a mapping which map that unique
	 * ID back to 'fullname'.
	 * So the visible table is 'dir_path_id' which points to the back table
	 * 'fullname'.
	 */

	// step 2: read fullpath table
	sql := "SELECT * FROM fullname"
	rows, err := tx.Queryx(sql)
	if err != nil {
		Printf("Select fullname. Error in Queryx %v\n", err)
		return nil, nil, err
	}

	for rows.Next() {
		var row FullnameMem
		err = rows.StructScan(&row)
		if err != nil {
			Printf("select fullname.StructScan failed %v\n", err)
			return nil, nil, err
		}

		row.Status = DBDELETE
		fullnameTable[row.Pathname] = row
	}

	// step 3: construct new fullname table rows
	highID := sqlite.Get_high_id("fullname")
	for _, path := range fullname {
		row, ok := fullnameTable[path]
		if ok && (row.Status == DBDELETE || row.Status == DBOK) {
			row.Status = DBOK
			fullnameTable[path] = row
			continue
		}
		row = FullnameMem{Status: DBNEW, Id: highID, Pathname: path}
		fullnameTable[path] = row
		highID++
	}


	// step 4: read dir_path_id table
	sql = "SELECT * FROM dir_path_id"
	rows, err = tx.Queryx(sql)
	if err != nil {
		Printf("Select dir_path_id. Error in Queryx %v\n", err)
		return nil, nil, err
	}

	for rows.Next() {
		var row DirPathIdMem
		err = rows.StructScan(&row)
		if err != nil {
			Printf("select dir_path_id.StructScan failed %v\n", err)
			return nil, nil, err
		}

		row.Status = DBDELETE
		pathDirTable[row.Id] = row
	}

	// step 5: construct dir_path_id table
	for meta_int, path := range fullname {
		if _, ok := fullnameTable[path]; ! ok {
			Printf("Internal inconsistency for fullname %s. Aborting!\n", path)
			panic("Internal inconsistency fullname. Aborting!")
		}
		row, ok := pathDirTable[int(meta_int)];
		if ok && (row.Status == DBDELETE || row.Status == DBOK) {
			row.Status = DBOK
			pathDirTable[int(meta_int)] = row
			continue
		}
		row = DirPathIdMem{Status: DBNEW, Id: int(meta_int), Pathname__id: fullnameTable[path].Id}
		pathDirTable[int(meta_int)] = row
	}

	if len(fullnameTable) > 0 {
		err = InsertTable("fullname", fullnameTable, tx, table_column_names, changes_made)
		if err != nil {
			Printf("InsertTable(fullname) %v\n", err)
			return nil, nil, err
		}
	}

	if len(pathDirTable) > 0 {
		err = InsertTable("dir_path_id", pathDirTable, tx, table_column_names, changes_made)
		if err != nil {
			Printf("InsertTable(dir_path_id) %v\n", err)
			return nil, nil, err
		}
	}
	return fullnameTable, pathDirTable, nil
}

/*
func ManageDeleteRows[KEY comparable, MEM any](tx *sqlx.Tx, any_table map[KEY]MEM,
table_name string, changes_made *bool) (err error) {

	// brute force attack: DELETE table rows via sql DELETE single row interface

	sql := fmt.Sprintf("DELETE FROM %s WHERE id=:id", table_name)
	delete_stmt, err := tx.Prepare(sql)
	if err != nil {
		Printf("Error Prepare %s error is %v\n", sql, err)
		return err
	}

	count := 0
	for _, row := range any_table {
		// Field(0) == Status
		if reflect.ValueOf(row).Field(0).Interface().(uint8) == DBDELETE {
			// Field(1) == Id
			_, err = delete_stmt.Exec(reflect.ValueOf(row).Field(1).Interface().(int))
			if err != nil {
				Printf("Eror deleting row from %s - error is '%v'\n", table_name, err)
				return err
			}
			count++
		}
	}
	delete_stmt.Close()
	if count > 0 {
		Printf("DELETE FROM %-15s - %7d rows.\n", table_name, count)
		*changes_made = true
	}
	return nil
}
*/

func CheckForeignKeys(tx *sqlx.Tx, echo bool) bool {
	type RemoveTable struct {
		Id int
	}

	// check consistency of keys (idem potence) in the following tables
	var err error
	all_good := true
	var compareTables = [][]string{
		{"snapshots", "id", "", "meta_dir",  "snap__id"},

		{"index_repo", "id", "type='tree'", "meta_dir",  "blob__id"}, // 1
		{"index_repo", "id", "type='tree'", "idd_file",  "blob__id"},
		{"index_repo", "id", "type='data'", "contents",  "data__id"}, // 3
		{"index_repo", "id", "type='tree'", "contents",  "blob__id"}, // 4
		{"index_repo", "id", "type='tree'", "dir_path_id", "id"},

		{"names", "id",       "", "idd_file", "name__id" },           // 5
		{"packfiles", "id",   "", "index_repo", "pack__id", },
		//{"dir_path_id", "pathname__id", "", "fullname", "id"},        // 7
	}

	sql := "SELECT id FROM index_repo WHERE blob=:blob"
	var emptyID int
	var empty_bytes []byte
	empty_bytes = EMPTY_NODE_ID[:]
	err = tx.Get(&emptyID, sql, empty_bytes)
	if err != nil {
		Printf("Can't get EMPTY_NODE_ID - error is %v\n", err)
		return false
	}

	for ix_compare, ctable := range compareTables {
		var sql1, sql2 string
		table1 := ctable[0]
		col1   := ctable[1]
		table2 := ctable[3]
		col2   := ctable[4]
		cond   := ctable[2]
		// sql1 forces the two database sets to be equal - otherwise this will show
		//  up as a difference in the two sets.
		// sql2 tests for the right side(t2.c2) to be a subset of the left hand side(t1.c1)
		if cond == "" {
			sql1 = fmt.Sprintf(`SELECT %s.%s from %s EXCEPT SELECT DISTINCT %s.%s from %s`,
				table1, col1, table1, table2, col2, table2)
			sql2 = fmt.Sprintf(`SELECT DISTINCT %s.%s from %s EXCEPT SELECT %s.%s from %s`,
				table2, col2, table2, table1, col1, table1)
		} else {
			sql1 = fmt.Sprintf(`SELECT %s.%s from %s WHERE %s EXCEPT SELECT DISTINCT %s.%s from %s`,
				table1, col1, table1, cond, table2, col2, table2)
			sql2 = fmt.Sprintf(`SELECT DISTINCT %s.%s As id from %s EXCEPT SELECT %s.%s from %s WHERE %s`,
				table2, col2, table2, table1, col1, table1, cond)
		}


		rows := []int{}
		if ix_compare != 4 {
			if echo {
				Printf("%s %s\n", time.Now().Format("2006-01-02 15:04:05.000"), sql1)
			}
			err = tx.Select(&rows, sql1)
			if err != nil {
				Printf("\nsql1:\n%s\n", sql1)
				Printf("SELECT error in sql1 - error is %v\n", err)
				return false
			}
			if len(rows) > 0 {
				if len(rows) > 1 || rows[0] != emptyID {
					Printf("\nsql1:\n%s\n", sql1)
					Printf("Inconsistency sql1 in %s.%s - found %5d rows\n", table2, col2, len(rows))
					all_good = false
					for ix, row := range rows {
						Printf("row.Id %7d\n", row)
						if ix > 10 { break }
					}
				}
			}
		}

		rows = []int{}
		if echo {
			Printf("%s %s\n", time.Now().Format("2006-01-02 15:04:05.000"), sql2)
		}
		err = tx.Select(&rows, sql2)
		if err != nil {
			Printf("\nsql2:\n%s\n", sql2)
			Printf("SELECT error in sql2 - error is %v\n", err)
			return false
		}
		if len(rows) > 0 {
			Printf("\nsql2:\n%s\n", sql2)
			Printf("Inconsistency sql2 in %s.%s - found %5d rows\n", table2, col2, len(rows))
			all_good = false
			for ix, row := range rows {
				Printf("row.Id %7d\n", row)
				if ix > 10 { break }
			}
		}
	}
	return all_good
}

func runIndexRepoUpdate(tx *sqlx.Tx, IndexRepoUpdateMap map[string]IndexUpdate) error {
	// run UPDATE on index_repo
	sql := "UPDATE index_repo SET pack__id=:pack__id WHERE id=:id"
	updateStmt, err := tx.Prepare(sql)
	if err != nil {
		Printf("Error in prepare '%s' - error is '%v'\n", sql, err)
		return err
	}

	for _, row := range IndexRepoUpdateMap {
		_, err := updateStmt.Exec(row.Pack__id, row.Id)
		if err != nil {
			Printf("Can't run UPDATE - error is '%v'\n", err)
			return err
		}
	}
	Printf("UPDATE index_repo           with %7d rows.\n", len(IndexRepoUpdateMap))
	return nil
}
