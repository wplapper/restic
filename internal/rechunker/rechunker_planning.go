package rechunker

import (
	"cmp"
	"context"
	"golang.org/x/sync/errgroup"
	"path"
	"slices"

	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/ui"
	"github.com/restic/restic/internal/ui/progress"
)

type SizeSort struct {
	fileID restic.ID
	size   uint64
}

type ScheduleAction struct {
	fileID restic.ID
	Type   string // primary or secondary
}

type BuildFilename struct {
	subDirectory restic.ID
	filename     string
}

type CpInfo struct {
	// input
	selectedSnaps []*data.Snapshot
	printer       progress.Printer

	// fileID related
	uniqueFileContent  map[restic.ID][]restic.ID   // per fileID -> content slice
	tempFileContentSet map[restic.ID]restic.IDSet  // per fileID -> content Set
	tempFileIDToFile   map[restic.ID]BuildFilename // per fileID -> (subDirID, basename), to be converted to full pathname (fileIDToFilename)
	fileIDToFilename   map[restic.ID]string        // per fileID -> full pathname
	packfilesPerFileID map[restic.ID]restic.IDSet  // per fileID: used packfiles
	fileSize           map[restic.ID]uint64        // per fileID -> sum((node.Content).Length)
	fileIDSortSize     []SizeSort                  // fileID slice (fileID, size): sorted by descending file size
	copySet            restic.IDSet                // fileID set can be scheduled for copying

	// blob related
	IndexInfo     map[restic.ID]restic.PackedBlob // per blob -> full blob info
	BlobToFileID  map[restic.ID]restic.IDSet      // per blob -> point back to fileID
	allocateBlobs map[restic.ID]bool              // per blob -> bool

	// packfile related
	usedBlobsPerPack map[restic.ID]restic.IDSet // per packfile: contains a set of used blobs
	allocatePacks    map[restic.ID]bool         // per packfile -> bool

	// tree related
	nameSubtree       map[restic.ID]string      // subdirectory -> basename
	parentToChildList map[restic.ID][]restic.ID // parent -> ordered list of children

	// output
	downloadPrimary   []ScheduleAction
	downloadSecondary []ScheduleAction
	packListOrder     []restic.ID
}

type LengthInfo struct {
	LengthIndexInfo         int
	LengthSelectedSnapshots int
	LengthUsedBlobs         int
	LengthSelectedTrees     int
}

func NewCPInfo(selectedSnaps []*data.Snapshot, printer progress.Printer) *CpInfo {
	cpInfo := &CpInfo{
		selectedSnaps: selectedSnaps,
		printer:       printer,

		IndexInfo:          make(map[restic.ID]restic.PackedBlob),
		uniqueFileContent:  make(map[restic.ID][]restic.ID),
		tempFileContentSet: make(map[restic.ID]restic.IDSet),
		tempFileIDToFile:   make(map[restic.ID]BuildFilename),
		fileIDToFilename:   make(map[restic.ID]string),
		packfilesPerFileID: make(map[restic.ID]restic.IDSet),
		usedBlobsPerPack:   make(map[restic.ID]restic.IDSet),
		BlobToFileID:       make(map[restic.ID]restic.IDSet),
		allocatePacks:      make(map[restic.ID]bool),
		allocateBlobs:      make(map[restic.ID]bool),
		fileSize:           make(map[restic.ID]uint64),
		nameSubtree:        make(map[restic.ID]string),
		parentToChildList:  make(map[restic.ID][]restic.ID),
		copySet:            restic.NewIDSet(),
	}

	return cpInfo
}

// buildname recusively builds the name tree, overwriting the basename entry
func (cpInfo *CpInfo) buildNameTree(parent restic.ID, pathName string) {
	for _, child := range cpInfo.parentToChildList[parent] {
		currentPath := path.Join(pathName, cpInfo.nameSubtree[child])
		cpInfo.nameSubtree[child] = currentPath

		cpInfo.buildNameTree(child, currentPath)
	}
}

func (cpInfo *CpInfo) buildFullpathNames() {
	for fileID, info := range cpInfo.tempFileIDToFile {
		subDir := cpInfo.nameSubtree[info.subDirectory]
		cpInfo.fileIDToFilename[fileID] = path.Join(subDir, info.filename)
	}

	cpInfo.tempFileIDToFile = nil
	cpInfo.nameSubtree = nil
}

// processNode works on a fileID and collects information about
// blobs, their size and their packfiles. Also checks if a blob
// is used across different fileIDs
func (cpInfo *CpInfo) processNode(fileID restic.ID, node *data.Node) {
	size := uint64(0)
	packSet := restic.NewIDSet()
	for _, blob := range node.Content {
		packID := cpInfo.IndexInfo[blob].PackID
		size += uint64(cpInfo.IndexInfo[blob].Length)
		packSet.Insert(packID)

		// link 'blob' to fileID and  its packfile
		if _, ok := cpInfo.BlobToFileID[blob]; !ok {
			cpInfo.BlobToFileID[blob] = restic.NewIDSet()
		}
		cpInfo.BlobToFileID[blob].Insert(fileID)
		if _, ok := cpInfo.usedBlobsPerPack[packID]; !ok {
			cpInfo.usedBlobsPerPack[packID] = restic.NewIDSet()
		}
		cpInfo.usedBlobsPerPack[packID].Insert(blob)
	}

	// memorize size and packset
	cpInfo.fileSize[fileID] = size
	cpInfo.packfilesPerFileID[fileID] = packSet
}

// processTreeItem walk through a whole subdirectory (list of nodes) and
// collects contents resp. subdirectory information
func (cpInfo *CpInfo) processTreeItem(tree data.TreeItem) {
	treeID := tree.ID
	cpInfo.parentToChildList[tree.ID] = make([]restic.ID, 0, len(tree.Nodes))
	for _, node := range tree.Nodes {
		if node.Type == data.NodeTypeFile {
			fileID := hashOfIDs(node.Content)
			cpInfo.uniqueFileContent[fileID] = node.Content
			cpInfo.tempFileContentSet[fileID] = restic.NewIDSet(node.Content...)
			cpInfo.tempFileIDToFile[fileID] = BuildFilename{treeID, node.Name}

			// calculate disk size for the file
			cpInfo.processNode(fileID, node)
		} else if node.Type == data.NodeTypeDir {
			ID := *node.Subtree
			cpInfo.nameSubtree[ID] = node.Name
			cpInfo.parentToChildList[treeID] = append(cpInfo.parentToChildList[treeID], ID)
		}
	}
}

// gatherSubTrees uses data.StreamTrees to collect all trees needed for this run
// this will run once
func (cpInfo *CpInfo) gatherSubTrees(ctx context.Context, repo restic.Repository) error {
	selectedTrees := make([]restic.ID, len(cpInfo.selectedSnaps))
	for i, sn := range cpInfo.selectedSnaps {
		selectedTrees[i] = *sn.Tree
	}

	wg, wgCtx := errgroup.WithContext(ctx)
	treeStream := data.StreamTrees(wgCtx, wg, repo, selectedTrees, func(_ restic.ID) bool {
		return false
	}, nil)

	wg.Go(func() error {
		for tree := range treeStream {
			if tree.Error != nil {
				return errors.Fatalf("LoadTree(%v) returned error %v", tree.ID.Str(), tree.Error)
			}

			cpInfo.processTreeItem(tree)
		}
		return nil
	})

	err := wg.Wait()
	if err != nil {
		return err
	}

	// build fullpath names
	for _, tree := range selectedTrees {
		cpInfo.buildNameTree(tree, "/")
	}
	cpInfo.buildFullpathNames()

	return nil
}

// scheduleFileID inserts a new 'fileID' element into the download list
func (cpInfo *CpInfo) scheduleFileID(fileID restic.ID, scheduleType string) {
	cpInfo.copySet.Insert(fileID)
	cpInfo.tempFileContentSet[fileID] = nil //restic.NewIDSet()

	newElement := ScheduleAction{fileID, scheduleType}
	if scheduleType == "primary" {
		cpInfo.downloadPrimary = append(cpInfo.downloadPrimary, newElement)
	} else {
		cpInfo.downloadSecondary = append(cpInfo.downloadSecondary, newElement)
	}
	cpInfo.printNewElement(newElement)
}

// checkSecondaries goes over all 'cpInfo.fileIDSortSize' and 'tempFileContentSet' and
// finds entries which could be scheduled for copying, they are scheduled as "secondary"
func (cpInfo *CpInfo) checkSecondaries() {
	for _, item := range cpInfo.fileIDSortSize {
		fileID := item.fileID
		if cpInfo.copySet.Has(fileID) {
			continue
		}

		// find out if 'fileID' can be scheduled
		canBeScheduled := true
		for blob := range cpInfo.tempFileContentSet[fileID] {
			if !cpInfo.allocateBlobs[blob] {
				canBeScheduled = false
				break
			}
		}
		if !canBeScheduled {
			continue
		}

		// all needed blobs are allocated im memory, schedule file as "secondary"
		cpInfo.scheduleFileID(fileID, "secondary")
	}
}

// sortSizeMap returns a list of packfile IDs entries, sorted by descending
// packfile size
func (cpInfo *CpInfo) sortSizeMap(fileID restic.ID) []SizeSort {
	// step 1: get the needed packfiles and
	// calculate how much each packfile contributes to 'fileID'
	toSortMap := make(map[restic.ID]uint64)
	for _, blob := range cpInfo.uniqueFileContent[fileID] {
		// translate blob to 'packID'
		packID := cpInfo.IndexInfo[blob].PackID
		// sum up
		toSortMap[packID] += uint64(cpInfo.IndexInfo[blob].Length)
	}

	// and sort by descending size of packfile
	toSort := make([]SizeSort, len(toSortMap))
	i := 0
	for packID, size := range toSortMap {
		toSort[i] = SizeSort{packID, size}
		i++
	}
	slices.SortStableFunc(toSort, func(a, b SizeSort) int {
		return cmp.Compare(b.size, a.size)
	})

	return toSort
}

// loadPrimary(fileID) simulates loading packfiles for 'fileID'
func (cpInfo *CpInfo) loadPrimary(fileID restic.ID) {
	cpInfo.printer.V("\nstart primary download for fileID %s\n", fileID.Str())

	for packID := range cpInfo.packfilesPerFileID[fileID] {
		if cpInfo.allocatePacks[packID] {
			// skip if already done this packfile
			continue
		}

		cpInfo.printer.VV("load packfile %s as part of fileID %s with %5d blobs\n",
			packID.Str(), fileID.Str(), len(cpInfo.usedBlobsPerPack[packID]))
		cpInfo.packListOrder = append(cpInfo.packListOrder, packID)

		if !cpInfo.copySet.Has(fileID) {
			cpInfo.scheduleFileID(fileID, "primary")
		}

		cpInfo.allocatePacks[packID] = true
		// mark blobs in this packfile as 'in memory'
		for blob := range cpInfo.usedBlobsPerPack[packID] {
			cpInfo.allocateBlobs[blob] = true

			// simulate: remove these blobs from the download request list
			for fileIDx := range cpInfo.BlobToFileID[blob] {
				cpInfo.tempFileContentSet[fileIDx].Delete(blob)
			}
		}

		// schedule files which became ready during the last download
		cpInfo.checkSecondaries()
	}

	cpInfo.printer.V("ended download for fileID %s\n", fileID.Str())
}

// sortFileIDsBySize sorts the fileIDs by descending size of its contributing blobs
func (cpInfo *CpInfo) sortFileIDsBySize() []SizeSort {
	// sort cpInfo.fileSize once by descending number of blobs in file
	i := 0
	cpInfo.fileIDSortSize = make([]SizeSort, len(cpInfo.fileSize))
	for fileID := range cpInfo.fileSize {
		nBlobs := len(cpInfo.uniqueFileContent[fileID])
		cpInfo.fileIDSortSize[i] = SizeSort{fileID, uint64(nBlobs)}
		i++
	}
	slices.SortStableFunc(cpInfo.fileIDSortSize, func(a, b SizeSort) int {
		return cmp.Compare(b.size, a.size)
	})

	return cpInfo.fileIDSortSize
}

func (cpInfo *CpInfo) shutdownCheck() error {
	if len(cpInfo.copySet) != len(cpInfo.uniqueFileContent) {
		return errors.Fatalf("execpted length of copySet and uniqueFileContent to be euqual, but they are copySet %d and uniqueFileContent %d",
			len(cpInfo.copySet), len(cpInfo.uniqueFileContent))
	}
	cpInfo.copySet = nil

	for fileID, tempFileContentSet := range cpInfo.tempFileContentSet {
		if len(tempFileContentSet) != 0 {
			return errors.Fatalf("execpted length of tempFileContentSet for fileID %s to be zero, but is is %d",
				fileID.Str(), len(tempFileContentSet))
		}
	}
	cpInfo.tempFileContentSet = nil

	// we could in theory use an even more robust approach and remove everything
	// from cpInfo bar the resulting lists
	return nil
}

// CreateCopyPlan gathers a lot of data from various sources
// - index information
// - tree related information, including IDs and names of subTrees
func (cpInfo *CpInfo) CreateCopyPlan(ctx context.Context, repo restic.Repository) ([]ScheduleAction, []ScheduleAction, []restic.ID, error) {

	// get information about all indexed blobs and packfiles
	err := repo.ListBlobs(ctx, func(blob restic.PackedBlob) {
		cpInfo.IndexInfo[blob.ID] = blob
	})
	if err != nil {
		return nil, nil, nil, err
	}

	// gather basic data from trees: data.StreamTrees will do the data gathering job
	if err := cpInfo.gatherSubTrees(ctx, repo); err != nil {
		return nil, nil, nil, err
	}

	// scheduling packfiles
	cpInfo.printer.VV("\n=== Plan: schedule files ==\n")
	cpInfo.downloadPrimary = make([]ScheduleAction, 0, len(cpInfo.uniqueFileContent))
	cpInfo.downloadSecondary = make([]ScheduleAction, 0, len(cpInfo.uniqueFileContent))
	cpInfo.packListOrder = make([]restic.ID, 0, len(cpInfo.usedBlobsPerPack))

	// walk through all the files, sorted by descending size and request downloads
	for _, item := range cpInfo.sortFileIDsBySize() {
		fileID := item.fileID
		if !cpInfo.copySet.Has(fileID) {
			cpInfo.loadPrimary(fileID)
		}
	}

	// shutdown: sanity check and clear used variables
	err = cpInfo.shutdownCheck()
	return cpInfo.downloadPrimary, cpInfo.downloadSecondary, cpInfo.packListOrder, err
}

func (cpInfo *CpInfo) printNewElement(item ScheduleAction) {
	fileID := item.fileID
	Type := item.Type
	cpInfo.printer.V("%s %11d %9s %5d packs %5d blobs %11s %s\n", fileID.Str(),
		cpInfo.fileSize[fileID], Type, len(cpInfo.packfilesPerFileID[fileID]),
		len(cpInfo.uniqueFileContent[fileID]), ui.FormatBytes(cpInfo.fileSize[fileID]),
		cpInfo.fileIDToFilename[fileID])
}

func (cpInfo *CpInfo) GetLengths() LengthInfo {
	return LengthInfo{
		LengthIndexInfo:         len(cpInfo.IndexInfo),
		LengthSelectedSnapshots: len(cpInfo.selectedSnaps),
		LengthUsedBlobs:         len(cpInfo.BlobToFileID),
		LengthSelectedTrees:     len(cpInfo.parentToChildList),
	}
}
