package main

import (
  // system
  "context"
  "golang.org/x/sync/errgroup"
  "runtime"
  "sort"
  "sync"
  "time"

  // restic library
  "github.com/wplapper/restic/library/repository"
  "github.com/wplapper/restic/library/restic"

  // sets
  "github.com/deckarep/golang-set/v2"

  // stack and queue
  "github.com/golang-collections/collections/queue"
)

// system wide variables and containers
var (
  gOptions                 GlobalOptions
  repositoryData           *RepositoryData
  EMPTY_NODE_ID            restic.ID
  EMPTY_NODE_ID_TRANSLATED IntID
)

func init_repositoryData(repositoryData *RepositoryData) {
  repositoryData.Snaps = []SnapshotWpl{}
  repositoryData.SnapMap = map[string]SnapshotWpl{}
  repositoryData.DirectoryMap = map[IntID][]BlobFile2{}
  repositoryData.FullPath = map[IntID]string{}
  repositoryData.MetaDirMap = map[restic.ID]mapset.Set[IntID]{}
  repositoryData.IndexHandle = map[restic.ID]Index_Handle{}
  repositoryData.BlobToIndex = map[restic.ID]IntID{}
  repositoryData.IndexToBlob = make([]restic.ID, 2, 300000)
}

// GatherAllSnapshots retrieves all snapshots from the repository
// return: slice of all *sn, sorted by ascending snapshot time (*sn.Time)
func GatherAllSnapshots(ctx context.Context,
  repo *repository.Repository) ([]SnapshotWpl, map[string]SnapshotWpl, error) {
  // collect all snap records
  snaps := []SnapshotWpl{}
  repo.List(ctx, restic.SnapshotFile, func(id restic.ID, size int64) error {
    sn, err := restic.LoadSnapshot(ctx, repo, id)
    if err != nil {
      Printf("GatherAllSnapshots.skip loading snap record %s! - reason: %v\n", id, err)
      return err
    }
    snaps = append(snaps, SnapshotWpl{
      ID: *sn.ID(), Hostname: sn.Hostname, Paths: sn.Paths, Tree: *sn.Tree,
      Time: sn.Time,
    })
    return nil
  })

  // custom sort: we sort 'snaps' by sn.Time
  sort.SliceStable(snaps, func(i, j int) bool {
    return snaps[i].Time.Before(snaps[j].Time)
  })

  // fill snap_map
  snap_map := make(map[string]SnapshotWpl, len(snaps))
  for _, sn := range snaps {
    snap_map[sn.ID.String()] = sn
  }
  return snaps, snap_map, nil
}

func HandleIndexRecords(ctx context.Context, repo *repository.Repository,
  repositoryData *RepositoryData) error {
  // load index files and their contents
  // 'LoadIndex' is in library/repository/repository.go, needs to happen first
  if err := repo.LoadIndex(ctx, nil); err != nil {
    Printf("repo.LoadIndex - failed. Error is %v\n", err)
    return err
  }

  ConvertToIntSet(ctx, repo, repositoryData)
  return nil
}

// this function converts restic.ID to IntID, forth and back
// It also correlates blobs and pack IDs
// build 'repositoryData.BlobToIndex' and 'repositoryData.IndexToBlob'
func ConvertToIntSet(ctx context.Context, repo *repository.Repository,
  repositoryData *RepositoryData) {

  EMPTY_NODE_ID = restic.Hash([]byte(`{"nodes":[]}` + "\n"))
  // ID,  PackID, Type, Length and Offset
  // build 'blob_to_index' and 'index_to_blob' for all known restic.ID(s)
  // sources are the index files, packfiles and the snapshot records

  // this is the start of the NORMAL index records. Slot 0 and 1 are reserved!
  var pos IntID
  var lastPackIndex IntID
  if len(repositoryData.IndexToBlob) == 0 {
    pos = 2
  } else {
    pos = IntID(len(repositoryData.IndexToBlob))
  }

  // loop over all indices
  err := repo.ListBlobs(ctx, func(blob restic.PackedBlob) {
    if blob.Type == restic.TreeBlob {
      if _, ok := repositoryData.BlobToIndex[blob.ID]; !ok {
        repositoryData.BlobToIndex[blob.ID] = pos
        repositoryData.IndexToBlob = append(repositoryData.IndexToBlob, blob.ID)
        pos++
      }
    }
  })
  if err != nil {
    Printf("repo.ListBlobs failed with %v\n", err)
    return
  }

  repo.ListBlobs(ctx, func(blob restic.PackedBlob) {
    if blob.Type == restic.DataBlob {
      if _, ok := repositoryData.BlobToIndex[blob.ID]; !ok {
        repositoryData.BlobToIndex[blob.ID] = pos
        repositoryData.IndexToBlob = append(repositoryData.IndexToBlob, blob.ID)
        pos++
      }
    }
  })

  repo.ListBlobs(ctx, func(blob restic.PackedBlob) {
    blPack := blob.PackID
    if _, ok := repositoryData.BlobToIndex[blPack]; !ok {
      repositoryData.BlobToIndex[blPack] = pos
      repositoryData.IndexToBlob = append(repositoryData.IndexToBlob, blPack)
      pos++
    }

    // build our index_handle from 'blob' records
    lastPackIndex = repositoryData.BlobToIndex[blPack]
    repositoryData.IndexHandle[blob.ID] = Index_Handle{Type: blob.Type,
      size:               int(blob.Length),
      pack_index:         lastPackIndex,
      blob_index:         repositoryData.BlobToIndex[blob.ID],
      UncompressedLength: int(blob.UncompressedLength),
    }
  })

  // add the restic.IDs from the snapshots list to these list / maps
  for _, sn := range repositoryData.Snaps {
    snap := sn.ID
    if _, ok := repositoryData.BlobToIndex[snap]; !ok {
      repositoryData.BlobToIndex[snap] = pos
      repositoryData.IndexToBlob = append(repositoryData.IndexToBlob, snap)
      pos++
    }
  }

  // we need to initialize EMPTY_NODE_ID_TRANSLATED
  ok := false
  EMPTY_NODE_ID_TRANSLATED, ok = repositoryData.BlobToIndex[EMPTY_NODE_ID]
  if !ok {
    repositoryData.BlobToIndex[EMPTY_NODE_ID] = pos
    repositoryData.IndexToBlob = append(repositoryData.IndexToBlob, EMPTY_NODE_ID)
    EMPTY_NODE_ID_TRANSLATED = pos
  }
}

// FindChildren steps through the directory_map and finds subdirectories.
// The children get attached their parent
func FindChildren(repositoryData *RepositoryData) (children map[IntID]mapset.Set[IntID]) {
  children = CreateAllChildren(repositoryData)
  subdirectoryNames := make(map[IntID]string)
  for _, file_list := range repositoryData.DirectoryMap {
    for _, node := range file_list {
      if node.subtree_ID == EMPTY_NODE_ID_TRANSLATED {
        continue
      }
      subdirectoryNames[node.subtree_ID] = node.name
    }
  }

  // 2. create tree root names for fullpath
  repositoryData.roots = make([]RootOfTree, 0, len(repositoryData.Snaps))
  initials := mapset.NewThreadUnsafeSet[IntID]()
  for _, sn := range repositoryData.Snaps {
    tree := repositoryData.BlobToIndex[sn.Tree]
    repositoryData.FullPath[tree] = "/"
    initials.Add(tree)
  }

  // 3. create full name tree for all meta_blobs (repositoryData.FullPath)
  // dfs sorts the children topologically, so parents appear before their children
  t1 := time.Now()
  dfs_res := dfs2(children, initials)
  if DFS_DEBUG {
    Printf("time for dfs2 %10.3f seconds, %d entries\n", time.Since(t1).Seconds(),
      len(dfs_res))
  }

  for _, meta_blob := range dfs_res {
    if meta_blob == EMPTY_NODE_ID_TRANSLATED {
      continue
    }
    for child := range children[meta_blob].Iter() {
      if child == EMPTY_NODE_ID_TRANSLATED {
        continue
      }
      if repositoryData.FullPath[meta_blob] == "/" {
        repositoryData.FullPath[child] = "/" + subdirectoryNames[child]
      } else {
        repositoryData.FullPath[child] = repositoryData.FullPath[meta_blob] + "/" +
          subdirectoryNames[child]
      }
    }
  }
  return children
}

// build a topology structure for one snapshot
// the function relies on 'children' being initialized properly
// this basicly a 'bfs' algorithm (parents come before their children)
func TopologyStructure(root IntID, children map[IntID]mapset.Set[IntID]) (visited mapset.Set[IntID]) {

  // return container initialisation
  visited = mapset.NewThreadUnsafeSet[IntID]()

  // use 'queue' as a FIFO queue to walk the topology, breadth first
  bQueue := queue.New()
  bQueue.Enqueue(root)
  for bQueue.Len() > 0 {
    next := bQueue.Dequeue().(IntID)
    visited.Add(next)
    for child := range children[next].Iter() {
      if !visited.Contains(child) {
        bQueue.Enqueue(child)
      }
    }
  }

  // at the end of the loop, 'visited' contains all directories
  // referenced in the snapshot
  return visited
}

// this methods runs through all the steps to gather the pertinent repository data
func GatherAllRepoData(ctx context.Context, repo *repository.Repository,
  repositoryData *RepositoryData) error {
  // step 1: build a slice of all meta_blob IDs in the repo
  if err := ForAllMyTrees(ctx, repo, repositoryData); err != nil {
    Printf("ForAllMyTrees returned %v\n", err)
    return err
  }

  // step 2: prepare children and parents from meta_data_store records
  children := FindChildren(repositoryData)

  // step 3: build flat topology Set for each snapshot in repository
  for _, sn := range repositoryData.Snaps {
    repositoryData.MetaDirMap[sn.ID] = TopologyStructure(
      repositoryData.BlobToIndex[sn.Tree], children)
  }
  return nil
}

// auxiliary function to deliver meta blobs from the index to ForAllMyTrees
// for parallel processing
func DeliverTreeBlobs(repositoryData *RepositoryData, fn func(restic.ID) error) error {

  for blob_ID, data := range repositoryData.IndexHandle {
    if data.Type == restic.TreeBlob {
      fn(blob_ID)
    }
  }
  return nil
}

// home built parallel call to restic.LoadTree. All trees are accessed by the
// method 'DeliverTreeBlobs' which accesses 'repositoryData.IndexHandle'
// which has been built beforehand
func ForAllMyTrees(ctx context.Context, repo *repository.Repository, repositoryData *RepositoryData) error {

  var m sync.Mutex
  wg, ctx := errgroup.WithContext(ctx)
  chan_tree_blob := make(chan restic.ID)

  wg.Go(func() error {
    defer close(chan_tree_blob)

    // this callback function get fed the 'id'
    return DeliverTreeBlobs(repositoryData, func(id restic.ID) error {
      select {
      case <-ctx.Done():
        return nil
      case chan_tree_blob <- id:
        return nil
      }
      return nil
    })
  })

  // a worker receives a metablob ID from chan_tree_blob, loads the tree
  // and runs fn with id, the snapshot and the error
  //var name string
  worker := func() error {
    for id := range chan_tree_blob {
      tree, err := restic.LoadTree(ctx, repo, id)
      if err != nil {
        Printf("LoadTree returned %v\n", err)
        return err
      }

      idd_file_list := make([]BlobFile2, len(tree.Nodes))
      // do the work on the tree just received
      for offset_in_node_list, node := range tree.Nodes {
        // setup these two place holders
        content := []IntID{}
        subt_ID := EMPTY_NODE_ID_TRANSLATED

        if node.Type == "file" {
          for _, cont := range node.Content {
            // get the index for our restic.ID storage
            ix_data, ok := repositoryData.BlobToIndex[cont]
            if !ok {
              Printf("Fatal: %v not in blob_to_index\n", cont)
              panic("ForAllMyTrees: error during content processing")
            }
            content = append(content, ix_data)
          }
        } else if node.Type == "dir" {
          subt_ID = repositoryData.BlobToIndex[*node.Subtree]
        }

        blob_file := BlobFile2{name: node.Name,
          Type: node.Type, size: node.Size,
          DeviceID: node.DeviceID, inode: node.Inode,
          mtime: node.ModTime, content: content, subtree_ID: subt_ID,
          Mode: node.Mode, Links: node.Links,
          LinkTarget: node.LinkTarget}
        idd_file_list[offset_in_node_list] = blob_file
      }

      ix := repositoryData.BlobToIndex[id]
      // insert directory_map: this is the critical region, so lock it
      m.Lock()
      repositoryData.DirectoryMap[ix] = idd_file_list
      m.Unlock()
    }
    return nil
  }

  // start all these parallel workers
  max_parallel := int(repo.Connections()) + runtime.GOMAXPROCS(0)
  for i := 0; i < max_parallel; i++ {
    wg.Go(worker)
  }
  return wg.Wait()
}

// PrintMemUsage outputs the current, total and OS memory being used.
// As well as the number of garbage collection cycles completed.
func PrintMemUsage() {

  var m2 runtime.MemStats
  runtime.GC()
  runtime.ReadMemStats(&m2)
  // For info on each, see: https://golang.org/pkg/runtime/#MemStats
  Printf("Alloc = %4d MiB", bToMb(m2.Alloc))
  Printf("\tSys = %4d MiB", bToMb(m2.Sys))
  Printf("\tHeap = %4d MiB\n", bToMb(m2.HeapInuse))
}

func bToMb(b uint64) uint64 {
  // full megabytes
  return b / 1024 / 1024
}

func timeMessage(memory_use bool, format string, args ...interface{}) {
  Printf(format, args...)
  if memory_use {
    PrintMemUsage()
  }
}

// collect the usual stuff from a repository:
// snapshots, Index records, node records
func gather_base_data_repo(repo *repository.Repository, gopts GlobalOptions,
  ctx context.Context, repositoryData *RepositoryData, timing bool) error {

  var err error
  start := time.Now()
  if timing {
    timeMessage(true, "%-30s %10.1f seconds\n", "repository is open",
      time.Since(start).Seconds())
  }

  // step 1: gather all snapshots
  repositoryData.Snaps, repositoryData.SnapMap, err = GatherAllSnapshots(ctx, repo)
  if err != nil {
    return err
  }
  if timing {
    timeMessage(true, "%-30s %10.1f seconds\n", "gather snapshots",
      time.Since(start).Seconds())
  }

  // step 2: manage Index Records
  if err = HandleIndexRecords(ctx, repo, repositoryData); err != nil {
    return err
  }
  if timing {
    timeMessage(true, "%-30s %10.1f seconds\n", "read index records",
      time.Since(start).Seconds())
  }

  // step 3: read all meta blobs and create the incore tables
  GatherAllRepoData(ctx, repo, repositoryData)
  if timing {
    timeMessage(true, "%-30s %10.1f seconds\n", "GatherAllRepoData",
      time.Since(start).Seconds())
  }
  return nil
}

const DFS_DEBUG = false

type IntIDSlice []IntID

// create a topological sort of all the tree nodes, starting at all tree roots
func dfs2(childrenMap map[IntID]mapset.Set[IntID],
  initials mapset.Set[IntID]) (results IntIDSlice) {

  // local containers
  results = IntIDSlice{}
  visited := mapset.NewThreadUnsafeSet[IntID]()

  // prime search
  for root := range initials.Iter() {
    bfs(root, &results, childrenMap, visited)
  }
  return results
}

// breadth first search
// bfs uses a FIFO queue for holding the unprocessed elements
// NOTE: 'visited' and 'results' are global to this function, so others
// calls are cumulative.
// During performance testing 'bfs' to be the fastest of the algorithms
func bfs(root IntID, results *IntIDSlice, childrenMap map[IntID]mapset.Set[IntID],
  visited mapset.Set[IntID]) {

  // local container
  bQueue := queue.New()
  bQueue.Enqueue(root)
  for bQueue.Len() > 0 {
    next := bQueue.Dequeue().(IntID)
    if !visited.Contains(next) {
      *results = append(*results, next)
    }
    visited.Add(next)

    // total loop count for the following loop is "count(*) FROM children"
    for child := range childrenMap[next].Iter() {
      if !visited.Contains(child) {
        bQueue.Enqueue(child)
      }
    }
  }
}
