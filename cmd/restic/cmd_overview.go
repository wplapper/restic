package main
// compile with "go run build.go -tags debug""
// run with DEBUG_LOG=/home/wplapper/restic/debug.log fully-qualified-name/restic -r <repo> overview

// ref to onedrive: rclone:onedrive:restic_backups

import (
  // system
  "time"
  "sort"
  "strings"

  //argparse
  "github.com/spf13/cobra"

  // restic library
  "github.com/wplapper/restic/library/restic"
)

type snapGroup struct {
    host string
    fsys string
}

// type BlobSet map[BlobHandle]struct{} . from blob_set.go
/*
 type BlobHandle struct {
  ID   ID
  Type BlobType
 }
 */

var cmdOverview = &cobra.Command{
  Use:   "overview [flags]",
  Short: "show summary of snapshots by host and fiesystems backed up",
  Long: `
show summary of snapshots by host and filesystems backed up.

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
  DisableAutoGenTag: true,
  RunE: func(cmd *cobra.Command, args []string) error {
    return runOverview(globalOptions)
  },
}

func init() {
  cmdRoot.AddCommand(cmdOverview)
}

func runOverview(gopts GlobalOptions) error {

  // step 1: open repository
  start := time.Now()
  repo, err := OpenRepository(gopts)
  if err != nil {
    return err
  }
  Printf("open repository %10.1f seconds.\n", time.Now().Sub(start).Seconds())

  // step 2.1: manage Index Records
  start = time.Now()
  err = HandleIndexRecords(gopts, repo)
  if err != nil {
    return err
  }
  // step 2.2 gather size info for each blob
  blobs_from_ix := make(map[restic.ID]uint)
  for blob := range repo.Index().Each(gopts.ctx) {
    blobs_from_ix[blob.ID] = blob.Length
  }
  Printf("read all index records in %10.1f seconds\n",
      time.Now().Sub(start).Seconds())

  // step 3: gather all snapshots
  start = time.Now()
  snaps := make([]*restic.Snapshot, 0, 10)
  snaps, err = GatherAllSnapshots(gopts, repo, snaps)
  if err != nil {
      return err
  }
  Printf("read %d snapshot records in %10.1f seconds\n", len(snaps),
    time.Now().Sub(start).Seconds())

  // step 4: build snap groups by host and filesystem
  start = time.Now()
  groups := make(map[snapGroup][]*restic.Snapshot)
  for _, sn := range snaps {
      host := sn.Hostname
      if host == "new-PC-Mate-2004-prod" {
          host = "new-PC"
      }
      fsys := sn.Paths[0]
      group := snapGroup{host : host, fsys: fsys}
      groups[group] = append(groups[group], sn)
  }

  // step 5: sort the groups according to host and filesystem
  usage := make(map[snapGroup]uint64)
  // the group list wants to be sorted: host first, then filesystem
  groups_sorted := make([]snapGroup, 0, len(groups))
  for group := range groups {
      groups_sorted = append(groups_sorted, group)
  }

  sort.SliceStable(groups_sorted, func (i, j int) bool {
      if groups_sorted[i].host < groups_sorted[j].host {
          return true
      } else if groups_sorted[i].host > groups_sorted[j].host {
          return false
      }
      return groups_sorted[i].fsys < groups_sorted[j].fsys
  })

  // step 6: extract size information for these groups
  Printf("%-22s %-50s %-5s %-11s %-6s %10s\n",
      "hostname", "filesystem_path", "snaps", "directories", "dblobs", "size[MiB]")
  Printf("%s\n", strings.Repeat("=", 109))
  for _, group := range groups_sorted {
    host  := group.host
    fsys  := group.fsys

    // step 7: build tree list for 'FindUsedBlobs'
    tree_list := make([]restic.ID, 0, len(groups[group]))
    for _, sn := range groups[group] {
        tree_list = append(tree_list, *sn.Tree)
    }

    // step 8: gather blobs for the constructed tree list
    usedBlobs := restic.NewBlobSet()
    err = restic.FindUsedBlobs(gopts.ctx, repo, tree_list, usedBlobs, nil)
    if err != nil {
      return err
    }

    // step 9: access size information on used blobs
    count_data_blobs := 0
    count_meta_blobs := 0
    for blob := range usedBlobs {
        if blob.Type == restic.DataBlob {
            usage[group] += uint64(blobs_from_ix[blob.ID])
            count_data_blobs++
        } else if blob.Type == restic.TreeBlob {
            count_meta_blobs++
        }
    }
    Printf("%-22s %-50s %5d %11d %6d %10.1f\n",
      host, fsys,
      len(groups[group]), count_meta_blobs, count_data_blobs,
      float64(usage[group]) / 1024.0 / 1024.0)
  }
  Printf("gather size data for groups in %10.1f seconds\n",
    time.Now().Sub(start).Seconds())
  return nil
}
