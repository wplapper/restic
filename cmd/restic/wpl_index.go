package main

// collect some info about the index files of this repository
// do it manually - access index file(s) via Backend().List(),
// repo.LoadUnpacked() and json.Unmarshal()

import (
  // system
  "context"
  "encoding/json"

  //argparse
  "github.com/spf13/cobra"

  // restic library
  "github.com/wplapper/restic/library/backend"
  "github.com/wplapper/restic/library/restic"
)

// copied from library/repository/index/index.go - top down
type jsonIndex struct {
  Packs []packJSON `json:"packs"`
}

type packJSON struct {
  ID    restic.ID  `json:"id"`
  Blobs []blobJSON `json:"blobs"`
}

type blobJSON struct {
  ID                 restic.ID       `json:"id"`
  Type               restic.BlobType `json:"type"`
  Offset             uint            `json:"offset"`
  Length             uint            `json:"length"`
  UncompressedLength uint            `json:"uncompressed_length,omitempty"`
}

var cmdIndex = &cobra.Command{
  Use:   "wpl-index",
  Short: "analyze index files manually",
  Long: `analyze index files manually.

EXIT STATUS
===========
Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
  DisableAutoGenTag: false,
  RunE: func(cmd *cobra.Command, args []string) error {
    return runIndex(cmd.Context(), cmd, globalOptions)
  },
}

func init() {
  cmdRoot.AddCommand(cmdIndex)
}

func runIndex(ctx context.Context, cmd *cobra.Command, gopts GlobalOptions) error {
  repo, err := OpenRepository(ctx, gopts)
  if err != nil {
    return err
  }

  var fileInfos []backend.FileInfo
  err = repo.Backend().List(ctx, restic.IndexFile, func(be_info backend.FileInfo) error {
    fileInfos = append(fileInfos, be_info)
    return nil
  })
  if err != nil {
    Printf("be.List failed - reason %v\n", err)
    return err
  }

  for _, finfo := range fileInfos {
    Id, err := restic.ParseID(finfo.Name)
    if err != nil {
      Printf("Can't Parse index filename - reason %v\n", err)
      return err
    }

    // read data from IndexFile
    buf, err := repo.LoadUnpacked(ctx, restic.IndexFile, Id)
    if err != nil {
      Printf("repo.LoadUnpacked failed -reason %v\n", err)
      return err
    }

    //run  "DecodeIndex(buf, Id)" manually
    idxJSON := &jsonIndex{}
    err = json.Unmarshal(buf, idxJSON)
    if err != nil {
      Printf("json.Unmarshal - reason %v", err)
      return err
    }

    Printf("index file %s\n", finfo.Name[:12])
    for _, pack := range idxJSON.Packs {
      packID := pack.ID.String()[:12]

      for _, blob := range pack.Blobs {
        Printf("  packID %s ty %s ID %s off %8d ln %8d uln %8d\n",
          packID,
          blob.Type,
          blob.ID.String()[:12],
          blob.Offset,
          blob.Length,
          blob.UncompressedLength,
        )
      }
    }
  }
  return nil
}
