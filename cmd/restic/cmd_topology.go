package main
// compile with "go run build.go -tags debug""
// run with DEBUG_LOG=/home/wplapper/restic/debug.log fully-qualified-name/restic -r <repo> overview

// ref to onedrive: rclone:onedrive:restic_backups

import (
	// system
	"time"
	//"sort"
	//"fmt"

	//argparse
	"github.com/spf13/cobra"

	// restic library
	"github.com/wplapper/restic/library/restic"
)

type TopologyOptions struct {
		cutoff int
		snap string
}

var cmdTopology = &cobra.Command{
	Use:   "topology [flags]",
	Short: "generate topology of one snapshot",
	Long: `generate topology for all snapshots.

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
	DisableAutoGenTag: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runTopology(globalOptions, args)
	},
}

func init() {
	cmdRoot.AddCommand(cmdTopology)
}

func runTopology(gopts GlobalOptions, args []string) error {

	// step 0: setup global stuff
	start := time.Now()
	repositoryData := init_repositoryData()
	EMPTY_NODE_ID = restic.Hash([]byte("{\"nodes\":[]}\n"))
	gOptions = gopts

	// step 1: open repository
	repo, err := OpenRepository(gopts)
	if err != nil {
		return err
	}
	timeMessage("%-30s %10.1f seconds\n", "open repository", time.Now().Sub(start).Seconds())
	PrintMemUsage()

	// step 4.1: manage Index Records
	start = time.Now()
	if err = HandleIndexRecords(gopts, repo, repositoryData); err != nil {
		return err
	}
	timeMessage("%-30s %10.1f seconds\n", "read index records", time.Now().Sub(start).Seconds())
	PrintMemUsage()

	// extract all information about indexes from a call to <master_index>.Each()
	// setup index_handle, blob_to_index and index_to_blob
	start = time.Now()
	GatherAllRepoData(gopts, repo, repositoryData)
	timeMessage("%-30s %10.1f seconds\n", "GatherAllRepoData (sum)", time.Now().Sub(start).Seconds())
	PrintMemUsage()

	// convert the keys of 'directory_map' to a IntID set
	idd_file_keys := restic.NewIntSet()
	for key := range repositoryData.directory_map {
			idd_file_keys.Insert(key)
	}

	blobs_from_ix := restic.NewIntSet()
	for _, data := range repositoryData.index_handle {
		if data.Type == restic.TreeBlob {
			blobs_from_ix.Insert(data.blob_index)
		}
	}

	//compare the sets
	if !idd_file_keys.Equals(blobs_from_ix)  {
		rest := blobs_from_ix.Sub(idd_file_keys)
		Printf("len blobs_from_ix      %7d\n", len(blobs_from_ix))
		Printf("len keys directory_map %7d\n", len(idd_file_keys))
		Printf("len rest               %7d\n", len(rest))
	}

	/*for _, sbp := range sort_by_pack {
		Printf("sbp %s %7d %s\n", sbp.pack_ID.String()[:12], sbp.offset, sbp.blob_ID.String()[:12])
	}*/

	// demo functions
	//service1(repositoryData)
	service2(repositoryData)
	//service3(repositoryData)
	PrintMemUsage()
	service4(repositoryData)
	return nil
}
