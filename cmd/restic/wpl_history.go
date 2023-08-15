package main

/* execute wpl-history function:
	It shows how the repository grew over time. By default only the latest
	additions are shown
*/

import (
	// system
	"context"
	//"time"
	//"strings"

	//argparse
	"github.com/spf13/cobra"

	// restic
	"github.com/wplapper/restic/library/restic"

	// sets
	"github.com/deckarep/golang-set/v2"
)

type SnapSummaryRecord struct {
	CountMetaBlobs int
	CountDataBlobs int
	SizesMetaBlobs int
	SizesDataBlobs int
}


type HistoryOptions struct {
	Timing bool
	All    bool
}
var historyOptions HistoryOptions

var cmdHistory = &cobra.Command{
	Use:   "wpl-history [flags]",
	Short: "show development of repository over time",
	Long: `show development of repository over time.

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
	DisableAutoGenTag: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runHistory(cmd.Context(), cmd, globalOptions)
	},
}

func init() {
	cmdRoot.AddCommand(cmdHistory)
	f := cmdHistory.Flags()
	f.BoolVarP(&historyOptions.All, "all", "A", false, "show all snapshots")
	f.BoolVarP(&historyOptions.Timing, "timing", "T", false, "produce timings")
}

func runHistory(ctx context.Context, cmd *cobra.Command, gopts GlobalOptions) error {
	var (
		err            error
		repositoryData RepositoryData
	)
	init_repositoryData(&repositoryData)

	repo, err := OpenRepository(ctx, gopts)
	if err != nil {
		return err
	}

	Verboseff("Repository is %s\n", globalOptions.Repo)
	err = gather_base_data_repo(repo, gopts, ctx, &repositoryData, historyOptions.Timing)
	if err != nil {
		return err
	}

	repoHistory := mapset.NewSet[IntID]()
	lastIndex := len(repositoryData.snaps) - 1
	all := historyOptions.All
	for snap_ix, sn := range repositoryData.snaps {
		if ! all && snap_ix < lastIndex {
			id_ptr := Ptr2ID(*sn.ID(), &repositoryData)
			for metaBlobInt := range repositoryData.meta_dir_map[id_ptr].Iter() {
				repoHistory.Add(metaBlobInt)
				for _, meta := range repositoryData.directory_map[metaBlobInt] {
					repoHistory.Append(meta.content ...)
				}
			}
		} else if ! all && snap_ix == lastIndex {
			thisSnap := mapset.NewSet[IntID]()
			id_ptr := Ptr2ID(*sn.ID(), &repositoryData)
			for metaBlobInt := range repositoryData.meta_dir_map[id_ptr].Iter() {
				thisSnap.Add(metaBlobInt)
				for _, meta := range repositoryData.directory_map[metaBlobInt] {
					thisSnap.Append(meta.content ...)
				}
			}

			// form differences
			summary := CountSnapSet(thisSnap.Difference(repoHistory), &repositoryData)
			if summary.CountMetaBlobs == 0 { continue }
			Printf("%s %s %s:%s\n", sn.ID().Str(), sn.Time.String()[:19],
				sn.Hostname, sn.Paths[0])
			Printf("meta %7d %10.1f MiB ", summary.CountMetaBlobs,
				float64(summary.SizesMetaBlobs) / ONE_MEG)
			Printf("data %7d %10.1f MiB\n", summary.CountDataBlobs,
				float64(summary.SizesDataBlobs) / ONE_MEG)
		}	else if all {
			thisSnap := mapset.NewSet[IntID]()
			id_ptr := Ptr2ID(*sn.ID(), &repositoryData)
			for metaBlobInt := range repositoryData.meta_dir_map[id_ptr].Iter() {
				thisSnap.Add(metaBlobInt)
				for _, meta := range repositoryData.directory_map[metaBlobInt] {
					thisSnap.Append(meta.content ...)
				}
			}

			// form differences
			summary := CountSnapSet(thisSnap.Difference(repoHistory),
				&repositoryData)
			if summary.CountMetaBlobs == 0 { continue }
			Printf("%s %s %s:%s\n", sn.ID().Str(), sn.Time.String()[:19],
				sn.Hostname, sn.Paths[0])
			Printf("meta %7d %10.1f MiB ", summary.CountMetaBlobs,
				float64(summary.SizesMetaBlobs) / ONE_MEG)
			Printf("data %7d %10.1f MiB\n", summary.CountDataBlobs,
				float64(summary.SizesDataBlobs) / ONE_MEG)

			// add to repo history instead of repoHistory = repoHistory.Union(thisSnap)
			for metaBlobInt := range thisSnap.Iter() {
				repoHistory.Add(metaBlobInt)
			}
		}
	}
	return nil
}

func CountSnapSet(theData mapset.Set[IntID], repositoryData *RepositoryData) (SnapSummaryRecord ) {
	countMeta := 0
	countData := 0
	sizesMeta := 0
	sizesData := 0
	for blobInt := range theData.Iter() {
		ih := repositoryData.index_handle[repositoryData.index_to_blob[blobInt]]
		if ih.Type == restic.TreeBlob {
			countMeta++
			sizesMeta += ih.size
		} else if ih.Type == restic.DataBlob {
			countData++
		  sizesData += ih.size
		}
	}
	return SnapSummaryRecord{
		CountMetaBlobs: countMeta, CountDataBlobs: countData,
		SizesMetaBlobs: sizesMeta, SizesDataBlobs: sizesData,
	}
}
