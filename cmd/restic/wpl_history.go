package main

/* execute wpl-history function:
	It shows how the repository grew over time. By default only the latest
	additions are shown
*/

import (
	// system
	"context"
	"errors"
	"time"
	"io/fs"
	"os/exec"
	"path/filepath"
	"strings"

	//argparse
	"github.com/spf13/cobra"

	// restic
	"github.com/wplapper/restic/library/restic"
	"github.com/wplapper/restic/library/repository"

	// sets
	"github.com/deckarep/golang-set/v2"

	// process management
	"github.com/shirou/gopsutil/v3/process"
)

type HistoryOptions struct {
	Timing     bool
	All        bool
	ShowFirst  bool
	Latest     bool
	Detail     int
	FileSystem string
}
var historyOptions HistoryOptions

var cmdHistory = &cobra.Command{
	Use:   "wpl-history [flags] [optional list of snap_ids]",
	Short: "show development of repository over time",
	Long: `show development of repository over time.

OPTIONS
=======
  --all,         -A show all changes since the oldest snapshot
  --timing,      -T show timing data - only half implemented.
  --latest,      -L determine most recent changes
  --detail,      -D less od more detail (between 1 and 4 'D's)
  --file-system, -F filter for file system

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
	DisableAutoGenTag: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runHistory(cmd.Context(), cmd, globalOptions, args)
	},
}

func init() {
	cmdRoot.AddCommand(cmdHistory)
	f := cmdHistory.Flags()
	f.BoolVarP(&historyOptions.All, "all", "A", false, "show all snapshots")
	f.BoolVarP(&historyOptions.Timing, "timing", "T", false, "produce timings")
	f.BoolVarP(&historyOptions.ShowFirst, "show-all", "S", false, "show first snap detail")
	f.BoolVarP(&historyOptions.Latest, "latest", "L", false, "determine latest changes")
	f.StringVarP(&historyOptions.FileSystem, "file-system", "F", "", "filter for filesystem")
	f.CountVarP(&historyOptions.Detail, "Detail", "D", "print dir/file details")
}

type DBStamp struct {
	youngSnapTime    time.Time
	youngSnapID      string
	youngIndexRecord time.Time
	youngIndexFile   string
}

func runHistory(ctx context.Context, cmd *cobra.Command, gopts GlobalOptions, args []string) error {
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

	if historyOptions.Latest {
		changeTimes, err := findLatestChanges(repo, &repositoryData)
		if err != nil {
			return err
		}

		Printf("youngest snap %s at %s\n", changeTimes.youngSnapID,
			changeTimes.youngSnapTime.Format(time.DateTime))
		Printf("youngest cache index at %s\n",
			changeTimes.youngIndexRecord.Format(time.DateTime))
	}

	var data_map map[IntID]mapset.Set[CompIddFile]

	repoHistory := mapset.NewThreadUnsafeSet[IntID]()
	lastIndex   := len(repositoryData.Snaps) - 1
	all         := historyOptions.All
	detail      := historyOptions.Detail
	fileSystem  := historyOptions.FileSystem

	if detail > 0 {
		data_map = map_data_blob_file(&repositoryData)
	}

	selected_snaps := mapset.NewThreadUnsafeSet[string]()
	snapMapShort   := map[string]string{}
	for snap_long  := range repositoryData.SnapMap {
		snapMapShort[snap_long[:8]] = snap_long
	}

	if len(args) > 0 { // try to find the snap_id in the list of repo snapshots
		for _, snap_id := range args {
			snap_long, ok := snapMapShort[snap_id]
			if !ok {
				Printf("snap %s not found in repository, skipping\n", snap_id)
				continue
			}
			selected_snaps.Add(snap_long)
		}

		if selected_snaps.Cardinality() == 0 {
			return errors.New("no valid snaps selected. Terminating!")
		}
	}

	if selected_snaps.Cardinality() > 0 {
		for snap_id := range selected_snaps.Iter() {
			repoHistory := mapset.NewThreadUnsafeSet[IntID]()
			for _, sn := range repositoryData.Snaps {
				if sn.ID.String() != snap_id {
					for metaBlobInt := range repositoryData.MetaDirMap[sn.ID].Iter() {
						repoHistory.Add(metaBlobInt)
						for _, meta := range repositoryData.DirectoryMap[metaBlobInt] {
							repoHistory.Append(meta.content ...)
						}
					}
				} else {
					thisSnap := mapset.NewThreadUnsafeSet[IntID]()
					for metaBlobInt := range repositoryData.MetaDirMap[sn.ID].Iter() {
						thisSnap.Add(metaBlobInt)
						for _, meta := range repositoryData.DirectoryMap[metaBlobInt] {
							thisSnap.Append(meta.content ...)
						}
					}
					reportSnap(sn, thisSnap, repoHistory, &repositoryData, detail, data_map)
					break
				}
			}
		}
		return nil
	}

	for snap_ix, sn := range repositoryData.Snaps {
		if all {
			if fileSystem != "" && fileSystem != sn.Paths[0] {
				continue
			}

			thisSnap := mapset.NewThreadUnsafeSet[IntID]()
			for metaBlobInt := range repositoryData.MetaDirMap[sn.ID].Iter() {
				thisSnap.Add(metaBlobInt)
				for _, meta := range repositoryData.DirectoryMap[metaBlobInt] {
					thisSnap.Append(meta.content ...)
				}
			}
			reportSnap(sn, thisSnap, repoHistory, &repositoryData, detail, data_map)

			// add to repo history instead of repoHistory = repoHistory.Union(thisSnap)
			// this would create a new result Set[IntID]
			for metaBlobInt := range thisSnap.Iter() {
				repoHistory.Add(metaBlobInt)
			}
		} else if snap_ix < lastIndex {
			for metaBlobInt := range repositoryData.MetaDirMap[sn.ID].Iter() {
				repoHistory.Add(metaBlobInt)
				for _, meta := range repositoryData.DirectoryMap[metaBlobInt] {
					repoHistory.Append(meta.content ...)
				}
			}
		} else {
			thisSnap := mapset.NewThreadUnsafeSet[IntID]()
			for metaBlobInt := range repositoryData.MetaDirMap[sn.ID].Iter() {
				thisSnap.Add(metaBlobInt)
				for _, meta := range repositoryData.DirectoryMap[metaBlobInt] {
					thisSnap.Append(meta.content ...)
				}
			}
			reportSnap(sn, thisSnap, repoHistory, &repositoryData, detail, data_map)
		}
	}
	return nil
}

type SnapSummaryRecord struct {
	CountMetaBlobs int
	CountDataBlobs int
	SizesMetaBlobs int
	SizesDataBlobs int
}

func CountSnapSet(theData mapset.Set[IntID], repositoryData *RepositoryData) (SnapSummaryRecord) {

	countMeta := 0
	countData := 0
	sizesMeta := 0
	sizesData := 0
	for blobInt := range theData.Iter() {
		ih := repositoryData.IndexHandle[repositoryData.IndexToBlob[blobInt]]
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

func findLatestChanges(repo *repository.Repository, repositoryData *RepositoryData) (DBStamp, error) {

	// find youngest snapshot
	youngestSn := repositoryData.Snaps[len(repositoryData.Snaps)-1]
	youngestTime := youngestSn.Time

  // find youngest index file in backend! This works for MS onedrive, but
  // might NOT work for other backends!!
	var (
		err error
		youngestPathI string
		youngestTimeI time.Time
	)

	subdir_name := globalOptions.Repo
	if subdir_name[0:1] != "/" {
		check_and_mount() // onedrive
		comp := strings.Split(subdir_name, ":")
		subdir_name = "/media/wplapper/onedrive/" + comp[len(comp) - 1]
		//Printf("onedrive root: %s\n", subdir_name)
	}


	// year int, month Month, day, hour, min, sec, nsec int, loc *Location
	youngestTimeI = time.Date(1999, 12, 31, 23, 59, 59, 1, time.UTC)
	err = filepath.Walk(subdir_name+"/index", func(path string, info fs.FileInfo, err error) error {
		// skip on error
		if err != nil {
			Printf("prevent panic by handling failure accessing path %q: '%v'\n",
				path, err)
			return err
		}

		// skip directories
		if info.IsDir() {
			return nil
		}

		if info.ModTime().After(youngestTimeI) {
			youngestTimeI = info.ModTime()
			youngestPathI = filepath.Base(path)
		}
		return nil
	})

	if err != nil {
		return DBStamp{}, err
	}

	return DBStamp{
		youngSnapTime    : youngestTime,
		youngSnapID      : youngestSn.ID.String()[:8],
		youngIndexRecord : youngestTimeI,
		youngIndexFile   : youngestPathI,
	}, nil
}

func reportSnap(sn SnapshotWpl, thisSnap mapset.Set[IntID], repoHistory mapset.Set[IntID],
repositoryData *RepositoryData, detail int, data_map map[IntID]mapset.Set[CompIddFile]) {
	diff := thisSnap.Difference(repoHistory)
	summary := CountSnapSet(diff, repositoryData)
	if summary.CountMetaBlobs == 0 {
		return
	}

	Printf("\n*** %s %s %s:%s ***\n", sn.ID.Str(), sn.Time.Format(time.DateTime),
		sn.Hostname, sn.Paths[0])
	Printf("meta %7d %10.1f MiB ", summary.CountMetaBlobs,
		float64(summary.SizesMetaBlobs) / ONE_MEG)
	Printf("data %7d %10.1f MiB\n", summary.CountDataBlobs,
		float64(summary.SizesDataBlobs) / ONE_MEG)

	if detail > 0 && summary.CountDataBlobs  <= 100 {
		if detail == 4 {
			Print_very_raw(repositoryData, diff)
		} else if detail == 3 {
			Print_raw(repositoryData, diff, data_map)
		} else if detail == 2 || detail == 1 {
			Print_some_detail(repositoryData, diff, detail, true, data_map)
		}
	}
}

// check if rclone mount is running, if not start it
func check_and_mount () error {
	found, err := check_mount()
	if found {
		return nil
	}

	// start new process for rclone mount
	Verboseff("rclone mount not found - starting\n")
	err = exec.Command("/usr/bin/daemonize", "/usr/bin/rclone",
         "mount",
         "--vfs-cache-mode", "full",
         "--allow-other",
         "--allow-non-empty",
         "--poll-interval", "30s",
         "onedrive:", "/media/wplapper/onedrive").Run()
	if err != nil {
		Printf("Could not start rclone mount - error is '%v'\n", err)
		return err
	}

	// do a loop with short sleeps in between
	for {
		time.Sleep(1 * time.Second)
		found, err := check_mount()
		if err != nil {
			return err
		}
		if found {
			return nil
		}
	}
	// make sure the rclone mount daemon is ready to receive requests
	time.Sleep(1500 * time.Millisecond)
	return nil
}

// check if rclone mount ondrive: is running
func check_mount() (bool, error) {
	processSlice, err := process.Processes()
	if err != nil {
		Printf("Can't get list of processes - error is '%v'\n", err)
		return false, err
	}

	found := false
	for _, proc := range processSlice {
		cmdSlice, err := proc.CmdlineSlice()
		if err != nil {
			Printf("Can't get CmdlineSlice - error is '%v'\n", err)
			return false, err
		}

		if len(cmdSlice) >= 9 &&
			cmdSlice[0] == "/usr/bin/rclone" &&
		  cmdSlice[1] == "mount" &&
		  cmdSlice[8] == "onedrive:" {
			found = true
			break
		}
	}
	return found, nil
}
