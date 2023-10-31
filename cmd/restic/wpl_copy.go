package main

// run backup on host for selected filesystem.
// also execute mange repository, check soundness
// and compare two repositories for identical snap content.

import (
	// system
	"context"
	"time"
	"sort"

	//argparse
	"github.com/spf13/cobra"
)

type CopyFilesystemOptions struct {
	Doit     bool
	Path     string
	Hostname string
	Exclude  string
	secondaryRepoOptions
}

var copy_filesys_opts CopyFilesystemOptions

var cmdResticCopy = &cobra.Command{
	Use:   "wpl-copy [flags]",
	Short: "run restic copy to equalise the source and target repositories",
	Long: `run restic copy to equalise the source and target repositories.
  The repositories are compared and only the missing snapshots are copied to
  the target directory. Currently there is no filtering via host / datestamp
  etc available.

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
	DisableAutoGenTag: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		return RunResticCopy(cmd.Context(), cmd, globalOptions, args)
	},
}

func init() {
	cmdRoot.AddCommand(cmdResticCopy)
	f := cmdResticCopy.Flags()
	f.BoolVarP(&copy_filesys_opts.Doit, "doit", "", false, "execute command")
	f.StringVarP(&copy_filesys_opts.Path, "path", "P", "", "filter by filesystem")
	f.StringVarP(&copy_filesys_opts.Hostname, "host", "H", "", "filter by hostname")
	f.StringVarP(&copy_filesys_opts.Exclude, "exclude", "X", "", "filter to exclude filesystems")
	initSecondaryRepoOptions(f, &copy_filesys_opts.secondaryRepoOptions, "destination", "to copy snapshots from")
}

// the run restic backup command and other subcommands
func RunResticCopy(ctx context.Context, cmd *cobra.Command, gopts GlobalOptions, args []string) error {

	var err error
	gOptions = gopts
	secondaryGopts, _, err := fillSecondaryGlobalOpts(
		copy_filesys_opts.secondaryRepoOptions, gopts, "destination")
	if err != nil {
		Printf("fillSecondaryGlobalOpts failed with '%v'\n", err)
		return err
	}

	err = step_copy(ctx, secondaryGopts, gopts, copy_filesys_opts)
	if err != nil {
		Printf("step_copy returned '%v'\n", err)
		return err
	}
	return nil
}

// compare source and target repositories and copy missing snapshots from
// source to target
func step_copy(ctx context.Context, src_gopts GlobalOptions, dst_gopts GlobalOptions,
	copy_filesys_opts CopyFilesystemOptions) error {
	// open repositories
	srcRepo, err := OpenRepository(ctx, src_gopts)
	if err != nil {
		Printf("FAIL open source repository - %v\n", err)
		return err
	}

	dstRepo, err := OpenRepository(ctx, dst_gopts)
	if err != nil {
		Printf("FAIL open target repository - %v\n", err)
		return err
	}

	// load snapshots
	_, srcSnapMap, err := GatherAllSnapshots(src_gopts, ctx, srcRepo)
	if err != nil {
		Printf("GatherAllSnapshots source - %v\n", err)
		return err
	}
	_, dstSnapMap, err := GatherAllSnapshots(dst_gopts, ctx, dstRepo)
	if err != nil {
		Printf("GatherAllSnapshots target - %v\n", err)
		return err
	}

	// transform snapMaps into triples (SnapHost, SnapFilesys, SnapTime)
	// do some source filtering at the same time
	file_system := copy_filesys_opts.Path
	hostname := copy_filesys_opts.Hostname
	exclude  := copy_filesys_opts.Exclude
	srcMap := map[SnapTriple]string{}
	for snap_id, sn := range srcSnapMap {
		if file_system != "" && sn.Paths[0] != file_system {
			continue
		}
		if exclude != "" && sn.Paths[0] == exclude {
			continue
		}
		if hostname != "" && sn.Hostname != hostname {
			continue
		}

		triple := SnapTriple{Snap_host: sn.Hostname, Snap_fsys: sn.Paths[0],
			Snap_time: sn.Time.Format(time.DateTime)}
		srcMap[triple] = snap_id
	}

	dstMap := map[SnapTriple]string{}
	for snap_id, sn := range dstSnapMap {
		triple := SnapTriple{Snap_host: sn.Hostname, Snap_fsys: sn.Paths[0],
			Snap_time: sn.Time.Format(time.DateTime)}
		dstMap[triple] = snap_id
	}

	copied := false
	srcRepo.Close()
	dstRepo.Close()

	var wpl_copyOptions CopyOptions
	wpl_copyOptions.secondaryRepoOptions = copy_filesys_opts.secondaryRepoOptions

	sort_keys := make([]SnapTriple, 0, len(srcMap))
	for triple := range srcMap {
		// snaps identical?
		if _, ok := dstMap[triple]; ok {
			continue
		}
		sort_keys = append(sort_keys, triple)
	}

	sort.SliceStable(sort_keys, func(i, j int) bool {
		if        sort_keys[i].Snap_host < sort_keys[j].Snap_host {
			return true
		} else if sort_keys[i].Snap_host > sort_keys[j].Snap_host {
			return false
		} else if sort_keys[i].Snap_fsys < sort_keys[j].Snap_fsys {
			return true
		} else if sort_keys[i].Snap_fsys > sort_keys[j].Snap_fsys {
			return false
		} else {
			return sort_keys[i].Snap_time < sort_keys[j].Snap_time
		}
	})

	for _, triple := range sort_keys {
		snap_id := srcMap[triple]
		// prepare to copy
		Printf("\n%s %s: %s  %s:%s\n", time.Now().Format(time.DateTime), snap_id, triple.Snap_time,
			triple.Snap_host, triple.Snap_fsys)
		if copy_filesys_opts.Doit {
			// run restic copy [...]
			copyTime := time.Now()
			Printf("restic copy %s -s %s -t %s\n", snap_id, src_gopts.Repo, dst_gopts.Repo)
			// call command in cmd_copy.go for a single snap_id `snap_id`
			err = runCopy(ctx, wpl_copyOptions, dst_gopts, []string{snap_id})
			if err != nil {
				Printf("runCopy error '%v'\n", err)
				return err
			}
			Printf("time to copy snapshot %s: %6.1f seconds\n", snap_id,
				time.Since(copyTime).Seconds())
			copied = true
		}
	}

	if !copied {
		Printf("Repositories are identical.\n")
	}
	return nil
}
