package main

// execute manage repository, check soundness

import (
	// system
	"context"
	"time"
	"strings"

	//argparse
	"github.com/spf13/cobra"
)

type ManageOptions struct {
	timing        bool
	db_manage     bool
	rollback      bool
	doit          bool
	dry_run       bool
	check         bool
	repo_manage   bool
	rebuild_index bool
	lost          bool
	config_file   string
	cutoff        int
	detail        int
}

var wpl_manage_opts_opts ManageOptions

var cmdWplManage = &cobra.Command{
	Use:   "wpl-manage [flags]",
	Short: "run a sep of restic commands from within the sub-command",
	Long: `run a set of restic commands from within the sub-command.
  the following functions are implemented:
  --repo-manage: remove old snapshots based on a cutoff date
  --db-manage updates the SQLite database inside the restic repository
  --check: run restic check and restic read-all
           which is a enhancement to restic check --read-data
	--rebuild-index: the usual 'restic --rebuild-index'
	flags:
  --dry-run: the usual simulation of commands
  --doit: used for 'repo-manage'
	--detail: shows the files to be removed from the repository
	--lost: show only files will be irreversibly removed from the repository

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
	DisableAutoGenTag: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		return RunWplManage(cmd.Context(), cmd, globalOptions, args)
	},
}

func init() {
	cmdRoot.AddCommand(cmdWplManage)
	f := cmdWplManage.Flags()
	f.BoolVarP(&wpl_manage_opts_opts.repo_manage, "repo-manage", "", false, "remove old snapshots based on --cutoff")
	f.BoolVarP(&wpl_manage_opts_opts.db_manage, "db-manage", "", false, "run the database manage step (add_record, rem_record, verify")
	f.BoolVarP(&wpl_manage_opts_opts.rollback, "rollback", "R", false, "ROLLBACK database operations")
	f.BoolVarP(&wpl_manage_opts_opts.doit, "doit", "", false, "execute command")
	f.BoolVarP(&wpl_manage_opts_opts.rebuild_index, "rebuild-index", "", false, "execute command restic rebuild-index")
	f.BoolVarP(&wpl_manage_opts_opts.check, "check", "", false, "check repository")
	f.BoolVarP(&wpl_manage_opts_opts.dry_run, "dry-run", "", false, "do not execute command, just simulate")
	f.BoolVarP(&wpl_manage_opts_opts.lost, "lost", "L", false, "print lost file details")
	f.StringVarP(&wpl_manage_opts_opts.config_file, "config-file", "", "", "config file for backup configuration")
	f.IntVarP(&wpl_manage_opts_opts.cutoff, "cutoff", "C", 270, "cutoff snaps which are older than <cutoff> days")
	f.CountVarP(&wpl_manage_opts_opts.detail, "detail", "D", "print dir/file details")
}

// the run restic backup command and other subcommands
func RunWplManage(ctx context.Context, cmd *cobra.Command, gopts GlobalOptions, args []string) error {

	var err error
	gOptions = gopts

	// set up types and slices for generic test loop
	type TestFunc func(context.Context, *cobra.Command, GlobalOptions) error
	type RunTests struct {
		test bool
		test_func TestFunc
	}
	var tests = []RunTests{
		{wpl_manage_opts_opts.repo_manage, step_manage},
		{wpl_manage_opts_opts.rebuild_index, run_rebuild_index},
		{wpl_manage_opts_opts.check, run_check},
	}

	for ix, tester := range tests {
		if tester.test {
			err = tester.test_func(ctx, cmd, gopts)
			if err != nil {
				Printf("tester %d returned '%v'\n", ix, err)
				return err
			}
		}
	}

	if wpl_manage_opts_opts.db_manage {
		var db_args []string
		Verbosef("\n%s sync database with repository %s\n", time.Now().String()[:19], gopts.Repo)
		err := runDBManage(ctx, cmd, gopts, db_args)
		if err != nil {
			return err
		}
	}
	return nil
}

type SnapTriple struct {
	Snap_time string
	Snap_host string
	Snap_fsys string
}

// check for old snapshots, remove and prune them
func step_manage(ctx context.Context, cmd *cobra.Command, gopts GlobalOptions) error {
	var empty_args []string

	// run tremove
	tremoveOptions.cutoff = wpl_manage_opts_opts.cutoff
	tremoveOptions.detail = wpl_manage_opts_opts.detail
	tremoveOptions.lost   = wpl_manage_opts_opts.lost
	dont_forget := false
	Printf("\n%s find snapshots old enough to be deleted.\n", time.Now().String()[:19])
	err := runTRemove(ctx, cmd, gopts, empty_args)
	if err != nil {
		if err.Error() == "No snapshots selected." {
			dont_forget = true
		} else {
			Printf("runTRemove returned '%v'\n", err)
			return err
		}
	}

	if !dont_forget {
		repo, err := OpenRepository(ctx, gopts)
		if err != nil {
			return err
		}
		Verboseff("Repository is %s\n", globalOptions.Repo)

		// extract snap_ids from repository which are old enough to be removed
		_, snap_map, err := GatherAllSnapshots(gopts, ctx, repo)
		if err != nil {
			Printf("GatherAllSnapshots failed with '%v'\n", err)
			return err
		}

		now := time.Now()
		remove_list := make([]string, 0)
		for snap_id, sn := range snap_map {
			year, month, day := sn.Time.Date()
			// diff in days
			diff := int(now.Sub(time.Date(
				year, month, day, 0, 0, 0, 0, time.UTC)).Hours() / 24.0)
			if diff >= wpl_manage_opts_opts.cutoff {
				remove_list = append(remove_list, snap_id)
			}
		}
		repo.Close()

		// run forget [snap_id,...] --prune --max-unused 0 --repack-small
		var opts ForgetOptions
		opts.Prune = true
		opts.DryRun = wpl_manage_opts_opts.dry_run
		pruneOptions.MaxUnused = "0"
		pruneOptions.DryRun = wpl_manage_opts_opts.dry_run
		pruneOptions.RepackSmall = true

		// run restic forget [...] --prune --max-unused 0 --repack-small [--dry-run]
		if wpl_manage_opts_opts.doit {
			the_list := strings.Join(remove_list, ", ")
			Printf("\n%s restic forget %s -r %s --prune --max-unused 0 --repack-small\n",
				time.Now().String()[:19], the_list, globalOptions.Repo)
			// call command in cmd_forget.go
			err = runForget(ctx, opts, gopts, remove_list)
			if err != nil {
				Printf("runForget returned '%v'\n", err)
				return err
			}
		}
	}
	return nil
}

// run restic rebuild-index
func run_rebuild_index(ctx context.Context, cmd *cobra.Command, gopts GlobalOptions) error {

	var opts RepairIndexOptions
	Printf("\n%s restic rebuild-index -r %s\n",
		time.Now().String()[:19], globalOptions.Repo)
	err := runRebuildIndex(ctx, opts, gopts)
	if err != nil {
		Printf("runRebuildIndex would not run - error is '%v'\n", err)
		return err
	}
	return nil
}

// run some consistency checks on the repository
// restic check
// restic read-all
func run_check(ctx context.Context, cmd *cobra.Command, gopts GlobalOptions) error {

	var my_args []string
	Printf("\n%s restic check -r %s\n", time.Now().String()[:19], globalOptions.Repo)
	// call command in cmd_check.go
	err := runCheck(ctx, checkOptions, gopts, my_args)
	if err != nil {
		Printf("runCheck would not run - error is '%v'\n", err)
		return err
	}

	// run restic read-all
	Printf("\n%s restic read-all -r %s\n", time.Now().String()[:19], globalOptions.Repo)
	// call command in wpl_read-allfiles.go
	err = runReadall(ctx, cmd, gopts)
	if err != nil {
		Printf("runReadall would not run - error is '%v'\n", err)
		return err
	}
	return nil
}
