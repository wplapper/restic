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
  Lost          bool
  config_file   string
  Cutoff        int
  Detail        int
}

var wpl_manage_opts_opts ManageOptions

var cmdWplManage = &cobra.Command{
  Use:   "wpl-manage [flags]",
  Short: "run a set of restic commands from within the sub-command",
  Long: `run a set of restic commands from within the sub-command.
  the following functions are implemented:
  --repo-manage: remove old snapshots based on a Cutoff date
  --check: run restic check and restic read-all
           which is a enhancement to restic check --read-data
  --rebuild-index: the usual 'restic repair index'
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
  f.BoolVarP(&wpl_manage_opts_opts.repo_manage, "repo-manage", "", false, "remove old snapshots based on --Cutoff")
  f.BoolVarP(&wpl_manage_opts_opts.rollback, "rollback", "R", false, "ROLLBACK database operations")
  f.BoolVarP(&wpl_manage_opts_opts.doit, "doit", "", false, "execute command")
  f.BoolVarP(&wpl_manage_opts_opts.rebuild_index, "rebuild-index", "", false, "execute command restic rebuild-index")
  f.BoolVarP(&wpl_manage_opts_opts.check, "check", "", false, "check repository")
  f.BoolVarP(&wpl_manage_opts_opts.dry_run, "dry-run", "N", false, "do not execute command, just simulate")
  f.BoolVarP(&wpl_manage_opts_opts.Lost, "lost", "L", false, "print Lost file details")
  f.StringVarP(&wpl_manage_opts_opts.config_file, "config-file", "", "", "config file for backup configuration")
  f.IntVarP(&wpl_manage_opts_opts.Cutoff, "cutoff", "C", 270, "Cutoff snaps which are older than <Cutoff> days")
  f.CountVarP(&wpl_manage_opts_opts.Detail, "detail", "D", "print dir/file details")
}

// the run restic backup command and other subcommands
func RunWplManage(ctx context.Context, cmd *cobra.Command, gopts GlobalOptions, args []string) error {

  var err error
  gOptions = gopts

  // set up types and slices for generic test loop
  type TestFunc func(context.Context, *cobra.Command, GlobalOptions) error
  type RunTests struct {
    test      bool
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
  tremoveOptions.Cutoff = wpl_manage_opts_opts.Cutoff
  tremoveOptions.Detail = wpl_manage_opts_opts.Detail
  tremoveOptions.Lost   = wpl_manage_opts_opts.Lost
  dont_forget := false
  Printf("\n%s find snapshots old enough to be deleted.\n", time.Now().Format(time.DateTime))
  err := runTRemove(ctx, gopts, empty_args)
  if err != nil {
    Printf("runTestRemove returned '%v'\n", err)
    return err
  }

  if !dont_forget {
    repo, err := OpenRepository(ctx, gopts)
    if err != nil {
      return err
    }
    Verboseff("Repository is %s\n", globalOptions.Repo)

    // extract snap_ids from repository which are old enough to be removed
    _, snap_map, err := GatherAllSnapshots(ctx, repo)
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
      if diff >= wpl_manage_opts_opts.Cutoff {
        remove_list = append(remove_list, snap_id[:8])
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
    if wpl_manage_opts_opts.doit && len(remove_list) > 0 {
      the_list := strings.Join(remove_list, ", ")
      Printf("\n%s restic forget %s -r %s --prune --max-unused 0 --repack-small\n",
        time.Now().Format(time.DateTime), the_list, globalOptions.Repo)
      // call command in cmd_forget.go
      /* cmd/restic/wpl_manage.go:166:38: not enough arguments in call to runForget
          have (context.Context, ForgetOptions, GlobalOptions, []string)
          want (context.Context, ForgetOptions, PruneOptions, GlobalOptions, *termstatus.Terminal, []string)
      */
      //err = runForget(ctx, opts, gopts, remove_list)
      term, cancel := setupTermstatus()
      defer cancel()
      err = runForget(ctx, opts, pruneOptions, gopts, term, remove_list)
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
    time.Now().Format(time.DateTime), globalOptions.Repo)
  //err := runRebuildIndex(ctx, opts, gopts)
  term, cancel := setupTermstatus()
  defer cancel()
  err := runRebuildIndex(ctx, opts, gopts, term)
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
  Printf("\n%s restic check -r %s\n", time.Now().Format(time.DateTime), globalOptions.Repo)
  // call command in cmd_check.go
  //err := runCheck(ctx, checkOptions, gopts, my_args)
  term, cancel := setupTermstatus()
  defer cancel()
  err := runCheck(ctx, checkOptions, gopts, my_args, term)
  if err != nil {
    Printf("runCheck would not run - error is '%v'\n", err)
    return err
  }

  // run restic read-all
  Printf("\n%s restic read-all -r %s\n", time.Now().Format(time.DateTime), globalOptions.Repo)
  // call command in wpl_read-allfiles.go
  err = runReadall(ctx, cmd, gopts)
  if err != nil {
    Printf("runReadall would not run - error is '%v'\n", err)
    return err
  }
  return nil
}
