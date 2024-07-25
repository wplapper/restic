
package main

// run backup on host for selected filesystem.
// also execute mange repository, check soundness
// and compare two repositories for identical snap content.
// run 'restic' as an external command!

import (
  // system
  "context"
  "encoding/json"
  "errors"
  "os"
  "os/exec"
  "time"
  "strings"

  //argparse
  "github.com/spf13/cobra"
)

type BackupFilesystemOptions struct {
  dry_run       bool
  config_file   string
}

type StringSlice []string

var backup_filesys_opts BackupFilesystemOptions
var hostname, _ = os.Hostname()

var cmdResticBackup = &cobra.Command{
  Use:   "wpl-backup [flags]",
  Short: "run the restic backup command_args from within this command_args",
  Long: `run a sep run the restic backup command_args from within this commandof restic commands from within the sub-command_args.
  the following functions are implemented:
  run a backup for the filesystems defined in the json file --config-file

EXIT STATUS
===========

Exit status is 0 if the command_args was successful, and non-zero if there was any error.
`,
  DisableAutoGenTag: false,
  RunE: func(cmd *cobra.Command, args []string) error {
    return RunResticBackup(cmd.Context(), globalOptions)
  },
}

func init() {
  cmdRoot.AddCommand(cmdResticBackup)
  f := cmdResticBackup.Flags()
  f.StringVarP(&backup_filesys_opts.config_file, "config-file", "", "", "config file for backup configuration")
  f.BoolVarP(&backup_filesys_opts.dry_run, "dry-run", "", false, "do not execute command_args, just simulate")
}

// run restic backup command_args
func RunResticBackup(ctx context.Context, gopts GlobalOptions) error {

  err := step_backup(ctx, gopts)
  if err != nil {
    Printf("step_backup returned '%v'\n", err)
    return err
  }
  return nil
}

// backup the filesystems defined in the configuration file for this host
func step_backup(ctx context.Context, gopts GlobalOptions) error {
  var (
    err         error
    json_config map[string]map[string]StringSlice
    ok          bool
  )
  monthly := map[string]bool{"MagPi": true, "WinXP": true}

  // read json from file
  json_string, err := os.ReadFile(backup_filesys_opts.config_file)
  if err != nil {
    Printf("Cant read backup config file %v\n", err)
    return err
  }

  err = json.Unmarshal(json_string, &json_config)
  if err != nil {
    Printf("Cant decode config file %v\n", err)
    return err
  }

  //we need access to json_config["BACKUP_FILESYSTEMS"]
  backup_fsys := json_config["BACKUP_FILESYSTEMS"]
  filesystem_to_backup, ok := backup_fsys[hostname]
  if ! ok {
    Printf("Cant find config for host '%s'\n", hostname)
    return errors.New("backup: host not found")
  }

  _, _, day := time.Now().Date()
  for _, logical_backup := range filesystem_to_backup {
    if _, ok := monthly[logical_backup]; ok && day > 7 {
      continue
    }

    Printf("\n%s *** backing up file system '%s' ***\n",
      time.Now().Format(time.DateTime), logical_backup)
    backup_excl := json_config["BACKUP_EXCLUDES"]
    backup_basedirs := json_config["BACKUP_BASEDIRS"]
    if _, ok = backup_basedirs[logical_backup]; ! ok {
      Printf("No reference for %s found in BACKUP_BASEDIRS\n", logical_backup)
      panic("no reference to BACKUP_BASEDIRS")
    }


    repo_basepath := json_config["BASE_CONFIG"]["Mint21-nvme"]
    target_repo   := json_config["PATH_TO_REPO"][logical_backup]
    var command_args = []string{"backup", backup_basedirs[logical_backup][0],
     "--one-file-system", "--verbose",
     "--repo", repo_basepath[0] + target_repo[0],
      "--password-file", "/home/wplapper/backup_password2",
    }

    backup_excl = json_config["BACKUP_EXCLUDES"]
    if len(backup_excl[logical_backup]) > 0 {
      command_args = append(command_args, "--exclude-file", backup_excl[logical_backup][0])
    }
    if backup_filesys_opts.dry_run {
      command_args = append(command_args, "--dry-run")
    }

    Printf("%s %s %s\n", time.Now().Format(time.DateTime), "spec_restic",
      strings.Join(command_args, " "))

    start := time.Now()
    cmd := exec.Command("/home/wplapper/restic/restic/spec_restic", command_args ...)
    stdoutStderr, err := cmd.CombinedOutput()
    if err != nil {
      Printf("Backup %s does not want to run - error is '%v'\n",
        logical_backup, err)
      return err
    }
    Printf("%s\n", stdoutStderr)
    Printf("backup took %.1f seconds to run\n", time.Since(start).Seconds())
  }
  return nil
}
