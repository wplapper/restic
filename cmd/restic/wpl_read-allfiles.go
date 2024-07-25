
package main

// read all data packfiles to check for errors

import (
  // system
  "bytes"
  "context"
  "encoding/json"
  "errors"
  "io"
  "time"

  //argparse
  "github.com/spf13/cobra"

  // restic library
  "github.com/wplapper/restic/library/checker"
  "github.com/wplapper/restic/library/repository"
  "github.com/wplapper/restic/library/repository/pack"
  "github.com/wplapper/restic/library/restic"
  "github.com/wplapper/restic/library/backend"
)

type readall_options struct {
  one_hour     bool
  check_packs  int
  amt_to_check int // in MiB
  reset_count  int
}

type CheckedPack struct {
  Pack_ID string
  Checked bool
}

var ReadallOptions readall_options

const UPPER_LIMIT int = 999999999

var cmdReadall = &cobra.Command{
  Use:   "read-all [flags]",
  Short: "wpl read all data files (extended version of the 'check' command)",
  Long: `wpl read all data files (extended version of the 'check' command).
Read all data packfiles in order to find errors in the packs.

OPTIONS
=======
* number of packfiles and blocksize for one round of checks can be chosen.
* reset-packs <n> reset <n> packs randomly to "non-checked" status
* one-hour: run for one hour and then stop

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
  DisableAutoGenTag: false,
  RunE: func(cmd *cobra.Command, args []string) error {
    return runReadall(cmd.Context(), cmd, globalOptions)
  },
}

func init() {
  cmdRoot.AddCommand(cmdReadall)
  f := cmdReadall.Flags()
  f.BoolVarP(&ReadallOptions.one_hour, "one-hour", "O", false, "run for about an hour")
  f.IntVarP(&ReadallOptions.check_packs, "check-packs", "C", 1000000, "check next <next> packfiles")
  f.IntVarP(&ReadallOptions.reset_count, "reset-count", "R", 0, "reset <reset-count> blobs as not checked")
  f.IntVarP(&ReadallOptions.amt_to_check, "amount", "A", 1024, "check size for one round of checks in MiB")
}

// extended version of the restic check command - for the --read-data option
func runReadall(ctx context.Context, cmd *cobra.Command, gopts GlobalOptions) error {

  // step 1: open repository
  repo, err := OpenRepository(ctx, gopts)
  if err != nil {
    Printf("Could not open repository - error is %v\n", err)
    return err
  }
  Printf("Repository is %s\n", globalOptions.Repo)

  // step 2: load the index files
  if err := repo.LoadIndex(ctx, nil); err != nil {
    Printf("repo.LoadIndex - failed. Error is %v\n", err)
    return err
  }

  // step 3: extract packfiles with their sizes from the Master Index
  repo_packs, err := pack.Size(ctx, repo, false)
  if err != nil {
    Printf("pack.Size failed. Error is %v\n", err)
    return err
  }

  pack_size := int64(0)
  for _, size := range repo_packs {
    pack_size += size
  }
  Printf("%7d packfiles with a size of %10.1f MiB\n", len(repo_packs),
    float64(pack_size)/ONE_MEG)

  // step 4: get the stats file loaded into memory
  handle := backend.Handle{Type: backend.WplFile, Name: "wpl/packfiles.stat"}
  checked_packs, err := ReadStatsFile(*repo, ctx, handle)
  if err != nil {
    return err
  }

  err = do_check(repo, ctx, ReadallOptions, checked_packs, repo_packs)
  if err != nil {
    return err
  }
  //Printf("No errors found\n")

  count_unchecked := 0
  for _, checked := range checked_packs {
    if !checked {
      count_unchecked++
    }
  }
  Printf("%7d unchecked packfiles remaining.\n", count_unchecked)

  // write stats file
  jsonString := PrettyStruct(checked_packs)
  err = repo.Backend().Save(ctx, handle, backend.NewByteReader(jsonString, nil))
  if err != nil {
    Printf("repo.Backend().Save failed with %v\n", err)
    return err
  }
  return nil
}

// process the read-all command
func do_check(repo *repository.Repository, ctx context.Context,
  ReadallOptions readall_options, checked_packs map[string]bool,
  repo_packs map[restic.ID]int64) error {

  type custom_sort_ID struct {
    ID     restic.ID
    ID_str string
  }

  // remove cruft from stats file
  for ID_str := range checked_packs {
    ID, err := restic.ParseID(ID_str)
    if _, ok := repo_packs[ID]; !ok || err != nil {
      delete(checked_packs, ID_str)
    }
  }

  one_hour := ReadallOptions.one_hour
  action_size := int64(ReadallOptions.amt_to_check * 1024 * 1024)
  packs_to_process := ReadallOptions.check_packs
  if one_hour {
    packs_to_process = UPPER_LIMIT
  }

  // optional reset of already checked packs
  count_unchecked := 0
  reset_count := ReadallOptions.reset_count
  count := 0 // for resetting
  size_unchecked := int64(0)
  for ID, length := range repo_packs {
    ID_str := ID.String()
    if _, ok := checked_packs[ID_str]; !ok || count < reset_count {
      checked_packs[ID_str] = false
      size_unchecked += length
      count++
    }

    if !checked_packs[ID_str] {
      count_unchecked++
    }
  }
  Printf("checked_packs has %7d entries.\n", len(checked_packs))
  Printf("unchecked packs   %7d entries with size %10.1f MiB\n", count_unchecked,
    float64(size_unchecked)/ONE_MEG)

  // get a checker instance
  chkr := checker.New(repo, false)
  chkr.LoadIndex(ctx, nil)

  // prepare checker function with error callback
  errorsFound := false
  doReadData := func(packs map[restic.ID]int64) {
    errChan := make(chan error)

    go chkr.ReadPacks(ctx, packs, nil, errChan)

    // print out errors
    for err := range errChan {
      errorsFound = true
      Printf("ReadAll: %v\n", err)
    }
  }

  // now we are ready to check data blobs
  loop_start := time.Now()
  var checked_size int64 = 0

  count_checked_packs := 0
  total_size := int64(0)
  Printf("%-19s %-6s %10s %9s %13s\n", "time", "#packs", "size [MiB]", "delta [s]", "speed [MiB/s]")

  // outer loop
  handle := backend.Handle{Type: backend.WplFile, Name: "wpl/packfiles.stat"}
  for count_checked_packs < packs_to_process {
    selected_packs := make(map[restic.ID]int64)
    now := time.Now()
    if one_hour && now.Sub(loop_start).Seconds() > 3600.0 {
      break
    }

    // inner loop
    checked_size = int64(0)
    for ID, size := range repo_packs {
      if checked_packs[ID.String()] {
        continue
      }

      selected_packs[ID] = size
      checked_size += size
      total_size += size
      if checked_size > action_size || len(selected_packs)+count_checked_packs >= packs_to_process {
        break
      }
    } // end inner loop

    if len(selected_packs) == 0 {
      break
    }

    // do the check and read all the data blobs
    inner_time := time.Now()
    doReadData(selected_packs)
    t_end := time.Now()
    diff_time := t_end.Sub(inner_time).Seconds()

    Printf("%s %6d %10.1f %9.1f %13.1f\n", t_end.String()[:19], len(selected_packs),
      float64(checked_size)/ONE_MEG, diff_time,
      float64(checked_size)/ONE_MEG/diff_time)

    if errorsFound {
      return errors.New("repository contains errors")
    }

    // set checked status for packfiles just processed
    for ID := range selected_packs {
      checked_packs[ID.String()] = true
    }
    count_checked_packs += len(selected_packs)

    // whenever a block of packfiles has been done, write a checkpoint
    jsonString := PrettyStruct(checked_packs)
    err := repo.Backend().Save(ctx, handle, backend.NewByteReader(jsonString, nil))
    if err != nil {
      Printf("repo.Backend().Save failed with %v\n", err)
      return err
    }
  } // end outer loop

  // final display record
  t_end := time.Now()
  diff_time := t_end.Sub(loop_start).Seconds()
  Printf("=============================================================\n")
  Printf("%s %6d %10.1f %9.1f %13.1f\n", t_end.String()[:19], count_checked_packs,
    float64(total_size)/ONE_MEG, diff_time,
    float64(total_size)/ONE_MEG/diff_time)
  return nil
}

func ReadStatsFile(repo repository.Repository, ctx context.Context,
  handle backend.Handle) (map[string]bool, error) {
  // read stat file data from subdirectory 'wpl' below the root
  checked_packs := make(map[string]bool) // contains status of packfile check
  wr := new(bytes.Buffer)
  _, err := repo.Backend().Stat(ctx, handle)
  if err == nil { // file exists
    // implicit open, read into 'rd'
    err = repo.Backend().Load(ctx, handle, 0, 0, func(rd io.Reader) error {
      wr.Reset()
      _, cerr := io.Copy(wr, rd) // wr <- rd
      if cerr != nil {
        return cerr
      }
      return nil
    })
    if err != nil {
      Printf("repo.Backend().Load stats file returned %v\n", err)
      return nil, err
    }
    err = json.Unmarshal(wr.Bytes(), &checked_packs)
    if err != nil {
      Printf("could not load 'checked_packs' for repo - error is %v", err)
      return nil, err
    }
  }
  wr = nil
  return checked_packs, nil
}

func PrettyStruct(data interface{}) []byte {
    val, err := json.MarshalIndent(data, "", " ")
    if err != nil {
        return []byte{}
    }
    val = append(val, '\n')
    return val
}
