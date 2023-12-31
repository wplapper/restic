package main

import (
	// system
	"context"
	"encoding/binary"
	"os"
	"os/exec"

	//argparse
	"github.com/spf13/cobra"

	// restic library
	"github.com/wplapper/restic/library/restic"
)

var cmdCreateIXR = &cobra.Command{
	Use:   "wpl-create-IXR",
	Short: "create share index_repo file",
	Long: `create share index_repo file.

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
	DisableAutoGenTag: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runCreateIXR(cmd.Context(), globalOptions)
	},
}

func init() {
	cmdRoot.AddCommand(cmdCreateIXR)
}

func runCreateIXR(ctx context.Context, gopts GlobalOptions) error {

	// the repositories
	repositoryList := []string{
		"/media/mount-points/Backup-ext4-Mate/restic_data",
		"/media/mount-points/Backup-ext4-Mate/restic_home",
		"/media/mount-points/Backup-ext4-Mate/restic_massive",
		"/media/mount-points/Backup-ext4-Mate/restic_rasp_winxp",

		"rclone:onedrive:restic_data",
		"rclone:onedrive:restic_home",
		"rclone:onedrive:restic_massive",
		"rclone:onedrive:restic_rasp_winxp",
	}

	globalOptions.Quiet = true
	globalOptions.verbosity = 0
	allIDs := map[restic.ID]struct{}{}
	for _, repoName := range repositoryList {
		Printf("repo %s\n", repoName)
		if repoName[0:6] == "/media" {
			// execute usr/bin/ncat -z new-PC 22 -w 1s
			pingCmd := exec.Command("/usr/bin/ncat", "-z", "new-PC", "22", "-w", "1s")
			err := pingCmd.Run()
			if err != nil {
				Printf("Could not ping host new-PC on port 22\n")
				continue
			}
		}

		gopts.Repo = repoName
		repo, err := OpenRepository(ctx, gopts)
		if err != nil {
			Printf("Can't open repo %s\n", repoName)
			continue
		}

		if err := repo.LoadIndex(ctx); err != nil {
			Printf("repo.LoadIndex - failed. Error is %v\n", err)
			continue
		}

		// loop over all indices
		repo.Index().Each(ctx, func(blob restic.PackedBlob) {
			allIDs[blob.ID] = struct{}{}
		})

		// release resources
		repo.Close()
	}

	// write data to file /home/wplapper/restic/all_index_repo.bin
	path := "/home/wplapper/restic/all_packfiles.bin"
	file, err := os.Create(path)
	if err != nil {
		Printf("Could not os.Create file %s\n", path)
		return err
	}
	defer file.Close()

	err = os.Chown(path, 1000, 1000)
	if err != nil {
		Printf("Cant Chown database - reason '%v'\n", err)
		return err
	}

	for ID := range allIDs {
		binary.Write(file, binary.LittleEndian, ID)
	}
	file.Close()
	Printf("file %s written with %d records\n", path, len(allIDs))
	return nil
}
