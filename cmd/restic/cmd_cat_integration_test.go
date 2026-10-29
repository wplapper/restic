package main

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/restic/restic/internal/global"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
	"github.com/restic/restic/internal/ui/progress"
)

func testRunCatMayFail(t testing.TB, gopts global.Options, args []string) ([]byte, error) {
	t.Helper()
	buf, err := withCaptureStdout(t, gopts, func(ctx context.Context, gopts global.Options) error {
		return runCat(ctx, gopts, args, gopts.Term)
	})

	return buf.Bytes(), err
}

func TestCatMasterkey(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9")}, BackupOptions{}, env.gopts)
	testListSnapshots(t, env.gopts, 1)

	stdout, err := testRunCatMayFail(t, env.gopts, []string{"masterkey"})
	rtest.OK(t, err)
	rtest.Assert(t, strings.Contains(string(stdout), "encrypt"), "expected encrypt record")
}

func TestCatConfig(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9")}, BackupOptions{}, env.gopts)
	testListSnapshots(t, env.gopts, 1)

	stdout, err := testRunCatMayFail(t, env.gopts, []string{"config"})
	rtest.OK(t, err)

	configUnmarshal := &restic.Config{}
	rtest.OK(t, json.Unmarshal(stdout, configUnmarshal))

	var config restic.Config
	err = withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		printer := progress.NewTerminalPrinter(gopts.JSON, gopts.Verbosity, gopts.Term)
		_, repo, unlock, err := openWithReadLock(ctx, gopts, false, printer)
		rtest.OK(t, err)
		defer unlock()

		config = repo.Config()
		return err
	})
	rtest.OK(t, err)

	rtest.Assert(t, configUnmarshal.ID == config.ID,
		"expected config IDs to be equal, but %v != %v", configUnmarshal.ID, config.ID)
}

func TestCatWrongType(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9")}, BackupOptions{}, env.gopts)
	testListSnapshots(t, env.gopts, 1)

	_, err := testRunCatMayFail(t, env.gopts, []string{"quack"})
	rtest.Assert(t, err != nil && strings.Contains(err.Error(), "invalid type"),
		"expected an 'invalid type' error, got %v", err)
}

type indexPack struct {
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

func TestCatIndex(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9")}, BackupOptions{}, env.gopts)
	testListSnapshots(t, env.gopts, 1)

	indexIDs := testRunList(t, env.gopts, "index")
	rtest.Assert(t, len(indexIDs) > 0, "expected at least one index file, got none")

	stdout, err := testRunCatMayFail(t, env.gopts, []string{"index", indexIDs[0].String()})
	rtest.OK(t, err)

	// unmarshal "cat index" output into above structs
	indexInfo := &indexPack{}
	rtest.OK(t, json.Unmarshal(stdout, indexInfo))

	// convert IDs into a blobSet
	blobHandles := restic.NewBlobSet()
	for _, pack := range indexInfo.Packs {
		for _, item := range pack.Blobs {
			handle := restic.BlobHandle{ID: item.ID, Type: item.Type}
			blobHandles.Insert(handle)
		}
	}
	rtest.Assert(t, len(blobHandles) > 0, "expected a few blobs in the repo")

	err = withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		printer := progress.NewTerminalPrinter(gopts.JSON, gopts.Verbosity, gopts.Term)
		ctx, repo, unlock, err := openWithReadLock(ctx, gopts, false, printer)
		rtest.OK(t, err)
		defer unlock()

		// don't forget to load the master index!'
		rtest.OK(t, repo.LoadIndex(ctx, printer))

		for handle := range blobHandles {
			_, found := repo.LookupBlobSize(handle)
			rtest.Assert(t, found, "expected to find blob in index, but did not find %v", handle)
		}

		return nil
	})

	rtest.OK(t, err)
}

func TestCatSnapshot(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9")}, BackupOptions{}, env.gopts)
	snapIDs := testListSnapshots(t, env.gopts, 1)

	_, err := testRunCatMayFail(t, env.gopts, []string{"snapshot", snapIDs[0].String()})
	rtest.OK(t, err)

	_, err = testRunCatMayFail(t, env.gopts, []string{"snapshot", "caffee01"})
	rtest.Assert(t, err != nil && strings.Contains(err.Error(), "could not find snapshot"),
		"expected no snapshot, but got %v", err)
}

func TestCatIDs(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9")}, BackupOptions{}, env.gopts)
	testListSnapshots(t, env.gopts, 1)

	tests := []struct {
		action  string
		ID      string
		wantErr string // substring of expected error, "" for no error
	}{
		{
			action: "pack",
		},
		{
			action:  "pack",
			ID:      "0076fb13c7b340e8455aca6620d50592b0088858c8f18b8b4cc7fbc8abbbaf00",
			wantErr: "no such file or directory",
		},
		{
			action:  "pack",
			ID:      "0076fb13c7b340e8455aca6620d50592b0088858c8f18b8b4cc7fbc8abbbafhh",
			wantErr: "unable to parse ID",
		},
		{
			action: "blob",
		},
		{
			action:  "blob",
			ID:      "0076fb13c7b340e8455aca6620d50592b0088858c8f18b8b4cc7fbc8abbbaf00",
			wantErr: "blob not found",
		},
		{
			action: "key",
		},
		{
			action:  "key",
			ID:      "0076fb13c7b340e8455aca6620d50592b0088858c8f18b8b4cc7fbc8abbbaf00",
			wantErr: "no such file or directory",
		},
		{
			action: "lock",
		},
		{
			action:  "snapshot",
			ID:      "0076fb13c7b340e8455aca6620d50592b0088858c8f18b8b4cc7fbc8abbbaf00",
			wantErr: "could not find snapshot",
		},
	}

	err := withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		printer := progress.NewTerminalPrinter(gopts.JSON, gopts.Verbosity, gopts.Term)
		_, repo, unlock, err := openWithReadLock(ctx, gopts, false, printer)
		rtest.OK(t, err)
		defer unlock()

		// don't forget to load the master index!'
		rtest.OK(t, repo.LoadIndex(ctx, printer))

		gopts.BackendTestHook = nil
		for _, tc := range tests {
			name := fmt.Sprintf("cat-%s", tc.action)
			if tc.ID != "" {
				name = fmt.Sprintf("cat-%s-%s", tc.action, tc.ID)
			}
			t.Run(name, func(t *testing.T) {
				IDs := testRunList(t, gopts, tc.action+"s")
				if len(IDs) == 0 && tc.action == "lock" {
					return
				}
				rtest.Assert(t, len(IDs) > 0,
					"expected at least one ID for 'list %s', got none", tc.action)

				var err error
				expectedError := tc.wantErr
				if expectedError == "no such file or directory" && runtime.GOOS == "windows" {
					expectedError = "The system cannot find the file specified."
				}
				if tc.ID == "" {
					_, err = testRunCatMayFail(t, gopts, []string{tc.action, IDs[0].String()})
				} else {
					_, err = testRunCatMayFail(t, gopts, []string{tc.action, tc.ID})
				}
				if expectedError == "" {
					rtest.OK(t, err)
				} else {
					rtest.Assert(t, strings.Contains(err.Error(), expectedError),
						"unexpected error: %v", err)
				}
			})
		}
		return nil
	})

	rtest.OK(t, err)
}

func TestCatTree(t *testing.T) {
	if runtime.GOOS == "windows" {
		return
	}

	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9")}, BackupOptions{}, env.gopts)
	snapID := testListSnapshots(t, env.gopts, 1)[0]

	out, err := testRunCatMayFail(t, env.gopts, []string{"tree", fmt.Sprintf("%s:%s/0/0", snapID, env.testdata)})
	rtest.OK(t, err)
	rtest.Assert(t, strings.Contains(string(out), `"name":"9","type":"dir"`),
		`expected "name":"9","type":"dir" got %v`, string(out))

	_, err = testRunCatMayFail(t, env.gopts, []string{"tree",
		fmt.Sprintf("%s:%s/0/0", "0076fb13c7b340e8455aca6620d50592b0088858c8f18b8b4cc7fbc8abbbaf00", env.testdata)})
	rtest.Assert(t, err != nil && strings.Contains(err.Error(), "could not find snapshot"),
		"expected 'could not find snapshot', but found %v", err)
	//could not find snapshot
}

func TestCatBlob(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "for_cmd_ls")}, BackupOptions{}, env.gopts)
	testListSnapshots(t, env.gopts, 1)

	err := withTermStatus(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		printer := progress.NewTerminalPrinter(gopts.JSON, gopts.Verbosity, gopts.Term)
		_, repo, unlock, err := openWithReadLock(ctx, gopts, false, printer)
		rtest.OK(t, err)
		defer unlock()

		// don't forget to load the master index!'
		rtest.OK(t, repo.LoadIndex(ctx, printer))

		var dataBlob, treeBlob restic.ID
		rtest.OK(t, repo.ListBlobs(ctx, func(pb restic.PackBlob) {
			switch pb.Handle().Type {
			case restic.TreeBlob:
				treeBlob = pb.Handle().ID
			case restic.DataBlob:
				dataBlob = pb.Handle().ID
			}
		}))

		out, err := testRunCatMayFail(t, env.gopts, []string{"blob", treeBlob.String()})
		rtest.OK(t, err)
		rtest.Assert(t, strings.Contains(string(out), `"nodes":[{"name"`),
			`expected "nodes":[{"name" got %v`, string(out))

		_, err = testRunCatMayFail(t, env.gopts, []string{"blob", dataBlob.String()})
		rtest.OK(t, err)

		return nil
	})
	rtest.OK(t, err)
}
