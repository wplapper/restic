package repository_test

import (
	"context"
	"testing"

	"github.com/wplapper/restic/library/backend"
	"github.com/wplapper/restic/library/checker"
	"github.com/wplapper/restic/library/repository"
	"github.com/wplapper/restic/library/restic"
	rtest "github.com/wplapper/restic/library/test"
	"github.com/wplapper/restic/library/ui/progress"
)

func listIndex(t *testing.T, repo restic.Lister) restic.IDSet {
	return listFiles(t, repo, restic.IndexFile)
}

func testRebuildIndex(t *testing.T, readAllPacks bool, damage func(t *testing.T, repo *repository.Repository, be backend.Backend)) {
	repo, be := repository.TestRepositoryWithVersion(t, 0)
	createRandomBlobs(t, repo, 4, 0.5, true)
	createRandomBlobs(t, repo, 5, 0.5, true)
	indexes := listIndex(t, repo)
	t.Logf("old indexes %v", indexes)

	damage(t, repo, be)

	repo = repository.TestOpenBackend(t, be)
	rtest.OK(t, repository.RepairIndex(context.TODO(), repo, repository.RepairIndexOptions{
		ReadAllPacks: readAllPacks,
	}, &progress.NoopPrinter{}))

	checker.TestCheckRepo(t, repo, true)
}

func TestRebuildIndex(t *testing.T) {
	for _, test := range []struct {
		name   string
		damage func(t *testing.T, repo *repository.Repository, be backend.Backend)
	}{
		{
			"valid index",
			func(t *testing.T, repo *repository.Repository, be backend.Backend) {},
		},
		{
			"damaged index",
			func(t *testing.T, repo *repository.Repository, be backend.Backend) {
				index := listIndex(t, repo).List()[0]
				replaceFile(t, be, backend.Handle{Type: restic.IndexFile, Name: index.String()}, func(b []byte) []byte {
					b[0] ^= 0xff
					return b
				})
			},
		},
		{
			"missing index",
			func(t *testing.T, repo *repository.Repository, be backend.Backend) {
				index := listIndex(t, repo).List()[0]
				rtest.OK(t, be.Remove(context.TODO(), backend.Handle{Type: restic.IndexFile, Name: index.String()}))
			},
		},
		{
			"missing pack",
			func(t *testing.T, repo *repository.Repository, be backend.Backend) {
				pack := listPacks(t, repo).List()[0]
				rtest.OK(t, be.Remove(context.TODO(), backend.Handle{Type: restic.PackFile, Name: pack.String()}))
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			testRebuildIndex(t, false, test.damage)
			testRebuildIndex(t, true, test.damage)
		})
	}
}
