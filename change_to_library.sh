#! /bin/bash
set -e

cd /home/wplapper/restic/restic
grep -Rl github.com/restic/restic/internal | while read filename
do
  echo "== ${filename} =="
  sed -i 's|restic/restic/internal/|wplapper/restic/library/|' ${filename}
done

exit 0

# clone github.com/restic/restic to github.com/wplapper/restic
# cd restic
# change internal to library
# change .git/config: restic/restic to wplapper/restic
# change go.mod: restic/restic to wplapper/restic

# execute go mod tidy
# git remote add origin https://github.com/wplapper/restic.git
# git branch -M main
# git push -u origin main
# git status / git add ... git rm -r /internal, ... git commit

# copy wpl*.go files to cmd/restic

# modify manually
# cmd/restic/global.go             -- for short repo
# library/backend/file.go          -- for Wplfile file type
# library/repository/repository.go -- for making repo.be available

# push to github
# git remote add origin https://github.com/wplapper/restic.git
# git branch -M main
# git push -u origin main
