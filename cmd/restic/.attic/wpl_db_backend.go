package main

import (
	// system
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"time"

	// restic library
	"github.com/wplapper/restic/library/cache"
	"github.com/wplapper/restic/library/restic"
	"github.com/wplapper/restic/library/sqlite"

	// zstd compressor and decompressor
	"github.com/klauspost/compress/zstd"
)

type TimeStampDatabase struct {
	Uncompressed_size  int    `json:"uncompressed_size"`
	Uncompressed_mtime string `json:"uncompressed_mtime"`
	Compressed_size    int    `json:"compressed_size"`
	Compressed_mtime   string `json:"compressed_mtime"`
}

// copy compressed backend SQLite database into uncompressed
// database in cache at wpl/restic.db - if a copy is needed
func database_via_cache(repo restic.Repository, ctx context.Context) (string, error) {
	// this database will always be accessed through the backend

	// target file in cache
	var cache_dir string
	var err error
	config := repo.Config()
	cache_dir, err = cache.DefaultDir()
	subdir_name := cache_dir + "/" + config.ID + "/wpl"
	// local database in cache, uncompressed
	db_name := subdir_name + "/restic.db"

	cstat, err := os.Stat(db_name)
	if err != nil {
		err2 := os.MkdirAll(subdir_name, 0700)
		if err2 != nil && !os.IsExist(err2) {
			Printf("Fatal err %v\n", err2)
			panic("Can't create subdirectory wpl")
		}

		// we dont have a file in the cache with this name, create one
		_, cerr := os.Create(db_name)
		if cerr != nil {
			Printf("Can't create file %s in cache - error is %v\n", db_name, cerr)
			panic("Can't create cache wpl/restic.db file")
		}
		cstat, _ = os.Stat(db_name)
	}

	// get timestamp file from Backend(), contains size & mtime of
	// last uncompressed database
	handle_ts := restic.Handle{Type: restic.WplFile, Name: "wpl/timestamp-db"}
	be_stat, err := repo.Backend().Stat(ctx, handle_ts)

	update_timestamp_file := false
	if err == nil {
		// Load() timestamp file from Backend()
		wr := new(bytes.Buffer)
		err = repo.Backend().Load(ctx, handle_ts, 0, 0, func(rd io.Reader) error {
			// and copy to buffer
			_, cerr := io.Copy(wr, rd) // wr <- rd
			if cerr != nil {
				Printf("database_timestamp: copy from backend: cerr=%v\n", cerr)
				return cerr
			}
			return nil
		})

		if err != nil {
			Printf("Could not read timestamp file '%v'\n", err)
			return "", err
		}

		var database_timestamp TimeStampDatabase
		// convert json data to 'database_timestamp' struct
		err = json.Unmarshal(wr.Bytes(), &database_timestamp)
		if err != nil {
			Printf("could not load 'database_timestamp' from backend - error is %v", err)
			return "", err
		}

		// we have a timestamp file in our hands
		if int(cstat.Size()) == database_timestamp.Uncompressed_size &&
			cstat.ModTime().String()[:19] == database_timestamp.Uncompressed_mtime {
			// dont need to load database from backend
			return db_name, nil
		}
	}

	Verboseff("Need to read compressed database file from Backend\n")
	update_timestamp_file = true
	handle_cmp_db := restic.Handle{Type: restic.WplFile, Name: "wpl/restic.zstd"}
	be_stat, err = repo.Backend().Stat(ctx, handle_cmp_db)
	if err == nil {
		//Printf("size restic.zstd is %d bytes\n", be_stat.Size)
		wr := new(bytes.Buffer)
		err = repo.Backend().Load(ctx, handle_cmp_db, 0, 0, func(rd io.Reader) error {

			_, cerr := io.Copy(wr, rd) // wr <- rd
			if cerr != nil {
				Printf("database_via_cache: copy from backend: cerr=%v\n", cerr)
				return cerr
			}
			return nil
		})

		if err != nil {
			Printf("Can't load database from backend - error is %v\n", err)
			return "", err
		}

		// here we execute zstd -d <backend-file restid.zstd> -c | sqlite3 <restic-cache/wpl/restic.db>
		decoder, err := zstd.NewReader(wr)
		if err != nil {
			Printf("zstd.NewReader failed, error is %v\n", err)
			return "", err
		}
		defer decoder.Close()

		start := time.Now()
		Verboseff("db_name is %s\n", db_name)
		os.Remove(db_name)
		cmd := exec.Command("/usr/bin/sqlite3", db_name)
		stdin, err := cmd.StdinPipe()
		if err != nil {
			Printf("os.Exec.StdinPipe failed with %v\n", err)
			return "", err
		}

		go func() {
			defer stdin.Close()
			_, err = decoder.WriteTo(stdin)
		}()


		// connect output of decoder to stdin
		if err != nil {
			Printf("decoder.WriteTo error is %v\n", err)
			return "", err
		}

		// calling CombinedOutput is essential, because it implies a cmd.Wait()
		// otherwise there will ne NO waitung!!
		_, err = cmd.CombinedOutput()
		if err != nil {
			Printf("CombinedOutput error is %v\n", err)
			return "", err
		}

		diff := time.Now().Sub(start).Seconds()
		Verboseff("filling database in %.1f seconds.\n", diff)

		// want to VACUUM database after being restored from compressed .dump file
		db_conn, err := sqlite.OpenDatabase(db_name, false, 0, false)
		if err != nil {
			Printf("db_backend.Open().Vacuum(): OpenDatabase failed, error is %v\n", err)
			return "", err
		}
		Verboseff("VACUUM\n")
		db_conn.MustExec("VACUUM")

		// uncommitted READ for timestamp TABLE
		var database_updated time.Time
		sqll := "SELECT database_updated FROM timestamp WHERE id = 1"
		err = db_conn.Get(&database_updated, sqll)
		if err != nil {
			Printf("Can't read timestamp TABLE, error is %+v. Ignored\n", err)
		}
		Verboseff("timestamp.database_updated at %s\n",
			database_updated.Format("2006-01-02 15:04:05"))
		db_conn.Close()

		cstat, _ = os.Stat(db_name)
		if update_timestamp_file {
			// write back timestamp file with details about compressed database file
			ts_db := TimeStampDatabase{
				Uncompressed_size:  int(cstat.Size()),
				Uncompressed_mtime: cstat.ModTime().String()[:19],
				Compressed_size:    int(be_stat.Size),
				Compressed_mtime:   time.Now().String()[:19],
			}

			// rewrite backend stats file
			jsonString, err := json.Marshal(ts_db)
			if err != nil {
				Printf("Could not create JSON string for timestamp - error is %v\n", err)
				return db_name, err
			}

			target := "wpl/timestamp-db"
			handle := restic.Handle{Type: restic.WplFile, Name: target}
			err = repo.Backend().Save(ctx, handle, restic.NewByteReader(jsonString, nil))
			if err != nil {
				Printf("repo.Backend().Save(timestamp) failed with %v\n", err)
				return db_name, err
			}
			Verboseff("timestamp %s written\n", target)
		}
	} else {
		Printf("Compressed database file not found in backend, create new.\n")
	}
	return db_name, nil
}

// write cached database file back to backend
func write_back_database(db_name string, repo restic.Repository, ctx context.Context) error {
	// read database into buffer
	//Printf("called write_back_database\n")
	db_cache_info, err := os.Stat(db_name)
	if err != nil {
		Printf("Could not Stat() database from cache - error is '%v'\n", err)
		return err
	}
	//buf := make([]byte, 128 * 1024)
	// CompressEncoder allocates a large buffer so the results can be gathered below
	// buffer_writer := bytes.NewBuffer(make([]byte, 0, 1024 * 1024 * 1024))
	// dst := buffer_writer.Bytes()
	buffer_writer, cmp_handle, err := CompressEncoder()
	if err != nil {
		Printf("CompressEncoder failed with %v\n", err)
		return err
	}

	cmd := exec.Command("/usr/bin/sqlite3", db_name, ".dump")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		Printf("Fatal error creating StdoutPipe %v\n", err)
		return err
	}
	defer stdout.Close()

	Verboseff("run /usr/bin/sqlite3 %s .dump\n", db_name)
	if err := cmd.Start(); err != nil {
		Printf("Can't start slqite3 .dump")
		return err
	}

	// here we execute "sqlite3 <db_name> .dump | zstd -8 - > <backend-file/wpl/restic.zstd>"
	go cmp_handle.ReadFrom(stdout)
	if err := cmd.Wait(); err != nil {
		Printf("Wait failed with error %v\n", err)
		return err
	}

	err = cmp_handle.Close()
	if err != nil {
		Printf("CompressEncoder.Close() failed with %v\n", err)
		return err
	}

	// compressed data is stored in 'buffer_writer'
	dst := buffer_writer.Bytes()
	ts_db := TimeStampDatabase{
		Uncompressed_size:  int(db_cache_info.Size()),
		Uncompressed_mtime: db_cache_info.ModTime().String()[:19],
		Compressed_size:    len(dst),
		Compressed_mtime:   time.Now().String()[:19],
	}

	// write json stats file
	jsonString, err := json.Marshal(ts_db)
	if err != nil {
		Printf("Could not create JSON string for timestamp - error is %v\n", err)
		return err
	}

	backend_name := "wpl/timestamp-db"
	handle := restic.Handle{Type: restic.WplFile, Name: backend_name}
	err = repo.Backend().Save(ctx, handle, restic.NewByteReader(jsonString, nil))
	if err != nil {
		Printf("repo.Backend().Save(timestamp) failed with %v\n", err)
		return err
	}
	Verboseff("timestamp file %s written.\n", backend_name)

	t1 := time.Now()
	backend_name = "wpl/restic.zstd"
	handle = restic.Handle{Type: restic.WplFile, Name: backend_name}
	err = repo.Backend().Save(ctx, handle, restic.NewByteReader(dst, nil))
	if err != nil {
		Printf("repo.Backend().Save(cmp_db) failed with %v\n", err)
		return err
	}

	Verboseff("compressed database written back to %s, size is %d byes.\n",
		backend_name, len(dst))
	t2 := time.Now().Sub(t1).Seconds()
	if t2 > 0.0 {
		speed := float64(len(dst)) / ONE_MEG / t2
		Verboseff("time diff is %.1f seconds for %.1f MiB with speed %.1f MiB/s\n",
			t2, float64(len(dst))/ONE_MEG, speed)
	}
	return nil
}

// generic compressor
func CompressEncoder() (*bytes.Buffer, *zstd.Encoder, error) {
	opts := []zstd.EOption{
		// Set the compression to normal, CRC-check to true and the and good
		// lookbehind of 2 MiB
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderCRC(true),
		zstd.WithWindowSize(2 * 1024 * 1024),
	}

	// we need a buffer to compress into
	buffer_writer := bytes.NewBuffer(make([]byte, 0, 1024 * 1024 * 1024))
	enc, err := zstd.NewWriter(buffer_writer, opts...)
	if err != nil {
		return nil, nil, err
	}
	return buffer_writer, enc, err
}

// generic decompressor
func DecompressDecoder() (*zstd.Decoder, error) {
	opts := []zstd.DOption{
		// Use all available cores.
		zstd.WithDecoderConcurrency(0),
		// Limit the maximum decompressed memory. Set to a very high,
		// conservative value.
		zstd.WithDecoderMaxMemory(16 * 1024 * 1024 * 1024),
	}

	decoder, err := zstd.NewReader(nil, opts...)
	if err != nil {
		Printf("DecompressEncoder refuses to work - reason '%v'\n", err)
		return nil, err
	}
	return decoder, nil
}
