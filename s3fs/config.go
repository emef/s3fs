package s3fs

import (
	"os"
	"strconv"
	"strings"

	gcfg "gopkg.in/gcfg.v1"
)

func WithConfigFile(configPath string) Option {
	return func(s3fs *S3FS) error {

		cfg := struct {
			S3 struct {
				Bucket             string
				Region             string
				Workers            int
				MaxConcurrentReads int
			}

			FileSystem struct {
				BaseDir       string
				PartFileBytes string
			}
		}{}

		err := gcfg.ReadFileInto(&cfg, configPath)
		if err != nil {
			return err
		}

		homeDir := os.Getenv("HOME")
		baseDir := strings.Replace(cfg.FileSystem.BaseDir, "$HOME", homeDir, 1)
		if err := WithBaseDir(baseDir)(s3fs); err != nil {
			return err
		}

		if cfg.FileSystem.PartFileBytes != "" {
			partSize, err := strconv.ParseInt(cfg.FileSystem.PartFileBytes, 10, 64)
			if err != nil {
				return err
			}

			if err = WithPartFileBytes(partSize)(s3fs); err != nil {
				return err
			}
		}

		if err := WithBucket(cfg.S3.Bucket)(s3fs); err != nil {
			return err
		}

		if err := WithRegion(cfg.S3.Region)(s3fs); err != nil {
			return err
		}

		if err := WithWorkers(cfg.S3.Workers)(s3fs); err != nil {
			return err
		}

		if err := WithMaxConcurrentReads(cfg.S3.MaxConcurrentReads)(s3fs); err != nil {
			return err
		}

		return nil
	}
}

func WithBaseDir(baseDir string) Option {
	return func(s3fs *S3FS) error {
		s3fs.baseDir = baseDir
		return nil
	}
}

func WithPartFileBytes(partSizeInBytes int64) Option {
	return func(s3fs *S3FS) error {
		s3fs.partFileBytes = partSizeInBytes
		return nil
	}
}

func WithBucket(bucket string) Option {
	return func(s3fs *S3FS) error {
		s3fs.bucket = bucket
		return nil
	}
}

func WithRegion(region string) Option {
	return func(s3fs *S3FS) error {
		s3fs.region = region
		return nil
	}
}

func WithWorkers(workers int) Option {
	return func(s3fs *S3FS) error {
		s3fs.workers = workers
		return nil
	}
}

func WithMaxConcurrentReads(maxConcurrentReads int) Option {
	return func(s3fs *S3FS) error {
		s3fs.maxConcurrentReads = maxConcurrentReads
		return nil
	}
}
