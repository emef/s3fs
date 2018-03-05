package cmd

import (
	goflag "flag"
	"io"
	"os"
	"path"
	"strings"

	s3fs_lib "github.com/emef/s3fs/s3fs"
	"github.com/golang/glog"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	CONFIG_BASE_DIR             = "base_dir"
	CONFIG_PART_FILE_BYTES      = "part_file_size"
	CONFIG_BUCKET               = "bucket"
	CONFIG_REGION               = "region"
	CONFIG_WORKERS              = "workers"
	CONFIG_MAX_CONCURRENT_READS = "max_concurrent_reads"
)

var (
	configPath         string
	baseDir            string
	partFileBytes      int64
	bucket             string
	region             string
	workers            int
	maxConcurrentReads int
)

// Will be initialized on successful invocation of s3fs
var s3fs *s3fs_lib.S3FS

var rootCmd = &cobra.Command{
	Use:   "s3fs",
	Short: "s3fs maps s3 to a local fs cache",
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		glog.Fatalf("Error: %v", err)
	}
}

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(
		&configPath, "config", "", "config file (default is $HOME/.s3fs.conf)")
	rootCmd.PersistentFlags().StringVar(
		&baseDir, CONFIG_BASE_DIR, "",
		"base directory for storing cached s3 objects")
	rootCmd.PersistentFlags().Int64Var(
		&partFileBytes, CONFIG_PART_FILE_BYTES, 0,
		"size of individual part files (in bytes)")
	rootCmd.PersistentFlags().StringVarP(
		&bucket, CONFIG_BUCKET, "b", "",
		"s3 bucket to read/write s3 objects")
	rootCmd.PersistentFlags().StringVarP(
		&region, CONFIG_REGION, "r", "",
		"s3 region (override config)")
	rootCmd.PersistentFlags().IntVarP(
		&workers, CONFIG_WORKERS, "w", 0,
		"number of s3 connection workers")
	rootCmd.PersistentFlags().IntVar(
		&maxConcurrentReads, CONFIG_MAX_CONCURRENT_READS, 0,
		"maximum number of concurrent reads")
	rootCmd.PersistentFlags().AddGoFlagSet(goflag.CommandLine)
}

func initConfig() {
	goflag.Parse()

	opts := buildOpts()
	s3fs_init, err := s3fs_lib.NewS3FS(opts...)
	if err != nil {
		glog.Fatalf("Error initializing s3fs: %v", err)
	}

	s3fs = s3fs_init
}

func buildOpts() []s3fs_lib.Option {
	homeConfigPath := path.Join(os.Getenv("HOME"), ".s3fs.conf")
	if _, err := os.Stat(homeConfigPath); os.IsNotExist(err) {
		if err = createDefaultConfig(homeConfigPath); err != nil {
			glog.Warningf("Could not create default config at '%s'", homeConfigPath)
		}
	}

	if configPath == "" {
		configPath = homeConfigPath
	}

	opts := []s3fs_lib.Option{s3fs_lib.WithConfigFile(configPath)}

	rootCmd.Flags().VisitAll(func(flag *pflag.Flag) {
		if !flag.Changed {
			return
		}

		switch flag.Name {
		case CONFIG_BASE_DIR:
			opts = append(opts, s3fs_lib.WithBaseDir(baseDir))
		case CONFIG_PART_FILE_BYTES:
			opts = append(opts, s3fs_lib.WithPartFileBytes(partFileBytes))
		case CONFIG_BUCKET:
			opts = append(opts, s3fs_lib.WithBucket(bucket))
		case CONFIG_REGION:
			opts = append(opts, s3fs_lib.WithRegion(region))
		case CONFIG_WORKERS:
			opts = append(opts, s3fs_lib.WithWorkers(workers))
		case CONFIG_MAX_CONCURRENT_READS:
			opts = append(opts, s3fs_lib.WithMaxConcurrentReads(maxConcurrentReads))
		}
	})

	return opts
}

func createDefaultConfig(defaultPath string) error {
	file, err := os.OpenFile(defaultPath, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}

	_, err = io.Copy(file, strings.NewReader(DEFAULT_CONFIG))
	return err
}

const DEFAULT_CONFIG = `; s3fs default config
[s3]
bucket=mforbes-s3fs
region=us-east-1
workers=20
maxconcurrentreads=20

[filesystem]
basedir=$HOME/.s3fs/
partfilebytes=64000000
`
