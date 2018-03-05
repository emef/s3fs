s3fs is a simple commandline tool and library to stream data to/from amazon s3. 

**Install**
`go get github.com/emef/s3fs`

**Examples**

```
# stream a large file to s3. s3fs will break the input stream into 64mb chunks (by default) and
# upload them in parallel as separate files. 
cat /path/to/huge_file | s3fs -b my-aws-bucket path/to/s3/awesomeness
```

```
# stream a file written using s3fs to stdout. s3fs reads the part files in parallel but reassembles
# them in their original order. the output of this will be identical to the data written originally.
s3fs -b my-aws-bucket path/to/s3/awesomeness > /tmp/copy_of_huge_file
```

**Usage**

```
Usage:
  s3fs [command]

Available Commands:
  help        Help about any command
  read        Read some data from s3
  write       Writes some data to s3

Flags:
      --alsologtostderr                  log to standard error as well as files
      --base_dir string                  base directory for storing cached s3 objects
  -b, --bucket string                    s3 bucket to read/write s3 objects
      --config string                    config file (default is $HOME/.s3fs.conf)
  -h, --help                             help for s3fs
      --log_backtrace_at traceLocation   when logging hits line file:N, emit a stack trace (default :0)
      --log_dir string                   If non-empty, write log files in this directory
      --logtostderr                      log to standard error instead of files
      --max_concurrent_reads int         maximum number of concurrent reads
      --part_file_size int               size of individual part files (in bytes)
  -r, --region string                    s3 region (override config)
      --stderrthreshold severity         logs at or above this threshold go to stderr (default 2)
  -v, --v Level                          log level for V logs
      --vmodule moduleSpec               comma-separated list of pattern=N settings for file-filtered logging
  -w, --workers int                      number of s3 connection workers

Use "s3fs [command] --help" for more information about a command.
```

**Configuration**

AWS credentials will be looked up using the default creds chain (enviornment, shared credentials 
file, and IAM role), and cannot be manually specified in s3fs. Configuration is done by editing
the config file which is auto-generated on the first run of s3fs and lives at ~/.s3fs.conf. All
options can be overridden using commandline flags.
