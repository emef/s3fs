package s3fs

import (
	"bytes"
	"io"
	"path"
	"strconv"

	"github.com/golang/glog"
)

const (
	DEFAULT_PART_SIZE int64  = 64000000
	DEFAULT_BASE_DIR  string = "/tmp/s3fs"
)

type S3FS struct {
	baseDir            string
	partFileBytes      int64
	bucket             string
	region             string
	workers            int
	maxConcurrentReads int

	fleet *S3Fleet
}

type Option func(*S3FS) error

func NewS3FS(opts ...Option) (*S3FS, error) {
	s3fs := &S3FS{}
	s3fs.baseDir = DEFAULT_BASE_DIR
	s3fs.partFileBytes = DEFAULT_PART_SIZE

	for _, opt := range opts {
		if err := opt(s3fs); err != nil {
			return nil, err
		}
	}

	s3fs.fleet = NewS3Fleet(s3fs.workers, s3fs.region)

	glog.Infof("[S3FS] baseDir=%v", s3fs.baseDir)
	glog.Infof("[S3FS] partFileBytes=%v", s3fs.partFileBytes)
	glog.Infof("[S3FS] bucket=%v", s3fs.bucket)
	glog.Infof("[S3FS] region=%v", s3fs.region)
	glog.Infof("[S3FS] workers=%v", s3fs.workers)
	glog.Infof("[S3FS] maxConcurrentReads=%v", s3fs.maxConcurrentReads)

	return s3fs, nil
}

func (s3fs *S3FS) Write(dest string, data io.Reader) error {
	results := make([]AsyncWrite, 0)
	for i := 0; ; i++ {
		key := path.Join(dest, strconv.Itoa(i))

		buf := bytes.NewBuffer(make([]byte, 0, s3fs.partFileBytes))
		n, err := io.CopyN(buf, data, s3fs.partFileBytes)
		if err != nil && err != io.EOF {
			return err
		}

		if n > 0 {
			bufReader := bytes.NewReader(buf.Bytes())
			asyncResult := s3fs.fleet.AsyncWrite(s3fs.bucket, key, bufReader)
			results = append(results, asyncResult)
		}

		if err == io.EOF {
			break
		}
	}

	for _, asyncResult := range results {
		if err := <-asyncResult; err != nil {
			return err
		}
	}

	return nil
}

func (s3fs *S3FS) Read(src string) io.Reader {
	pr, pw := io.Pipe()

	reads := make(chan *AsyncRead, s3fs.maxConcurrentReads)
	done := make(chan interface{})

	go func() {
		for i := 0; ; i++ {
			select {
			case reads <- s3fs.fleet.AsyncRead(s3fs.bucket, src, i):
			case <-done:
				break
			}
		}
	}()

	go func() {
		defer close(done)

		for asyncRead := range reads {
			if err := <-asyncRead.Callback; err != nil {
				pw.CloseWithError(err)
				return
			}

			if _, err := pw.Write(asyncRead.Data); err != nil {
				pw.CloseWithError(err)
				return
			}
		}
	}()

	return pr
}
