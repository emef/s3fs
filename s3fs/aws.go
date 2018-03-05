package s3fs

import (
	"bytes"
	"context"
	"io"
	"path"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/golang/glog"
	"golang.org/x/time/rate"
)

type S3Fleet struct {
	workers       []*s3Worker
	readRequests  chan *readRequest
	writeRequests chan *writeRequest
}

type s3Worker struct {
	client        *s3.S3
	rateLimiter   *rate.Limiter
	readRequests  chan *readRequest
	writeRequests chan *writeRequest
	close         chan interface{}
}

type AsyncWrite chan error
type AsyncRead struct {
	Callback chan error
	Data     []byte
}

type writeRequest struct {
	bucket     string
	key        string
	data       io.ReadSeeker
	asyncWrite AsyncWrite
}

type readRequest struct {
	bucket    string
	key       string
	asyncRead *AsyncRead
}

func NewS3Fleet(n int, region string) *S3Fleet {
	rateLimiter := rate.NewLimiter(rate.Every(10*time.Millisecond), 10)
	readRequests := make(chan *readRequest, n)
	writeRequests := make(chan *writeRequest, n)
	workers := make([]*s3Worker, n)
	for i := 0; i < n; i++ {
		workers[i] = news3Worker(rateLimiter, region, readRequests, writeRequests)
	}

	return &S3Fleet{
		workers:       workers,
		readRequests:  readRequests,
		writeRequests: writeRequests,
	}
}

func (fleet *S3Fleet) AsyncWrite(bucket, key string, data io.ReadSeeker) AsyncWrite {
	asyncWrite := AsyncWrite(make(chan error, 1))
	req := &writeRequest{bucket, key, data, asyncWrite}
	fleet.writeRequests <- req
	return asyncWrite
}

func (fleet *S3Fleet) AsyncRead(bucket, key string, part int) *AsyncRead {
	fullKey := path.Join(key, strconv.Itoa(part))
	asyncRead := &AsyncRead{make(chan error, 1), nil}
	req := &readRequest{bucket, fullKey, asyncRead}
	fleet.readRequests <- req
	return asyncRead
}

func news3Worker(
	rateLimiter *rate.Limiter,
	region string,
	readRequests chan *readRequest,
	writeRequests chan *writeRequest) *s3Worker {

	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(region),
	}))

	worker := &s3Worker{
		client:        s3.New(sess),
		rateLimiter:   rateLimiter,
		readRequests:  readRequests,
		writeRequests: writeRequests,
		close:         make(chan interface{}),
	}

	go worker.work()
	return worker
}

func (worker *s3Worker) work() {
	for {
		select {
		case req := <-worker.writeRequests:
			worker.handleWriteRequest(req)

		case req := <-worker.readRequests:
			worker.handleReadRequest(req)

		case <-worker.close:
			break
		}
	}
}

func (worker *s3Worker) handleWriteRequest(req *writeRequest) {
	ctx := context.Background()

	worker.rateLimiter.Wait(ctx)
	glog.Infof("Starting put of %v/%v", req.bucket, req.key)
	_, err := worker.client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String(req.bucket),
		Key:    aws.String(req.key),
		Body:   req.data,
	})

	if err == nil {
		glog.Infof("Completed put of %s/%s", req.bucket, req.key)
	}

	req.asyncWrite <- err
	close(req.asyncWrite)
}

func (worker *s3Worker) handleReadRequest(req *readRequest) {
	ctx := context.Background()

	worker.rateLimiter.Wait(ctx)
	result, err := worker.client.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(req.bucket),
		Key:    aws.String(req.key),
	})

	if err != nil {
		// Cast err to awserr.Error to handle specific error codes.
		aerr, ok := err.(awserr.Error)
		if ok && aerr.Code() == s3.ErrCodeNoSuchKey {
			err = io.EOF
		}
	} else {
		buf := bytes.NewBuffer(make([]byte, *result.ContentLength))
		_, err = io.Copy(buf, result.Body)
		if err == nil {
			req.asyncRead.Data = buf.Bytes()
		}
		result.Body.Close()
	}

	if err == nil {
		glog.Infof("Read %s/%s", req.bucket, req.key)
	}

	req.asyncRead.Callback <- err
	close(req.asyncRead.Callback)
}
