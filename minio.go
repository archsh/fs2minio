package main

import (
	"context"
	"fmt"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"log"
	"net/url"
	"strings"
)

type MinioOption struct {
	Endpoint string
	Access   string
	Secret   string
	Bucket   string
	Secure   bool
}

type MinioClient struct {
	c      *minio.Client
	bucket string
	ctx    context.Context
}

func parseMinioURL(s string) (*MinioOption, error) {
	var opt MinioOption
	if u, e := url.Parse(s); nil != e {
		return nil, e //log.Fatalln(e)
	} else {
		if strings.ToLower(u.Scheme) == "https" {
			opt.Secure = true
		}
		sss := strings.Split(strings.TrimLeft(u.Path, "/"), "/")
		if len(sss) < 1 {
			return nil, fmt.Errorf("bucket should specified")
		} else {
			opt.Bucket = sss[0]
		}
		opt.Endpoint = u.Host
		opt.Access = u.User.Username()
		opt.Secret, _ = u.User.Password()
	}
	return &opt, nil
}

func NewMinioClient(minioUrl string) (*MinioClient, error) {
	if opt, e := parseMinioURL(minioUrl); nil != e {
		log.Fatalln(e)
		return nil, e
	} else if s3Client, err := minio.New(opt.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(opt.Access, opt.Secret, ""),
		Secure: opt.Secure,
	}); nil != err {
		return nil, err
	} else {
		var client *MinioClient = new(MinioClient)
		client.c = s3Client
		client.bucket = opt.Bucket
		client.ctx = context.Background()
		return client, nil
	}
}

func (cli MinioClient) StatFile(filename string) (minio.ObjectInfo, error) {
	return cli.c.StatObject(cli.ctx, cli.bucket, filename, minio.StatObjectOptions{})
}

func (cli MinioClient) PutFile(dst string, src string, contentTypes ...string) (minio.UploadInfo, error) {
	var contentType string = ""
	if len(contentTypes) > 0 {
		contentType = contentTypes[0]
	}
	return cli.c.FPutObject(cli.ctx, cli.bucket, dst, src, minio.PutObjectOptions{
		ContentType: contentType,
	})
}
