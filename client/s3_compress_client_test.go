package client

import (
	"fmt"
	"github.com/The-Data-Appeal-Company/s3-gompress/compressors"
	"github.com/The-Data-Appeal-Company/s3-gompress/test"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

type CompressorError struct {
}

func (c CompressorError) Compress(bytes []byte) ([]byte, error) {
	return []byte{}, fmt.Errorf("nullaaa")
}

func (c CompressorError) Decompress(data []byte) ([]byte, error) {
	return []byte{}, fmt.Errorf("nullaaa")
}

type S3clientMock struct {
	objects map[string][]byte
}

func NewS3clientMock() *S3clientMock {
	return &S3clientMock{objects: make(map[string][]byte)}
}

func (s *S3clientMock) Put(key string, object []byte) error {
	if key == "i_want_error" {
		return fmt.Errorf("ulalalala")
	}
	s.objects[key] = object
	return nil
}

func (s *S3clientMock) Get(key string) ([]byte, error) {
	obj, ok := s.objects[key]
	if ok {
		return obj, nil
	}
	return []byte{}, fmt.Errorf("aia")
}

func TestS3CompressorClient_Get(t *testing.T) {
	s3ClientMock := NewS3clientMock()
	compressorErrorMock := CompressorError{}
	err := s3ClientMock.Put("input.json.gz", test.ReadFileOrError(t, "test_data/input.json.gz"))
	assert.NoError(t, err)
	type fields struct {
		s3Client   S3Client
		compressor compressors.Compressor
	}
	type args struct {
		key string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "shouldGetFromS3",
			fields: fields{
				s3Client:   s3ClientMock,
				compressor: &compressors.GzipCompressor{},
			},
			args: args{
				key: "input.json.gz",
			},
			want:    test.ReadFileOrError(t, "test_data/input.json"),
			wantErr: false,
		},
		{
			name: "shouldErrorIfS3Error",
			fields: fields{
				s3Client:   s3ClientMock,
				compressor: &compressors.GzipCompressor{},
			},
			args: args{
				key: "antani.json",
			},
			want:    []byte{},
			wantErr: true,
		},
		{
			name: "shouldErrorWhenCompressorError",
			fields: fields{
				s3Client:   s3ClientMock,
				compressor: compressorErrorMock,
			},
			args: args{
				key: "i_want_error",
			},
			want:    []byte{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &S3CompressorClient{
				s3Client:   tt.fields.s3Client,
				compressor: tt.fields.compressor,
			}
			got, err := s.Get(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestS3CompressorClient_Put(t *testing.T) {
	s3ClientMock := NewS3clientMock()
	compressorErrorMock := CompressorError{}
	type fields struct {
		s3Client   S3Client
		compressor compressors.Compressor
	}
	type args struct {
		key    string
		object []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "shouldPutOnS3",
			fields: fields{
				s3Client:   s3ClientMock,
				compressor: &compressors.GzipCompressor{},
			},
			args: args{
				key:    "input.json.gz",
				object: test.ReadFileOrError(t, "test_data/input.json"),
			},
			wantErr: false,
		},
		{
			name: "shouldErrorWhenS3Error",
			fields: fields{
				s3Client:   s3ClientMock,
				compressor: &compressors.GzipCompressor{},
			},
			args: args{
				key:    "i_want_error",
				object: test.ReadFileOrError(t, "test_data/input.json"),
			},
			wantErr: true,
		},
		{
			name: "shouldErrorWhenCompressorError",
			fields: fields{
				s3Client:   s3ClientMock,
				compressor: compressorErrorMock,
			},
			args: args{
				key:    "i_want_error",
				object: test.ReadFileOrError(t, "test_data/input.json"),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &S3CompressorClient{
				s3Client:   tt.fields.s3Client,
				compressor: tt.fields.compressor,
			}
			if err := s.Put(tt.args.key, tt.args.object); (err != nil) != tt.wantErr {
				t.Errorf("Put() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				get, err := s3ClientMock.Get("input.json.gz")
				assert.NoError(t, err)
				assert.NotNil(t, get)
			}
		})
	}
}
