package client

import (
	"fmt"
	"github.com/The-Data-Appeal-Company/s3-gompress/test"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/mitchelldavis/go_localstack/pkg/localstack"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"reflect"
	"testing"
)

var LOCALSTACK *localstack.Localstack
var bucketName = "bachettaccio"

type SetupLocalStack struct {
}

func (s SetupLocalStack) BeforeTest(suiteName, testName string) {
	InitializeLocalstack()
}

type TearDownLocalStack struct {
}

func (t TearDownLocalStack) AfterTest(suiteName, testName string) {
	err := LOCALSTACK.Destroy()
	if err != nil {
		log.Error(err)
	}
}

func InitializeLocalstack() {
	s3, _ := localstack.NewLocalstackService("s3")

	// Gather them all up...
	LOCALSTACK_SERVICES := &localstack.LocalstackServiceCollection{
		*s3,
	}

	// Initialize the services
	var err error

	LOCALSTACK, err = localstack.NewLocalstack(LOCALSTACK_SERVICES)
	if err != nil {
		log.Fatal(fmt.Sprintf("Unable to create the localstack instance: %s", err))
	}
	if LOCALSTACK == nil {
		log.Fatal("LOCALSTACK was nil.")
	}
}

func initStack(svc *s3.S3) error {
	_, err := svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	return err
}

func S3PlainClient_Get(t *testing.T) {
	sess := LOCALSTACK.CreateAWSSession()

	svc := s3.New(sess)
	err := initStack(svc)

	if err != nil {
		t.Fatal(err)
	}
	type fields struct {
		client client.ConfigProvider
		bucket string
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
				client: sess,
				bucket: bucketName,
			},
			args: args{
				key: "input.json",
			},
			want:    test.ReadFileOrError(t, "test_data/input.json"),
			wantErr: false,
		},
		{
			name: "shouldErrorIfBucketNotExists",
			fields: fields{
				client: sess,
				bucket: "non-esisto",
			},
			args: args{
				key: "input.json",
			},
			want:    []byte{},
			wantErr: true,
		},
		{
			name: "shouldErrorIfFileNotExists",
			fields: fields{
				client: sess,
				bucket: "non-esisto",
			},
			args: args{
				key: "nullaaa.json",
			},
			want:    []byte{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewS3PlainClient(
				tt.fields.client,
				tt.fields.bucket,
			)
			if tt.args.key == "input.json" {
				_ = s.Put("input.json", test.ReadFileOrError(t, "test_data/input.json"))
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

func S3PlainClient_Put(t *testing.T) {
	sess := LOCALSTACK.CreateAWSSession()

	svc := s3.New(sess)
	err := initStack(svc)

	if err != nil {
		t.Fatal(err)
	}
	type fields struct {
		client client.ConfigProvider
		bucket string
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
				client: sess,
				bucket: bucketName,
			},
			args: args{
				key:    "antani.json",
				object: test.ReadFileOrError(t, "test_data/input.json"),
			},
			wantErr: false,
		},
		{
			name: "shouldErrorIfBucketNotExists",
			fields: fields{
				client: sess,
				bucket: "non-esisto",
			},
			args: args{
				key: "antani.json",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewS3PlainClient(
				tt.fields.client,
				tt.fields.bucket,
			)
			if err := s.Put(tt.args.key, tt.args.object); (err != nil) != tt.wantErr {
				t.Errorf("Put() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				object, err := svc.GetObject(&s3.GetObjectInput{
					Bucket: aws.String(tt.fields.bucket),
					Key:    aws.String(tt.args.key),
				})

				if err != nil {
					t.Fatal(err)
				}
				all, err := ioutil.ReadAll(object.Body)
				assert.NoError(t, err)
				assert.Equal(t, tt.args.object, all)
			}
		})
	}
}
