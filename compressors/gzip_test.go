package compressors

import (
	"github.com/The-Data-Appeal-Company/s3-gompress/test"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func TestGzipCompressor_Compress(t *testing.T) {
	type args struct {
		data []byte
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "shouldCompressMessage",
			args: args{
				data: test.ReadFileOrError(t, "test_data/input.json"),
			},
			want:    test.ReadFileOrError(t, "test_data/input.json"),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GzipCompressor{}
			got, err := g.Compress(tt.args.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("Compress() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			decompress, err := g.Decompress(got)
			assert.NoError(t, err)
			if !reflect.DeepEqual(decompress, tt.want) {
				t.Errorf("Compress() got = %v, want %v", decompress, tt.want)
			}
		})
	}
}

func TestGzipCompressor_Decompress(t *testing.T) {
	type args struct {
		input []byte
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "shouldDecompressGzippedMessage",
			args: args{
				input: test.ReadFileOrError(t, "test_data/input.json.gz"),
			},
			want:    test.ReadFileOrError(t, "test_data/input.json"),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GzipCompressor{}
			got, err := g.Decompress(tt.args.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Decompress() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Decompress() got = %v, want %v", got, tt.want)
			}
		})
	}
}
