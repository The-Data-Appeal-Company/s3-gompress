package compressors

import (
	"bytes"
	"compress/gzip"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
)

type GzipCompressor struct {
}

func (g *GzipCompressor) Compress(data []byte) ([]byte, error) {
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	if _, err := gz.Write(data); err != nil {
		log.Error(err)
		return nil, err

	}
	if err := gz.Close(); err != nil {
		log.Error(err)
		return nil, err
	}

	return b.Bytes(), nil
}

func (g *GzipCompressor) Decompress(input []byte) ([]byte, error) {
	gr, err := gzip.NewReader(bytes.NewBuffer(input))
	if err != nil {
		return []byte{}, err
	}
	defer gr.Close()
	data, err := ioutil.ReadAll(gr)
	if err != nil {
		return []byte{}, err
	}
	return data, nil
}
