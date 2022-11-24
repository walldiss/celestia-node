package eds

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/ipld/go-car"
	"github.com/ipld/go-car/util"
)

var errNilReader = errors.New("ods-reader: can't create ODSReader over nil reader")

// bufferedOdsReader will read odsSquareSize amount of leaves from reader into the buffer.
// It exposes the buffer to be read by io.Reader interface implementation
type bufferedOdsReader struct {
	carReader *bufio.Reader
	// current is the amount of CARv1 encoded leaves that have been read from reader. When current
	// reaches odsSquareSize, bufferedOdsReader will prevent further reads by returning io.EOF
	current, odsSquareSize int
	buf                    *bytes.Buffer
}

// ODSReader reads CARv1 encoded data from io.ReadCloser and limits the reader to the CAR header
// and first quadrant (ODS)
func ODSReader(carReader io.ReadCloser) (io.Reader, error) {
	if carReader == nil {
		return nil, errNilReader
	}

	odsR := &bufferedOdsReader{
		carReader: bufio.NewReaderSize(carReader, 4),
		buf:       new(bytes.Buffer),
	}

	// first LdRead reads the full CAR header to determine amount of shares in the ODS
	data, err := util.LdRead(odsR.carReader)
	if err != nil {
		return nil, fmt.Errorf("reading header: %v", err)
	}

	var header car.CarHeader
	err = cbor.DecodeInto(data, &header)
	if err != nil {
		return nil, fmt.Errorf("invalid header: %w", err)
	}

	// car header contains both row roots and col roots which is why
	// we divide by 4 to get the ODSWidth
	odsWidth := len(header.Roots) / 4
	odsR.odsSquareSize = odsWidth * odsWidth

	// NewCarReader will expect to read the header first, so write it first
	return odsR, util.LdWrite(odsR.buf, data)
}

func (r *bufferedOdsReader) Read(p []byte) (n int, err error) {
	// provided slice could be fully filled from buffer without extra reads
	if r.buf.Len() > len(p) {
		return r.buf.Read(p)
	}

	if r.current < r.odsSquareSize && r.buf.Len() < len(p) {
		if err := r.readLeaf(); err != nil {
			return 0, err
		}

		r.current++
	}

	// read buffer to slice
	return r.buf.Read(p)
}

// readLeaf reads one leaf from reader into bufferedOdsReader buffer
func (r *bufferedOdsReader) readLeaf() error {
	if _, err := r.carReader.Peek(1); err != nil { // no more blocks, likely clean io.EOF
		return err
	}

	l, err := binary.ReadUvarint(r.carReader)
	if err != nil {
		if err == io.EOF {
			return io.ErrUnexpectedEOF // don't silently pretend this is a clean EOF
		}
		return err
	}

	if l > uint64(util.MaxAllowedSectionSize) { // Don't OOM
		return errors.New("malformed car; header is bigger than util.MaxAllowedSectionSize")
	}

	buf := make([]byte, 8)
	n := binary.PutUvarint(buf, l)
	r.buf.Write(buf[:n])

	_, err = r.buf.ReadFrom(io.LimitReader(r.carReader, int64(l)))

	return err
}
