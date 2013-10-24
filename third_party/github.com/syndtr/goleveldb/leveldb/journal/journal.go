// Copyright 2011 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Taken from: https://code.google.com/p/leveldb-go/source/browse/leveldb/record/record.go?r=1d5ccbe03246da926391ee12d1c6caae054ff4b0
// License, authors and contributors informations can be found at bellow URLs respectively:
// 	https://code.google.com/p/leveldb-go/source/browse/LICENSE
//	https://code.google.com/p/leveldb-go/source/browse/AUTHORS
//  https://code.google.com/p/leveldb-go/source/browse/CONTRIBUTORS

// Package journal reads and writes sequences of journals. Each journal is a stream
// of bytes that completes before the next journal starts.
//
// When reading, call Next to obtain an io.Reader for the next journal. Next will
// return io.EOF when there are no more journals. It is valid to call Next
// without reading the current journal to exhaustion.
//
// When writing, call Next to obtain an io.Writer for the next journal. Calling
// Next finishes the current journal. Call Close to finish the final journal.
//
// Optionally, call Flush to finish the current journal and flush the underlying
// writer without starting a new journal. To start a new journal after flushing,
// call Next.
//
// Neither Readers or Writers are safe to use concurrently.
//
// Example code:
//	func read(r io.Reader) ([]string, error) {
//		var ss []string
//		journals := journal.NewReader(r)
//		for {
//			j, err := journals.Next()
//			if err == io.EOF {
//				break
//			}
//			if err != nil {
//				return nil, err
//			}
//			s, err := ioutil.ReadAll(j)
//			if err != nil {
//				return nil, err
//			}
//			ss = append(ss, string(s))
//		}
//		return ss, nil
//	}
//
//	func write(w io.Writer, ss []string) error {
//		journals := journal.NewWriter(w)
//		for _, s := range ss {
//			j, err := journals.Next()
//			if err != nil {
//				return err
//			}
//			if _, err := j.Write([]byte(s)), err != nil {
//				return err
//			}
//		}
//		return journals.Close()
//	}
//
// The wire format is that the stream is divided into 32KiB blocks, and each
// block contains a number of tightly packed chunks. Chunks cannot cross block
// boundaries. The last block may be shorter than 32 KiB. Any unused bytes in a
// block must be zero.
//
// A journal maps to one or more chunks. Each chunk has a 7 byte header (a 4
// byte checksum, a 2 byte little-endian uint16 length, and a 1 byte chunk type)
// followed by a payload. The checksum is over the chunk type and the payload.
//
// There are four chunk types: whether the chunk is the full journal, or the
// first, middle or last chunk of a multi-chunk journal. A multi-chunk journal
// has one first chunk, zero or more middle chunks, and one last chunk.
//
// The wire format allows for limited recovery in the face of data corruption:
// on a format error (such as a checksum mismatch), the reader moves to the
// next block and looks for the next full or first chunk.
package journal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/borgenk/qdo/third_party/github.com/syndtr/goleveldb/leveldb/util"
)

// These constants are part of the wire format and should not be changed.
const (
	fullChunkType   = 1
	firstChunkType  = 2
	middleChunkType = 3
	lastChunkType   = 4
)

const (
	blockSize  = 32 * 1024
	headerSize = 7
)

type flusher interface {
	Flush() error
}

// DroppedError is the error type that passed to Dropper.Drop method.
type DroppedError struct {
	Size   int
	Reason string
}

func (e DroppedError) Error() string {
	return fmt.Sprintf("leveldb/journal: dropped %d bytes: %s", e.Size, e.Reason)
}

// Dropper is the interface that wrap simple Drop method. The Drop
// method will be called when the journal reader dropping a chunk.
type Dropper interface {
	Drop(err error)
}

// Reader reads journals from an underlying io.Reader.
type Reader struct {
	// r is the underlying reader.
	r io.Reader
	// the dropper.
	dropper Dropper
	// strict flag.
	strict bool
	// seq is the sequence number of the current journal.
	seq int
	// buf[i:j] is the unread portion of the current chunk's payload.
	// The low bound, i, excludes the chunk header.
	i, j int
	// n is the number of bytes of buf that are valid. Once reading has started,
	// only the final block can have n < blockSize.
	n int
	// last is whether the current chunk is the last chunk of the journal.
	last bool
	// err is any accumulated error.
	err error
	// buf is the buffer.
	buf [blockSize]byte
}

// NewReader returns a new reader. The dropper may be nil, and if
// strict is true then corrupted or invalid chunk will halt the journal
// reader entirely.
func NewReader(r io.Reader, dropper Dropper, strict bool) *Reader {
	return &Reader{
		r:       r,
		dropper: dropper,
		strict:  strict,
		last:    true,
	}
}

// nextChunk sets r.buf[r.i:r.j] to hold the next chunk's payload, reading the
// next block into the buffer if necessary.
func (r *Reader) nextChunk(wantFirst, skip bool) error {
	for {
		if r.j+headerSize <= r.n {
			checksum := binary.LittleEndian.Uint32(r.buf[r.j+0 : r.j+4])
			length := binary.LittleEndian.Uint16(r.buf[r.j+4 : r.j+6])
			chunkType := r.buf[r.j+6]

			var err error
			if checksum == 0 && length == 0 && chunkType == 0 {
				// Drop entire block.
				err = DroppedError{r.n - r.j, "zero header"}
				r.i = r.n
				r.j = r.n
			} else {
				m := r.n - r.j
				r.i = r.j + headerSize
				r.j = r.j + headerSize + int(length)
				if r.j > r.n {
					// Drop entire block.
					err = DroppedError{m, "chunk length overflows block"}
					r.i = r.n
					r.j = r.n
				} else if checksum != util.NewCRC(r.buf[r.i-1:r.j]).Value() {
					// Drop entire block.
					err = DroppedError{m, "checksum mismatch"}
					r.i = r.n
					r.j = r.n
				}
			}
			if wantFirst && err == nil && chunkType != fullChunkType && chunkType != firstChunkType {
				if skip {
					// The chunk are intentionally skipped.
					if chunkType == lastChunkType {
						skip = false
					}
					continue
				} else {
					// Drop the chunk.
					err = DroppedError{r.j - r.i + headerSize, "orphan chunk"}
				}
			}
			if err == nil {
				r.last = chunkType == fullChunkType || chunkType == lastChunkType
			} else {
				if r.dropper != nil {
					r.dropper.Drop(err)
				}
				if r.strict {
					r.err = err
				}
			}
			return err
		}
		if r.n < blockSize && r.n > 0 {
			// This is the last block.
			if r.j != r.n {
				r.err = io.ErrUnexpectedEOF
			} else {
				r.err = io.EOF
			}
			return r.err
		}
		n, err := io.ReadFull(r.r, r.buf[:])
		if err != nil && err != io.ErrUnexpectedEOF {
			r.err = err
			return r.err
		}
		if n == 0 {
			r.err = io.EOF
			return r.err
		}
		r.i, r.j, r.n = 0, 0, n
	}
	panic("unreachable")
}

// Next returns a reader for the next journal. It returns io.EOF if there are no
// more journals. The reader returned becomes stale after the next Next call,
// and should no longer be used.
func (r *Reader) Next() (io.Reader, error) {
	r.seq++
	if r.err != nil {
		return nil, r.err
	}
	skip := !r.last
	for {
		r.i = r.j
		if r.nextChunk(true, skip) != nil {
			// So that 'orphan chunk' drop will be reported.
			skip = false
		} else {
			break
		}
		if r.err != nil {
			return nil, r.err
		}
	}
	return &singleReader{r, r.seq, nil}, nil
}

// Reset resets the journal reader, allows reuse of the journal reader.
func (r *Reader) Reset(reader io.Reader, dropper Dropper, strict bool) error {
	r.seq++
	err := r.err
	r.r = reader
	r.dropper = dropper
	r.strict = strict
	r.i = 0
	r.j = 0
	r.n = 0
	r.last = true
	r.err = nil
	return err
}

type singleReader struct {
	r   *Reader
	seq int
	err error
}

func (x *singleReader) Read(p []byte) (int, error) {
	r := x.r
	if r.seq != x.seq {
		return 0, errors.New("leveldb/journal: stale reader")
	}
	if x.err != nil {
		return 0, x.err
	}
	if r.err != nil {
		return 0, r.err
	}
	for r.i == r.j {
		if r.last {
			return 0, io.EOF
		}
		if x.err = r.nextChunk(false, false); x.err != nil {
			return 0, x.err
		}
	}
	n := copy(p, r.buf[r.i:r.j])
	r.i += n
	return n, nil
}

func (x *singleReader) ReadByte() (byte, error) {
	r := x.r
	if r.seq != x.seq {
		return 0, errors.New("leveldb/journal: stale reader")
	}
	if x.err != nil {
		return 0, x.err
	}
	if r.err != nil {
		return 0, r.err
	}
	for r.i == r.j {
		if r.last {
			return 0, io.EOF
		}
		if x.err = r.nextChunk(false, false); x.err != nil {
			return 0, x.err
		}
	}
	c := r.buf[r.i]
	r.i++
	return c, nil
}

// Writer writes journals to an underlying io.Writer.
type Writer struct {
	// w is the underlying writer.
	w io.Writer
	// seq is the sequence number of the current journal.
	seq int
	// f is w as a flusher.
	f flusher
	// buf[i:j] is the bytes that will become the current chunk.
	// The low bound, i, includes the chunk header.
	i, j int
	// buf[:written] has already been written to w.
	// written is zero unless Flush has been called.
	written int
	// first is whether the current chunk is the first chunk of the journal.
	first bool
	// pending is whether a chunk is buffered but not yet written.
	pending bool
	// err is any accumulated error.
	err error
	// buf is the buffer.
	buf [blockSize]byte
}

// NewWriter returns a new Writer.
func NewWriter(w io.Writer) *Writer {
	f, _ := w.(flusher)
	return &Writer{
		w: w,
		f: f,
	}
}

// fillHeader fills in the header for the pending chunk.
func (w *Writer) fillHeader(last bool) {
	if w.i+headerSize > w.j || w.j > blockSize {
		panic("leveldb/journal: bad writer state")
	}
	if last {
		if w.first {
			w.buf[w.i+6] = fullChunkType
		} else {
			w.buf[w.i+6] = lastChunkType
		}
	} else {
		if w.first {
			w.buf[w.i+6] = firstChunkType
		} else {
			w.buf[w.i+6] = middleChunkType
		}
	}
	binary.LittleEndian.PutUint32(w.buf[w.i+0:w.i+4], util.NewCRC(w.buf[w.i+6:w.j]).Value())
	binary.LittleEndian.PutUint16(w.buf[w.i+4:w.i+6], uint16(w.j-w.i-headerSize))
}

// writeBlock writes the buffered block to the underlying writer, and reserves
// space for the next chunk's header.
func (w *Writer) writeBlock() {
	_, w.err = w.w.Write(w.buf[w.written:])
	w.i = 0
	w.j = headerSize
	w.written = 0
}

// writePending finishes the current journal and writes the buffer to the
// underlying writer.
func (w *Writer) writePending() {
	if w.err != nil {
		return
	}
	if w.pending {
		w.fillHeader(true)
		w.pending = false
	}
	_, w.err = w.w.Write(w.buf[w.written:w.j])
	w.written = w.j
}

// Close finishes the current journal and closes the writer.
func (w *Writer) Close() error {
	w.seq++
	w.writePending()
	if w.err != nil {
		return w.err
	}
	w.err = errors.New("leveldb/journal: closed Writer")
	return nil
}

// Flush finishes the current journal, writes to the underlying writer, and
// flushes it if that writer implements interface{ Flush() error }.
func (w *Writer) Flush() error {
	w.seq++
	w.writePending()
	if w.err != nil {
		return w.err
	}
	if w.f != nil {
		w.err = w.f.Flush()
		return w.err
	}
	return nil
}

// Reset resets the journal writer, allows reuse of the journal writer. Reset
// will also closes the journal writer if not already.
func (w *Writer) Reset(writer io.Writer) (err error) {
	w.seq++
	if w.err == nil {
		w.writePending()
		err = w.err
	}
	w.w = writer
	w.f, _ = writer.(flusher)
	w.i = 0
	w.j = 0
	w.written = 0
	w.first = false
	w.pending = false
	w.err = nil
	return
}

// Next returns a writer for the next journal. The writer returned becomes stale
// after the next Close, Flush or Next call, and should no longer be used.
func (w *Writer) Next() (io.Writer, error) {
	w.seq++
	if w.err != nil {
		return nil, w.err
	}
	if w.pending {
		w.fillHeader(true)
	}
	w.i = w.j
	w.j = w.j + headerSize
	// Check if there is room in the block for the header.
	if w.j > blockSize {
		// Fill in the rest of the block with zeroes.
		for k := w.i; k < blockSize; k++ {
			w.buf[k] = 0
		}
		w.writeBlock()
		if w.err != nil {
			return nil, w.err
		}
	}
	w.first = true
	w.pending = true
	return singleWriter{w, w.seq}, nil
}

type singleWriter struct {
	w   *Writer
	seq int
}

func (x singleWriter) Write(p []byte) (int, error) {
	w := x.w
	if w.seq != x.seq {
		return 0, errors.New("leveldb/journal: stale writer")
	}
	if w.err != nil {
		return 0, w.err
	}
	n0 := len(p)
	for len(p) > 0 {
		// Write a block, if it is full.
		if w.j == blockSize {
			w.fillHeader(false)
			w.writeBlock()
			if w.err != nil {
				return 0, w.err
			}
			w.first = false
		}
		// Copy bytes into the buffer.
		n := copy(w.buf[w.j:], p)
		w.j += n
		p = p[n:]
	}
	return n0, nil
}
