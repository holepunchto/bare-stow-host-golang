package stow

import (
	"encoding/binary"
	"encoding/json"
	"errors"
)

const (
	tagControl = 0x00
	tagUser    = 0x01
)

type frame struct {
	tag     byte
	payload []byte
}

func encodeFrame(tag byte, payload []byte) []byte {
	b := make([]byte, 5+len(payload))

	binary.LittleEndian.PutUint32(b, uint32(len(payload)+1))
	b[4] = tag
	copy(b[5:], payload)

	return b
}

type control struct {
	Type  string `json:"type"`
	Grace *int64 `json:"grace,omitempty"`
}

func encodeControl(c control) []byte {
	payload, _ := json.Marshal(c)

	return encodeFrame(tagControl, payload)
}

type message struct {
	Type    string `json:"type"`
	Code    *int   `json:"code"`
	Message string `json:"message"`
	Stack   string `json:"stack"`
}

// Fields of the wrong type are left unset, as the other hosts do, rather than
// dropping the whole frame.
func decodeControl(payload []byte) (message, bool) {
	var m message

	err := json.Unmarshal(payload, &m)

	var mismatch *json.UnmarshalTypeError

	if err != nil && !errors.As(err, &mismatch) {
		return m, false
	}

	return m, true
}

type decoder struct {
	buf []byte
}

func (d *decoder) feed(data []byte) []frame {
	d.buf = append(d.buf, data...)

	var frames []frame

	offset := 0

	for len(d.buf)-offset >= 4 {
		length := int(binary.LittleEndian.Uint32(d.buf[offset:]))
		start := offset + 4
		end := start + length

		if len(d.buf) < end {
			break
		}

		if length > 0 {
			frames = append(frames, frame{
				tag:     d.buf[start],
				payload: append([]byte(nil), d.buf[start+1:end]...),
			})
		}

		offset = end
	}

	d.buf = append(d.buf[:0], d.buf[offset:]...)

	return frames
}
