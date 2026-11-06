package breakingit

import (
	"bytes"
	"crypto/md5" //nolint:gosec // Java UUID.nameUUIDFromBytes compatibility, not cryptography.
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/gofrs/uuid/v5"
)

const (
	binaryCopyHeader          = "PGCOPY\n\xff\r\n\x00\x00\x00\x00\x00\x00\x00\x00\x00"
	postgresEpochMicros int64 = 946684800000000
)

// Column types follow the pg-proxy/JMeter COPY contract: timestamp, text, bool, int4, UUID.
func (l *Logger) binarySchema(buffer *bytes.Buffer) string {
	switch buffer {
	case l.bufHTTP:
		if l.isSynthetic {
			return "tssssssbiisiiiu"
		}
		return "tssssbiisiii"
	case l.bufTrans:
		if l.isSynthetic {
			return "tsssssbiiiu"
		}
		return "tsssbiii"
	case l.bufErrors:
		if l.isSynthetic {
			return "tsssssssiissssssuu"
		}
		return "tssssssiissssssu"
	default:
		return "tsssiii"
	}
}

// syntheticRunUUID matches Java's nameUUIDFromBytes fallback for legacy run identifiers.
func syntheticRunUUID(value string) uuid.UUID {
	if id, err := uuid.FromString(value); err == nil {
		return id
	}
	if value == "" {
		value = "unknown"
	}
	id := uuid.UUID(md5.Sum([]byte(value))) //nolint:gosec // Protocol compatibility.
	id[6] = (id[6] & 0x0f) | 0x30
	id[8] = (id[8] & 0x3f) | 0x80
	return id
}

// writeBinaryRow is called with mu held. Buffers contain rows only; framing is added per batch.
func (l *Logger) writeBinaryRow(buffer *bytes.Buffer, row []any) {
	schema := l.binarySchema(buffer)
	// Reserve enough room for common rows, and write directly into unused buffer capacity.
	size := 2 + len(row)*12
	for _, value := range row {
		if text, ok := value.(string); ok {
			size += len(text)
		}
	}
	buffer.Grow(size)
	data := binary.BigEndian.AppendUint16(buffer.AvailableBuffer(), uint16(len(row))) //nolint:gosec
	for i, value := range row {
		var err error
		data, err = appendBinaryField(data, value, schema[i])
		if err != nil {
			_, _ = fmt.Fprintf(l.out, "breakingit: cannot encode COPY row: %v\n", err)
			return
		}
	}

	_, _ = buffer.Write(data)
	if !l.firstQueued && l.firstData != nil {
		l.firstQueued = true
		l.firstData <- struct{}{}
	}
}

func appendBinaryField(data []byte, value any, kind byte) ([]byte, error) {
	switch v := value.(type) {
	case time.Time:
		data = binary.BigEndian.AppendUint32(data, 8)
		return binary.BigEndian.AppendUint64(data, uint64(v.UnixMicro()-postgresEpochMicros)), nil //nolint:gosec
	case int64:
		if v < math.MinInt32 || v > math.MaxInt32 {
			return nil, fmt.Errorf("int4 out of range: %d", v)
		}
		data = binary.BigEndian.AppendUint32(data, 4)
		return binary.BigEndian.AppendUint32(data, uint32(v)), nil //nolint:gosec // Signed int4 bits.
	case string:
		return appendBinaryString(data, v, kind)
	default:
		return nil, fmt.Errorf("unsupported field type %T", value)
	}
}

func appendBinaryString(data []byte, value string, kind byte) ([]byte, error) {
	// Preserve CSV's NULL semantics for empty fields, except JMeter's run UUID fallback.
	if value == "" && kind != 'u' {
		return binary.BigEndian.AppendUint32(data, ^uint32(0)), nil
	}
	switch kind {
	case 'b':
		v, err := strconv.ParseBool(value)
		if err != nil {
			return nil, err
		}
		data = binary.BigEndian.AppendUint32(data, 1)
		if v {
			return append(data, 1), nil
		}
		return append(data, 0), nil
	case 'i':
		n, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return nil, err
		}
		return appendBinaryField(data, n, kind)
	case 'u':
		id := syntheticRunUUID(value)
		data = binary.BigEndian.AppendUint32(data, 16)
		return append(data, id[:]...), nil
	default:
		data = binary.BigEndian.AppendUint32(data, uint32(len(value))) //nolint:gosec // COPY field length.
		return append(data, value...), nil
	}
}
