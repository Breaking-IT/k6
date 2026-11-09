package breakingit

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"time"

	"github.com/gofrs/uuid/v5"
)

const pgEpochSeconds int64 = 946684800
const dbBatchBytes = 2 * 1024 * 1024

// PostgreSQL binary COPY types in the existing non-SM BreakTest table order.
// t = timestamp, s = text, i = int32, b = boolean, u = UUID.
var customSchemas = map[string]string{
	"requests_raw":   "tssssbiisiii",
	"transactions":   "tsssbiii",
	"requests_error": "tssssssiissssssu",
	"virtual_users":  "tsssiii",
	// time, run_id, screenshot_hash, mime_type, data (base64 PNG)
	"browser_screenshots": "tssss",
}

// Largest base64 screenshot stored in BreakTest DB. A batch holding one must
// stay within the storage service's 8 MiB compressed / 32 MiB decoded batch
// transport bound; base64 PNG barely compresses, so this keeps a margin.
const dbScreenshotBytes = 7 * 1024 * 1024

func encodeDBRow(table string, row []string) ([]byte, error) {
	schema := customSchemas[table]
	if schema == "" || len(row) != len(schema) {
		return nil, fmt.Errorf("invalid %s row width: %d", table, len(row))
	}
	// Reject before allocating a second copy of a potentially enormous error response.
	size := 2 + 4*len(row)
	for _, s := range row {
		size += len(s)
	}
	limit := dbBatchBytes - 1024
	if table == "browser_screenshots" {
		limit = dbScreenshotBytes + 4096
	}
	if size > limit {
		return nil, fmt.Errorf("%s row exceeds BreakTest DB batch budget", table)
	}
	b := bytes.NewBuffer(make([]byte, 0, size))
	_ = binary.Write(b, binary.BigEndian, int16(len(row)))
	for i, kind := range []byte(schema) {
		value := row[i]
		// Match the existing CSV writer's empty-field NULL semantics.
		if value == "" {
			_ = binary.Write(b, binary.BigEndian, int32(-1))
			continue
		}
		var data []byte
		switch kind {
		case 's':
			data = []byte(value)
		case 't':
			t, err := time.Parse(time.RFC3339Nano, value)
			if err != nil {
				return nil, fmt.Errorf("%s timestamp: %w", table, err)
			}
			n := (t.Unix()-pgEpochSeconds)*1_000_000 + int64(t.Nanosecond()/1000)
			if n < 0 {
				return nil, fmt.Errorf("timestamp before BreakTest DB epoch")
			}
			data = binary.BigEndian.AppendUint64(nil, uint64(n))
		case 'i':
			n, err := strconv.ParseInt(value, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("%s integer column %d: %w", table, i, err)
			}
			// Unavailable connect times must never become negative averages.
			if n < 0 && ((table == "requests_raw" && i == 9) || (table == "requests_error" && i == 8)) {
				_ = binary.Write(b, binary.BigEndian, int32(-1))
				continue
			}
			data = binary.BigEndian.AppendUint32(nil, uint32(n))
		case 'b':
			v, err := strconv.ParseBool(value)
			if err != nil {
				return nil, err
			}
			data = []byte{0}
			if v {
				data[0] = 1
			}
		case 'u':
			id, err := uuid.FromString(value)
			if err != nil {
				return nil, err
			}
			data = id[:]
		}
		_ = binary.Write(b, binary.BigEndian, int32(len(data)))
		_, _ = b.Write(data)
	}
	return b.Bytes(), nil
}
