package breakingit

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"strconv"
	"testing"
	"time"

	"go.k6.io/k6/v2/metrics"

	"github.com/gofrs/uuid/v5"
	"github.com/stretchr/testify/require"
)

// Decode independently of the writer, including strict field widths and stream framing.
func readBinaryRows(reader io.Reader, schema string, framed bool) ([][]string, error) {
	if framed {
		header := make([]byte, 19)
		if _, err := io.ReadFull(reader, header); err != nil {
			return nil, err
		}
		if !bytes.Equal(header, []byte("PGCOPY\n\xff\r\n\x00\x00\x00\x00\x00\x00\x00\x00\x00")) {
			return nil, fmt.Errorf("invalid header")
		}
	}
	var rows [][]string
	for {
		var count int16
		err := binary.Read(reader, binary.BigEndian, &count)
		if errors.Is(err, io.EOF) && !framed {
			return rows, nil
		}
		if err != nil {
			return nil, err
		}
		if count == -1 && framed {
			rest, err := io.ReadAll(reader)
			if len(rest) != 0 {
				return nil, fmt.Errorf("trailing bytes")
			}
			return rows, err
		}
		if int(count) != len(schema) {
			return nil, fmt.Errorf("column count %d for %s", count, schema)
		}
		row := make([]string, count)
		for i, kind := range schema {
			var size int32
			if err := binary.Read(reader, binary.BigEndian, &size); err != nil {
				return nil, err
			}
			if size == -1 {
				continue
			}
			if size < 0 || size > 10000000 {
				return nil, fmt.Errorf("invalid length")
			}
			field := make([]byte, size)
			if _, err := io.ReadFull(reader, field); err != nil {
				return nil, err
			}
			widths := map[rune]int{'t': 8, 'i': 4, 'b': 1, 'u': 16}
			if want, ok := widths[kind]; ok && len(field) != want {
				return nil, fmt.Errorf("field %d width %d", i, size)
			}
			switch kind {
			case 't':
				row[i] = time.UnixMicro(int64(binary.BigEndian.Uint64(field)) + 946684800000000).UTC().Format(time.RFC3339Nano)
			case 'i':
				row[i] = strconv.FormatInt(int64(int32(binary.BigEndian.Uint32(field))), 10)
			case 'b':
				if field[0] > 1 {
					return nil, fmt.Errorf("invalid bool")
				}
				row[i] = strconv.FormatBool(field[0] == 1)
			case 'u':
				id, err := uuid.FromBytes(field)
				if err != nil {
					return nil, err
				}
				row[i] = id.String()
			default:
				row[i] = string(field)
			}
		}
		rows = append(rows, row)
	}
}

func TestBinaryEpochAndJavaUUID(t *testing.T) {
	t.Parallel()
	l := newTransactionTestLogger(false)
	l.writeBinaryRow(l.bufVUs, []any{time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), "a,\"b\"\n日本", "", "node", "-1", "", ""})
	rows, err := readBinaryRows(l.bufVUs, "tsssiii", false)
	require.NoError(t, err)
	require.Equal(t, [][]string{{"2000-01-01T00:00:00Z", "a,\"b\"\n日本", "", "node", "-1", "", ""}}, rows)
	require.Equal(t, "68ca2f05-25ba-39de-9dcd-ba9ac7e36f46", syntheticRunUUID("run-123").String())
}

func TestBinaryFieldBytes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		value any
		kind  byte
		want  []byte
	}{
		{time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), 't', []byte{0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0}},
		{time.Date(1999, 12, 31, 23, 59, 59, 999999000, time.UTC), 't', []byte{0, 0, 0, 8, 255, 255, 255, 255, 255, 255, 255, 255}},
		{int64(-2), 'i', []byte{0, 0, 0, 4, 255, 255, 255, 254}},
		{"true", 'b', []byte{0, 0, 0, 1, 1}},
		{"false", 'b', []byte{0, 0, 0, 1, 0}},
		{"", 'i', []byte{255, 255, 255, 255}},
		{"é", 's', []byte{0, 0, 0, 2, 195, 169}},
		{"00112233-4455-6677-8899-aabbccddeeff", 'u', []byte{0, 0, 0, 16, 0, 17, 34, 51, 68, 85, 102, 119, 136, 153, 170, 187, 204, 221, 238, 255}},
	} {
		got, err := appendBinaryField(nil, tc.value, tc.kind)
		require.NoError(t, err)
		require.Equal(t, tc.want, got)
	}
}

func TestInvalidBinaryRowDoesNotCorruptBuffer(t *testing.T) {
	t.Parallel()
	logger := newTransactionTestLogger(false)
	valid := []any{time.Now(), "run", "location", "node", int64(1), "", ""}
	logger.writeBinaryRow(logger.bufVUs, valid)
	before := bytes.Clone(logger.bufVUs.Bytes())
	invalid := append([]any(nil), valid...)
	invalid[4] = int64(1) << 32
	logger.writeBinaryRow(logger.bufVUs, invalid)
	require.Equal(t, before, logger.bufVUs.Bytes())
	logger.writeBinaryRow(logger.bufVUs, valid)
	rows, err := readBinaryRows(logger.bufVUs, "tsssiii", false)
	require.NoError(t, err)
	require.Len(t, rows, 2)
}

func TestAllBinaryBatchTables(t *testing.T) {
	t.Parallel()
	for _, synthetic := range []bool{false, true} {
		t.Run(fmt.Sprint(synthetic), func(t *testing.T) {
			t.Parallel()
			logger := newTransactionTestLogger(synthetic)
			for _, metric := range []string{"group_duration", "ws_ping", "custom_duration", "browser_errors", "checks", "vus"} {
				sample := transactionTestSample(metric, map[string]string{"group": "::checkout"}, false)
				if metric == "checks" {
					sample.Value = 0
				}
				logger.AddMetricSamples([]metrics.SampleContainer{sample})
			}
			body, boundary := logger.batchBody(logger.bufHTTP.Bytes(), logger.bufVUs.Bytes(), logger.bufTrans.Bytes(), logger.bufErrors.Bytes())
			gz, err := gzip.NewReader(bytes.NewReader(body))
			require.NoError(t, err)
			defer func() { require.NoError(t, gz.Close()) }()
			reader := multipart.NewReader(gz, boundary)
			schemas := map[string]string{"public.requests_raw": "tssssbiisiii", "public.requests_raw_sm": "tssssssbiisiiiu", "public.transactions": "tsssbiii", "public.transactions_sm": "tsssssbiiiu", "public.requests_error": "tssssssiissssssu", "public.requests_error_sm": "tsssssssiissssssuu", "public.virtual_users": "tsssiii"}
			counts := map[string]int{}
			for {
				part, err := reader.NextPart()
				if errors.Is(err, io.EOF) {
					break
				}
				require.NoError(t, err)
				table := part.Header.Get("X-Table")
				require.Equal(t, "binary", part.Header.Get("X-Format"))
				schema, ok := schemas[table]
				require.True(t, ok)
				rows, err := readBinaryRows(part, schema, true)
				require.NoError(t, err)
				counts[table] = len(rows)
			}
			suffix := ""
			if synthetic {
				suffix = "_sm"
			}
			require.Equal(t, 2, counts["public.requests_raw"+suffix])
			require.Equal(t, 2, counts["public.requests_error"+suffix])
			require.Equal(t, 1, counts["public.transactions"+suffix])
			if synthetic {
				require.Len(t, counts, 3)
			} else {
				require.Len(t, counts, 4)
				require.Equal(t, 1, counts["public.virtual_users"])
			}
		})
	}
}
