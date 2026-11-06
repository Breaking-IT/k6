package breakingit

import (
	"bytes"
	"encoding/csv"
	"strconv"
	"testing"
	"time"
)

// Compare row serialization with the previous per-row CSV writer, excluding network and gzip.
func BenchmarkTransactionEncoding(b *testing.B) {
	at := time.Date(2026, 10, 13, 12, 0, 0, 123456000, time.UTC)
	b.Run("CSV", func(b *testing.B) {
		var buffer bytes.Buffer
		b.ReportAllocs()
		for b.Loop() {
			buffer.Reset()
			row := []string{at.UTC().Format(time.RFC3339Nano), "run-123", "Amsterdam", "Checkout", "true", "", "", strconv.FormatInt(123, 10)}
			writer := csv.NewWriter(&buffer)
			_ = writer.Write(row)
			writer.Flush()
		}
	})
	b.Run("Binary", func(b *testing.B) {
		logger := newTransactionTestLogger(false)
		b.ReportAllocs()
		for b.Loop() {
			logger.bufTrans.Reset()
			logger.writeBinaryRow(logger.bufTrans, []any{at, "run-123", "Amsterdam", "Checkout", "true", "", "", int64(123)})
		}
	})
}
