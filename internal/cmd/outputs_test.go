package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuiltinOutputString(t *testing.T) {
	t.Parallel()
	exp := []string{
		"cloud", "csv", "datadog", "experimental-prometheus-rw",
		"influxdb", "json", "kafka", "statsd",
		"experimental-opentelemetry", "opentelemetry",
		"summary", "timescaledb-breakingit", "custom-db-breakingit",
	}
	assert.Equal(t, exp, builtinOutputStrings())
}

func TestCustomDBAndTimescaleConstructors(t *testing.T) {
	t.Parallel()
	constructors, err := getAllOutputConstructors()
	assert.NoError(t, err)
	assert.Contains(t, constructors, "timescaledb-breakingit")
	assert.Contains(t, constructors, "custom-db-breakingit")
}
