package cmd

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.k6.io/k6/v2/cmd/state"
	"go.k6.io/k6/v2/lib/fsext"
)

func TestReportUsageDoesNotIdentifyInstallation(t *testing.T) {
	t.Parallel()

	for _, storedID := range []string{"", "123e4567-e89b-42d3-a456-426614174000"} {
		t.Run(storedID, func(t *testing.T) {
			t.Parallel()
			reports := make(chan map[string]any, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var report map[string]any
				if err := json.NewDecoder(r.Body).Decode(&report); err != nil {
					t.Error(err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				reports <- report
				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()

			gs := &state.GlobalState{
				FS: fsext.NewMemMapFs(), UserOSConfigDir: "/config",
				Logger: logrus.New(),
				Env:    map[string]string{state.UsageReportURL: server.URL},
			}
			idPath := filepath.Join(gs.UserOSConfigDir, "k6", "installation-id")
			if storedID != "" {
				require.NoError(t, gs.FS.MkdirAll(filepath.Dir(idPath), 0o700))
				require.NoError(t, fsext.WriteFile(gs.FS, idPath, []byte(storedID), 0o600))
			}
			reportUsage(context.Background(), gs, func(context.Context) map[string]any {
				report := map[string]any{"iterations": 1}
				addEnvironmentInfo(report, envLookup(gs.Env))
				return report
			})

			select {
			case report := <-reports:
				assert.Equal(t, float64(1), report["iterations"])
				assert.NotContains(t, report, "installation_id")
				assert.NotContains(t, report, "build_origin")
				assert.NotContains(t, report, "extensions")
			default:
				t.Fatal("usage report was not received")
			}
			if storedID == "" {
				exists, err := fsext.Exists(gs.FS, idPath)
				require.NoError(t, err)
				assert.False(t, exists, "must not create an installation identifier")
			} else {
				stored, err := fsext.ReadFile(gs.FS, idPath)
				require.NoError(t, err)
				assert.Equal(t, storedID, string(stored))
			}
		})
	}
}
