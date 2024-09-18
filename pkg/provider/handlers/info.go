package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/openfaas/faas-provider/types"
)

const (
	// OrchestrationIdentifier identifier string for provider orchestration
	OrchestrationIdentifier = "containerd"

	// ProviderName name of the provider
	ProviderName = "faasd-ce"
)

// MakeInfoHandler creates handler for /system/info endpoint
func MakeInfoHandler(version, sha string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil {
			defer r.Body.Close()
		}

		infoResponse := types.ProviderInfo{
			Orchestration: OrchestrationIdentifier,
			Name:          ProviderName,
			Version: &types.VersionInfo{
				Release: version,
				SHA:     sha,
			},
		}

		jsonOut, err := json.Marshal(infoResponse)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(jsonOut)
	}
}

const faasdMaxFunctions = 15
const faasdMaxNs = 1
