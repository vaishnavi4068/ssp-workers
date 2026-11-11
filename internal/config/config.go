package config

import (
	"os"
	"path/filepath"
)

// Worker mirrors Python workers.config.WorkerConfig env bindings (ADR-AUT-001).
type Worker struct {
	TemporalAddress   string
	TemporalNamespace string
	TaskQueue         string
}

func LoadWorker() Worker {
	return Worker{
		TemporalAddress:   env("TEMPORAL_ADDRESS", "localhost:7233"),
		TemporalNamespace: env("TEMPORAL_NAMESPACE", "platform"),
		TaskQueue:         env("TEMPORAL_TASK_QUEUE", "spp-gcs-provisioning"),
	}
}

func UseRealIntegrations() bool {
	return os.Getenv("SSP_WORKER_USE_K8S") == "true" ||
		os.Getenv("SSP_WORKER_USE_GCS") == "true" ||
		os.Getenv("SSP_WORKER_USE_DB") == "true"
}

// UsePythonActivityBridge runs side-effect activities via workers.run_activity (until native Go port).
func UsePythonActivityBridge() bool {
	if v := os.Getenv("SSP_WORKER_PYTHON_ACTIVITIES"); v != "" {
		return v == "true"
	}
	return UseRealIntegrations()
}

func PythonBin() string {
	if v := os.Getenv("SSP_WORKER_PYTHON"); v != "" {
		return v
	}
	return "python3"
}

// WorkerAppRoot is the image WORKDIR containing workers/ and modules/.
func WorkerAppRoot() string {
	if v := os.Getenv("SSP_WORKER_APP_ROOT"); v != "" {
		return v
	}
	if wd, err := os.Getwd(); err == nil {
		if st, err := os.Stat(filepath.Join(wd, "workers")); err == nil && st.IsDir() {
			return wd
		}
	}
	return "/app"
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
