package config

import "os"

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

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
