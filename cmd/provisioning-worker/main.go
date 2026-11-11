package main

import (
	"log"
	"os"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/vaishnavi4068/ssp-workers/internal/activities"
	"github.com/vaishnavi4068/ssp-workers/internal/config"
	"github.com/vaishnavi4068/ssp-workers/internal/workflows"
)

func main() {
	cfg := config.LoadWorker()
	c, err := client.Dial(client.Options{HostPort: cfg.TemporalAddress, Namespace: cfg.TemporalNamespace})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	w := worker.New(c, cfg.TaskQueue, worker.Options{})
	workflows.Register(w)
	activities.RegisterWorker(w)

	log.Printf("ssp-workers Go provisioning worker polling queue %q (namespace=%s)", cfg.TaskQueue, cfg.TemporalNamespace)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Println(err)
		os.Exit(1)
	}
}
