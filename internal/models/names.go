package models

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// K8sJobName mirrors idp.k8s_job_name / Python k8s_job_name(operation_id, phase).
func K8sJobName(operationID, phase string) string {
	sum := sha256.Sum256([]byte(operationID))
	digest := hex.EncodeToString(sum[:])[:20]
	name := fmt.Sprintf("tf-%s-%s", strings.ReplaceAll(phase, "_", "-"), digest)
	if len(name) > 63 {
		panic("k8s job name exceeds 63 chars: " + name)
	}
	return name
}
