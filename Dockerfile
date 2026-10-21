FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    git \
    jq \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Install OPA, Conftest, and Terraform 1.10.5 for runner K8s Jobs and policy evaluation
RUN curl -sL -o /usr/local/bin/opa https://openpolicyagent.org/downloads/v0.69.0/opa_linux_amd64_static && \
    chmod +x /usr/local/bin/opa && \
    curl -sL https://github.com/open-policy-agent/conftest/releases/download/v0.56.0/conftest_0.56.0_Linux_x86_64.tar.gz | tar xz -C /usr/local/bin conftest && \
    chmod +x /usr/local/bin/conftest && \
    curl -sL -o /tmp/terraform.zip https://releases.hashicorp.com/terraform/1.10.5/terraform_1.10.5_linux_amd64.zip && \
    unzip -qo /tmp/terraform.zip -d /usr/local/bin && \
    rm -f /tmp/terraform.zip

RUN groupadd -g 10001 spp && \
    useradd -u 10001 -g spp -m -s /bin/bash spp

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY workers /app/workers
COPY modules /app/modules

RUN chown -R spp:spp /app

USER 10001:10001

ENV TEMPORAL_ADDRESS=temporal-frontend.spp-temporal.svc.cluster.local:7233 \
    TEMPORAL_NAMESPACE=platform \
    TEMPORAL_TASK_QUEUE=idp-reconciler-v1 \
    SPP_K8S_NAMESPACE=spp-platform \
    SPP_RUNNER_KSA=spp-tf-runner \
    SPP_RUNNER_GSA=spp-deploy@microdrive-dev.iam.gserviceaccount.com \
    SPP_STATE_BUCKET=microdrive-dev-spp-tfstate \
    GOOGLE_CLOUD_PROJECT=microdrive-dev \
    GOOGLE_CLOUD_LOCATION=us-east1 \
    SPP_WORKER_USE_K8S=true

CMD ["python", "-m", "workers.main"]
