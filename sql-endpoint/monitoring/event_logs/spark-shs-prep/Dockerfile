# ─────────────────────────────────────────────────────────────
# Stage 1: OCI CLI installer
# Installs OCI CLI into a venv so it can be copied cleanly.
# Uses the same debian:bookworm-slim base as the final image
# to guarantee glibc compatibility.
# ─────────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS oci-installer

RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 \
      python3-pip \
      python3-venv \
      ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install OCI CLI into an isolated venv — avoids polluting system Python
RUN python3 -m venv /opt/oci-venv \
 && /opt/oci-venv/bin/pip install --no-cache-dir --upgrade pip \
 && /opt/oci-venv/bin/pip install --no-cache-dir oci-cli==3.29.2

# ─────────────────────────────────────────────────────────────
# Stage 2: Spark downloader
# ─────────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS spark-downloader

ARG SPARK_VERSION=3.5.1
ARG HADOOP_VERSION=3
ARG SPARK_TGZ=spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
ARG SPARK_URL=https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_TGZ}

RUN apt-get update && apt-get install -y --no-install-recommends \
      curl \
      ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fSL "${SPARK_URL}" -o /tmp/spark.tgz \
    && tar -xzf /tmp/spark.tgz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark \
    && rm /tmp/spark.tgz

# ─────────────────────────────────────────────────────────────
# Stage 3: Final image
# debian:bookworm-slim throughout — single glibc version,
# no cross-distro binary copying.
# ─────────────────────────────────────────────────────────────
FROM debian:bookworm-slim

LABEL maintainer="your-team"
LABEL description="OCI log preparer + Spark History Server — single image"
LABEL spark.version="3.5.1"

# ── System deps ───────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
      bash \
      curl \
      gzip \
      lz4 \
      jq \
      ca-certificates \
      openjdk-17-jre-headless \
      # Python for the OCI venv
      python3 \
      python3-venv \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# ── Spark ─────────────────────────────────────────────────────
COPY --from=spark-downloader /opt/spark /opt/spark
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

# ── OCI CLI venv (built on identical glibc — no conflicts) ────
COPY --from=oci-installer /opt/oci-venv /opt/oci-venv

# Expose the oci binary on PATH via a symlink
RUN ln -sf /opt/oci-venv/bin/oci /usr/local/bin/oci

# ── Verify everything works at build time ─────────────────────
RUN java -version \
 && python3 --version \
 && oci --version

# ── Scripts ───────────────────────────────────────────────────
COPY scripts/prepare_logs.sh /usr/local/bin/prepare_logs.sh
COPY scripts/entrypoint.sh   /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/prepare_logs.sh \
             /usr/local/bin/entrypoint.sh

# ── Spark config ──────────────────────────────────────────────
RUN mkdir -p /opt/spark/conf
COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# ── Volumes ───────────────────────────────────────────────────
VOLUME ["/shs-logs", "/root/.oci"]

# ── Expose SHS UI ─────────────────────────────────────────────
EXPOSE 18080

# ─────────────────────────────────────────────────────────────
# Environment variables
# ─────────────────────────────────────────────────────────────

ENV OCI_NAMESPACE=""
ENV OCI_BUCKET=""
ENV OCI_REGION=""
ENV OCI_PREFIX=""
ENV OCI_CLI_AUTH="api_key"
ENV OCI_PROFILE="DEFAULT"
ENV OCI_CONFIG_FILE="/root/.oci/config"
ENV OCI_CLI_SECURITY_TOKEN_FILE=""

ENV OUTPUT_DIR="/shs-logs"
ENV FLUENT_BIT_WRAPPED="auto"
ENV WATCH_MODE="true"
ENV POLL_INTERVAL="300"
ENV KEEP_RAW="false"
ENV DEBUG="false"

ENV SHS_PORT="18080"
ENV SHS_MAX_APPLICATIONS="500"
ENV SHS_RETAINED_APPLICATIONS="100"
ENV SHS_UPDATE_INTERVAL="10s"
ENV SHS_CLEANER_ENABLED="false"
ENV SHS_ACLS_ENABLED="false"

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
