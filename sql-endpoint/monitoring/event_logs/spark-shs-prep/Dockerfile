# ─────────────────────────────────────────────────────────────
# Stage 1: OCI CLI installer
# ─────────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS oci-installer

RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 \
      python3-pip \
      python3-venv \
      ca-certificates \
    && rm -rf /var/lib/apt/lists/*

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
# ─────────────────────────────────────────────────────────────
FROM debian:bookworm-slim

LABEL maintainer="your-team"
LABEL description="OCI log preparer + Spark History Server — single image"
LABEL spark.version="3.5.1"

# ── System deps ───────────────────────────────────────────────
# procps   — provides `ps`, required by Spark's load-spark-env.sh
# findutils — provides `find`, used by prepare_logs.sh
RUN apt-get update && apt-get install -y --no-install-recommends \
      bash \
      curl \
      gzip \
      lz4 \
      jq \
      ca-certificates \
      procps \
      findutils \
      openjdk-17-jre-headless \
      python3 \
      python3-venv \
    && rm -rf /var/lib/apt/lists/*

# ── JAVA_HOME: resolve arch-independently at build time ───────
# Debian installs the JRE under java-17-openjdk-<arch>, e.g.:
#   amd64  → java-17-openjdk-amd64
#   arm64  → java-17-openjdk-arm64
# Using the /etc/alternatives symlink works on both architectures.
RUN JAVA_REAL=$(readlink -f /usr/bin/java | sed 's|/bin/java||') \
 && echo "JAVA_HOME=${JAVA_REAL}" \
 && echo "export JAVA_HOME=${JAVA_REAL}" >> /etc/environment \
 && ln -sfn "${JAVA_REAL}" /usr/lib/jvm/java-17-openjdk
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# ── Spark ─────────────────────────────────────────────────────
COPY --from=spark-downloader /opt/spark /opt/spark
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

# ── OCI CLI venv ──────────────────────────────────────────────
COPY --from=oci-installer /opt/oci-venv /opt/oci-venv
RUN ln -sf /opt/oci-venv/bin/oci /usr/local/bin/oci

# ── Verify everything works at build time ─────────────────────
RUN java -version \
 && python3 --version \
 && oci --version \
 && ps --version

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

EXPOSE 18080

# ─────────────────────────────────────────────────────────────
# Environment variables
# ─────────────────────────────────────────────────────────────

ENV OCI_NAMESPACE=""
ENV OCI_BUCKET=""
ENV OCI_REGION=""
ENV OCI_PREFIX=""
ENV OCI_CLI_AUTH=""
ENV OCI_PROFILE=""
ENV OCI_CONFIG_FILE=""
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
