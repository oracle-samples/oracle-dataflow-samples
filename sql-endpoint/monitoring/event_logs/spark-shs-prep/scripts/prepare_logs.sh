#!/bin/bash

#Oracle Cloud Infrastrure Dataflow
#
#Copyright © 2025, Oracle and/or its affiliates.
#
#Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/


set -euo pipefail

# ─────────────────────────────────────────────────────────────
# Spark History Server Log Preparer
# ─────────────────────────────────────────────────────────────

log()   { echo "[$(date '+%Y-%m-%d %H:%M:%S')] INFO  $*"; }
warn()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] WARN  $*"; }
err()   { echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR $*" >&2; }
debug() { [[ "${DEBUG:-false}" == "true" ]] && echo "[$(date '+%Y-%m-%d %H:%M:%S')] DEBUG $*" || true; }

# ── Required env vars ─────────────────────────────────────────
: "${OCI_NAMESPACE:?OCI_NAMESPACE is required}"
: "${OCI_BUCKET:?OCI_BUCKET is required}"
: "${OCI_REGION:?OCI_REGION is required}"
: "${OUTPUT_DIR:=/shs-logs}"

# ── Auth config ───────────────────────────────────────────────
OCI_CLI_AUTH="${OCI_CLI_AUTH:-api_key}"
OCI_PROFILE="${OCI_PROFILE:-DEFAULT}"
# Original config path (may contain host-absolute paths)
OCI_CONFIG_FILE="${OCI_CONFIG_FILE:-/root/.oci/config}"
# Container-side OCI directory (where ~/.oci is mounted)
OCI_CONFIG_DIR="$(dirname "$OCI_CONFIG_FILE")"

# ── Optional operational config ───────────────────────────────
OCI_PREFIX="${OCI_PREFIX:-}"
WATCH_MODE="${WATCH_MODE:-false}"
POLL_INTERVAL="${POLL_INTERVAL:-300}"
FLUENT_BIT_WRAPPED="${FLUENT_BIT_WRAPPED:-auto}"
KEEP_RAW="${KEEP_RAW:-false}"

RAW_DIR="/tmp/oci-raw"
mkdir -p "$RAW_DIR" "$OUTPUT_DIR"

# ── OCI base args (populated by patch_and_build_config) ───────
OCI_BASE_ARGS=()

# Wrapper — all oci calls go through here
oci_cmd() { oci "${OCI_BASE_ARGS[@]}" "$@"; }

# ─────────────────────────────────────────────────────────────
# patch_and_build_config
#
# The OCI config written by `oci session authenticate` contains
# the host machine's absolute paths, e.g.:
#   key_file            = /Users/alice/.oci/sessions/PROF/oci_api_key.pem
#   security_token_file = /Users/alice/.oci/sessions/PROF/token
#
# Inside the container ~/.oci is mounted at $OCI_CONFIG_DIR
# (/root/.oci). This function:
#   1. Copies the config to a writable temp file
#   2. Detects the host-side ~/.oci prefix
#   3. Rewrites every file path in the config to the container equivalent
#   4. Sets OCI_CONFIG_FILE to the patched copy
#   5. Builds OCI_BASE_ARGS using the patched config
# ─────────────────────────────────────────────────────────────
patch_and_build_config() {
  case "$OCI_CLI_AUTH" in
    instance_principal|resource_principal)
      log "  Auth $OCI_CLI_AUTH — no config file needed"
      OCI_BASE_ARGS=(--auth "$OCI_CLI_AUTH")
      return
      ;;
  esac

  # ── Validate config file exists ───────────────────────────
  if [[ ! -f "$OCI_CONFIG_FILE" ]]; then
    err "OCI config not found: $OCI_CONFIG_FILE"
    err "Mount your ~/.oci dir:  -v ~/.oci:/root/.oci:ro"
    exit 1
  fi

  # ── Make a writable copy ──────────────────────────────────
  local patched="/tmp/oci_config_patched_$$"
  cp "$OCI_CONFIG_FILE" "$patched"

  # ── Detect host-prefix from any key_file / security_token_file entry ──
  # Strategy: find the first path value that does NOT start with
  # $OCI_CONFIG_DIR, strip the filename/suffix to get the root prefix.
  # IMPORTANT: Skip this entirely if config file is already in /tmp (was patched by entrypoint)
  local host_prefix=""
  
  # If OCI_CONFIG_FILE is already in /tmp or /var/tmp, assume it was patched by entrypoint
  if [[ "$OCI_CONFIG_FILE" == /tmp/* ]] || [[ "$OCI_CONFIG_FILE" == /var_tmp/* ]]; then
    log "  Config already patched by entrypoint (OCI_CONFIG_FILE in /tmp) — validating paths..."
  else
    # Only do path detection if config wasn't already patched
    local oci_dir_prefix
    oci_dir_prefix=$(dirname "$OCI_CONFIG_FILE")
    
    while IFS= read -r line; do
      # Extract the value (right-hand side of = , trimmed)
      local val
      val=$(echo "$line" | sed -E 's/^[^=]+=\s*//' | xargs 2>/dev/null || true)
      # Skip if it already points inside the container mount dir
      [[ "$val" == "${oci_dir_prefix}"* ]] && continue
      # Skip if it doesn't look like an absolute path
      [[ "$val" != /* ]] && continue
      # Derive the prefix: strip everything from /sessions/ onward
      # e.g. /Users/alice/.oci/sessions/PROF/token → /Users/alice/.oci
      # Also handle tilde paths: ~/.oci/sessions/PROF/token → ~/.oci → $HOME/.oci
      local candidate
      # First expand tilde to get absolute path, then strip /sessions/
      if [[ "$val" == ~* ]]; then
        candidate=$(echo "$val" | sed -E 's|~/||' | sed -E 's|/sessions/.*||')
        [[ -n "$HOME" ]] && candidate="$HOME/$candidate" || candidate=""
      else
        candidate=$(echo "$val" | sed -E 's|/sessions/.*||')
      fi
      if [[ -n "$candidate" && "$candidate" != "$val" ]]; then
        host_prefix="$candidate"
        break
      fi
    done < <(grep -E '^\s*(key_file|security_token_file)=??' "$patched" || true)

    if [[ -n "$host_prefix" ]]; then
      log "  Rewriting config paths:"
      log "    host prefix → $host_prefix"
      log "    container   → $oci_dir_prefix"
      # Replace every occurrence of the host prefix with the container dir
      sed -i "s|${host_prefix}|${oci_dir_prefix}|g" "$patched"
    else
      log "  Config paths already use container paths — no rewrite needed"
    fi
  fi

  # ── Validate the profile exists ───────────────────────────
  if ! grep -q "^\[${OCI_PROFILE}\]" "$patched"; then
    err "Profile [${OCI_PROFILE}] not found in $OCI_CONFIG_FILE"
    err "Available profiles:"
    grep '^\[' "$patched" | tr -d '[]' | sed 's/^/    /'
    rm -f "$patched"
    exit 1
  fi

  # ── Support OCI_CLI_SECURITY_TOKEN_FILE env var ──────────────────
  # If OCI_CLI_SECURITY_TOKEN_FILE is set, add/update security_token_file in the profile
  if [[ -n "${OCI_CLI_SECURITY_TOKEN_FILE:-}" ]]; then
    log "  Using OCI_CLI_SECURITY_TOKEN_FILE: $OCI_CLI_SECURITY_TOKEN_FILE"
    if [[ ! -f "$OCI_CLI_SECURITY_TOKEN_FILE" ]]; then
      err "Security token file not found: $OCI_CLI_SECURITY_TOKEN_FILE"
      rm -f "$patched"
      exit 1
    fi
    # Remove any existing security_token_file entry for this profile first to avoid duplicates
    sed -i "/^\[${OCI_PROFILE}\]/,/^\[/{ /^\s*security_token_file\s*=/d }" "$patched"
    # Add the new security_token_file entry after the profile header
    sed -i "/^\[${OCI_PROFILE}\]/a\\
security_token_file = ${OCI_CLI_SECURITY_TOKEN_FILE}" "$patched"
  fi

  # ── Validate all referenced files now exist ───────────────
  local missing=0
  while IFS= read -r fpath; do
    fpath=$(echo "$fpath" | xargs 2>/dev/null || true)
    if [[ -n "$fpath" && ! -f "$fpath" ]]; then
      err "  MISSING: $fpath"
      missing=$(( missing + 1 ))
    else
      debug "  OK: $fpath"
    fi
  done < <(
    awk -v profile="[${OCI_PROFILE}]" '
      $0 == profile { found=1; next }
      found && /^\[/ { exit }
      found && /^\s*(key_file|security_token_file)=??/ {
        sub(/^[^=]+=\s*/, ""); print
      }
    ' "$patched" || true
  )

  if [[ $missing -gt 0 ]]; then
    err "$missing file(s) referenced in config do not exist in the container."
    err "Make sure you mount the full ~/.oci directory: -v ~/.oci:/root/.oci:ro"
    rm -f "$patched"
    exit 1
  fi

  # ── Dump the resolved profile for diagnostics ─────────────
  if [[ "${DEBUG:-false}" == "true" ]]; then
    debug "Resolved profile [${OCI_PROFILE}]:"
    awk -v profile="[${OCI_PROFILE}]" '
      $0 == profile { found=1; print; next }
      found && /^\[/ { exit }
      found { print }
    ' "$patched" | sed 's/^/    /' >&2
  fi

  # ── Update OCI_CONFIG_FILE to the patched copy ────────────
  OCI_CONFIG_FILE="$patched"
  log "  Using config: $OCI_CONFIG_FILE  profile: $OCI_PROFILE"

  # ── Build base args ───────────────────────────────────────
  OCI_BASE_ARGS=(
    --config-file "$OCI_CONFIG_FILE"
    --profile     "$OCI_PROFILE"
    --auth        "$OCI_CLI_AUTH"
  )
}

# ── Security token expiry check ───────────────────────────────
check_token_expiry() {
  [[ "$OCI_CLI_AUTH" != "security_token" ]] && return

  # Read security_token_file path from the ALREADY PATCHED config
  local token_file
  token_file=$(awk -v profile="[${OCI_PROFILE}]" '
    $0 == profile { found=1; next }
    found && /^\[/ { exit }
    found && /^\s*security_token_file\s*=/ {
      sub(/^[^=]+=\s*/, ""); gsub(/ /, ""); print; exit
    }
  ' "$OCI_CONFIG_FILE" 2>/dev/null || true)

  if [[ -z "$token_file" ]]; then
    warn "security_token_file not found in profile [${OCI_PROFILE}] — cannot check expiry"
    return
  fi
  if [[ ! -f "$token_file" ]]; then
    err "Token file does not exist: $token_file"
    err "Refresh with: oci session authenticate --profile $OCI_PROFILE"
    exit 1
  fi

  local payload exp now
  payload=$(cut -d. -f2 <<< "$(cat "$token_file")" | base64 -d 2>/dev/null || true)
  exp=$(echo "$payload" | jq -r '.exp // empty' 2>/dev/null || true)
  now=$(date +%s)

  if [[ -z "$exp" ]]; then
    warn "Could not parse token expiry from $token_file"
    return
  fi

  local remaining=$(( exp - now ))
  local expiry_str
  expiry_str=$(date -d "@$exp" '+%Y-%m-%d %H:%M:%S UTC' 2>/dev/null \
    || date -r "$exp" '+%Y-%m-%d %H:%M:%S UTC' 2>/dev/null \
    || echo "unknown")

  if [[ $remaining -le 0 ]]; then
    err "Security token EXPIRED at $expiry_str"
    err "Refresh: oci session authenticate --profile $OCI_PROFILE"
    err "Then restart the container to reload the ~/.oci mount"
    exit 1
  elif [[ $remaining -le 300 ]]; then
    warn "Token expires in $(( remaining / 60 ))m ($expiry_str) — refresh soon"
  else
    log "  Token valid for ~$(( remaining / 60 )) more minutes (expires $expiry_str)"
  fi
}

# ── OCI auth check ────────────────────────────────────────────
check_auth() {
  log "Checking OCI authentication..."
  log "  Auth   : $OCI_CLI_AUTH"
  log "  Profile: $OCI_PROFILE"
  log "  Config : $OCI_CONFIG_FILE"

  # Patch host paths → container paths, build OCI_BASE_ARGS
  patch_and_build_config

  # Check token expiry AFTER patching (uses updated OCI_CONFIG_FILE)
  check_token_expiry

  # Verify credentials work
  local ns
  ns=$(oci_cmd os ns get --query 'data' --raw-output 2>&1) \
    || { err "OCI authentication failed: $ns"; exit 1; }
  log "OCI authentication OK (namespace: $ns)"
}

# ── Download files from OCI bucket ───────────────────────────
download_logs() {
  log "Listing objects in oci://$OCI_NAMESPACE/$OCI_BUCKET/${OCI_PREFIX:-(root)}"

  local list_args=(
    os object list
    --namespace   "$OCI_NAMESPACE"
    --bucket-name "$OCI_BUCKET"
    --region      "$OCI_REGION"
    --all
  )
  [[ -n "$OCI_PREFIX" ]] && list_args+=(--prefix "$OCI_PREFIX")

  local raw_list
  raw_list=$(oci_cmd "${list_args[@]}" 2>&1) || {
    err "Failed to list objects: $raw_list"
    return 1
  }

  mapfile -t objects < <(
    echo "$raw_list" \
      | jq -r '.data[].name' 2>/dev/null \
      | grep -E '\.(log\.gz|log\.lz4|log|lz4|gz)$' \
      || true
  )

  if [[ ${#objects[@]} -eq 0 ]]; then
    warn "No event log files found. All objects in bucket:"
    echo "$raw_list" | jq -r '.data[].name' 2>/dev/null | head -20 || true
    return 0
  fi

  log "Found ${#objects[@]} log file(s) to download"

  for obj in "${objects[@]}"; do
    local dest="$RAW_DIR/$obj"
    mkdir -p "$(dirname "$dest")"

    if [[ -f "$dest" ]]; then
      debug "Already downloaded: $obj"
      # Verify the existing file is valid
      local existing_size
      existing_size=$(wc -c < "$dest" 2>/dev/null || echo "0")
      if [[ "$existing_size" -eq 0 ]]; then
        warn "Existing file is empty, re-downloading: $obj"
        rm -f "$dest"
      else
        continue
      fi
    fi

    log "Downloading: $obj"
    oci_cmd os object get \
      --namespace   "$OCI_NAMESPACE" \
      --bucket-name "$OCI_BUCKET" \
      --region      "$OCI_REGION" \
      --name        "$obj" \
      --file        "$dest" 2>&1 \
      || { warn "Failed to download $obj — skipping"; rm -f "$dest"; continue; }
    
    # Verify download was successful
    if [[ -f "$dest" ]]; then
      local downloaded_size
      downloaded_size=$(wc -c < "$dest" 2>/dev/null || echo "0")
      log "  Downloaded: $obj ($downloaded_size bytes)"
    else
      warn "  Download failed - file not found after download: $obj"
    fi
  done
}

# ── Detect Fluent Bit wrapping ────────────────────────────────
detect_fluent_bit() {
  local sample="$1"
  echo "$sample" | grep -qE '"log"\s*:\s*".*Event' && echo "true" || echo "false"
}

# ── Extract clean Spark JSON event lines ─────────────────────
extract_spark_lines() {
  local file="$1" wrapped="$2"
  if [[ "$wrapped" == "true" ]]; then
    jq -r 'if type=="object" then (if .log then .log else (.|tostring) end) else . end' \
      2>/dev/null < "$file" | grep -E '^\{"Event":' || true
  else
    grep -E '^\{"Event":' "$file" || true
  fi
}

# ── Decompress a single raw file ─────────────────────────────
process_file() {
  local raw="$1"
  local decompressed wrapped
  
  # Input validation
  if [[ -z "$raw" ]]; then
    echo "ERROR: process_file called with empty raw parameter"
    return 1
  fi
  
  if [[ ! -f "$raw" ]]; then
    echo "ERROR: Source file not found: $raw"
    return 1
  fi
  
  local raw_size
  raw_size=$(wc -c < "$raw" 2>/dev/null || echo "0")
  if [[ "$raw_size" -eq 0 ]]; then
    echo "ERROR: Source file is empty: $raw"
    return 1
  fi
  
  # Create output temp file path
  decompressed=$(mktemp /tmp/shs_decompressed_XXXXXX.txt) || {
    echo "ERROR: Failed to create temp file"
    return 1
  }
  
  # Decompress based on file extension
  case "$raw" in
    *.gz)
      gunzip -dc "$raw" > "$decompressed" 2>&1 || {
        echo "ERROR: gunzip failed: $(cat 2>&1 || true)"
        rm -f "$decompressed"
        return 1
      }
      ;;
    *.lz4)
      lz4 -dc "$raw" > "$decompressed" 2>&1 || {
        echo "ERROR: lz4 failed"
        rm -f "$decompressed"
        return 1
      }
      ;;
    *)
      cp "$raw" "$decompressed" || {
        echo "ERROR: cp failed"
        rm -f "$decompressed"
        return 1
      }
      ;;
  esac
  
  # Verify output
  if [[ ! -s "$decompressed" ]]; then
    echo "ERROR: Decompression produced empty file"
    rm -f "$decompressed"
    return 1
  fi

  # Auto-detect fluent bit wrapping
  wrapped="false"
  if [[ "${FLUENT_BIT_WRAPPED:-auto}" == "auto" ]]; then
    local first_line
    first_line=$(head -1 "$decompressed" 2>/dev/null || echo "")
    if echo "$first_line" | grep -q '"log"'; then
      wrapped="true"
    fi
  fi

  echo "$decompressed|$wrapped"
}

# ── Merge files per app and write SHS-ready output ───────────
prepare_for_shs() {
  log "Preparing logs for Spark History Server in $OUTPUT_DIR ..."
  declare -A app_files

  while IFS= read -r -d '' raw; do
    local app_key
    app_key=$(dirname "$raw" | sed "s|^$RAW_DIR/?||")
    [[ -z "$app_key" ]] && app_key="root"
    app_files["$app_key"]+="$raw "
  done < <(find "$RAW_DIR" -type f \
    \( -name "*.log.gz" -o -name "*.log.lz4" -o -name "*.log" -o -name "*.lz4" -o -name "*.gz" \) \
    -print0 | sort -z)

  if [[ ${#app_files[@]} -eq 0 ]]; then
    warn "No raw files to process in $RAW_DIR"
    return 0
  fi

  for app_key in "${!app_files[@]}"; do
    local app_name out_file tmp_merged file_count event_count
    app_name=$(echo "$app_key" | awk -F'/' '{n=NF; if(n>=2) print $(n-1)"_"$n; else print $n}')
    out_file="$OUTPUT_DIR/$app_name"
    tmp_merged="/tmp/merged_$$_${RANDOM}"
    > "$tmp_merged"
    file_count=0; event_count=0

    log "Merging: $app_key"
    for raw in ${app_files[$app_key]}; do
      local result decompressed wrapped lines count
      result=$(process_file "$raw") || continue
      decompressed=$(echo "$result" | cut -d'|' -f1)
      wrapped=$(echo "$result" | cut -d'|' -f2)
      lines=$(extract_spark_lines "$decompressed" "$wrapped") || true
      count=$(echo "$lines" | grep -c . || true)

      if [[ $count -gt 0 ]]; then
        echo "$lines" >> "$tmp_merged"
        event_count=$(( event_count + count ))
        file_count=$(( file_count + 1 ))
        log "  + $(basename "$raw"): $count events"
      else
        warn "  - $(basename "$raw"): 0 events — first lines: $(head -3 "$decompressed")"
      fi
      rm -f "$decompressed"
    done

    if [[ $event_count -gt 0 ]]; then
      mv "$tmp_merged" "$out_file"
      log "✓ $out_file ($file_count files, $event_count events)"
      log "  First event: $(head -1 "$out_file" | jq -r '.Event' 2>/dev/null || echo 'unknown')"
    else
      warn "✗ $app_name: no valid Spark events — skipping"
      rm -f "$tmp_merged"
    fi
  done

  log "Output directory:"
  find "$OUTPUT_DIR" -type f -exec ls -lh {} \;
  # Make files readable by all users (Spark runs as different UID)
  chmod -R a+r "$OUTPUT_DIR"
  # Make directories executable
  find "$OUTPUT_DIR" -type d -exec chmod a+x {} \;
  # Sync filesystem to ensure permissions are written
  sync
  [[ "$KEEP_RAW" != "true" ]] && { log "Cleaning raw files..."; rm -rf "${RAW_DIR:?}"/*; }
}

# ── Main ──────────────────────────────────────────────────────
main() {
  log "═══════════════════════════════════════════════════"
  log " Spark SHS Log Preparer"
  log " Bucket  : oci://$OCI_NAMESPACE/$OCI_BUCKET"
  log " Prefix  : ${OCI_PREFIX:-(none)}"
  log " Output  : $OUTPUT_DIR"
  log " Auth    : $OCI_CLI_AUTH  profile=${OCI_PROFILE}"
  log " Watch   : $WATCH_MODE   interval=${POLL_INTERVAL}s"
  log " FB wrap : $FLUENT_BIT_WRAPPED"
  log "═══════════════════════════════════════════════════"

  check_auth

  if [[ "$WATCH_MODE" == "true" ]]; then
    log "Watch mode: polling every ${POLL_INTERVAL}s"
    while true; do
      check_token_expiry
      download_logs
      prepare_for_shs
      log "Sleeping ${POLL_INTERVAL}s..."
      sleep "$POLL_INTERVAL"
    done
  else
    download_logs
    prepare_for_shs
    log "Done."
  fi
}

main "$@"
