#!/bin/bash

# --- Configuration ---
ETCD_CONTAINER_NAME="etcd-W2151443-v3.3" # Changed name to reflect version
ETCD_IMAGE="quay.io/coreos/etcd:v3.3.27" # Using a specific v3.3.x version

# === Port Configuration ===
# Define the HOST ports you want etcd to be accessible on.
DEFAULT_HOST_CLIENT_PORT=2381 # Your desired host port for client
DEFAULT_HOST_PEER_PORT=2382   # Your desired host port for peer

# Override with environment variables if set, otherwise use defaults
HOST_CLIENT_PORT=${ETCD_HOST_CLIENT_PORT:-$DEFAULT_HOST_CLIENT_PORT}
HOST_PEER_PORT=${ETCD_HOST_PEER_PORT:-$DEFAULT_HOST_PEER_PORT}

# Internal ports etcd listens on INSIDE the container
# For v3.3 with v2 enabled, it should still primarily use 2379/2380 for its native v3 ops,
# but the v2 API would be accessible via the client port.
ETCD_INTERNAL_CLIENT_PORT=2379 # etcd's default v3 client port
ETCD_INTERNAL_PEER_PORT=2380   # etcd's default v3 peer port

# --- Command Line Argument for Cleaning ---
DO_CLEANUP=false
if [[ "$1" == "--clean" || "$1" == "clean" ]]; then
    DO_CLEANUP=true
    echo "Cleanup mode: Will attempt to stop and remove existing container '$ETCD_CONTAINER_NAME'."
fi

# --- Sudo Check ---
DOCKER_CMD="docker"
LSOF_CMD="lsof"
if [ "$EUID" -ne 0 ]; then
  # Check if sudo is actually needed for docker, only if direct command fails
  if ! command -v docker > /dev/null || ! docker ps > /dev/null 2>&1; then
    echo "Warning: Docker command failed. Will try with sudo."
    DOCKER_CMD="sudo docker"
    LSOF_CMD="sudo lsof" # If docker needs sudo, lsof likely will too for full visibility
  else
    echo "Running docker commands as current user."
  fi
else
  echo "Running as root. Sudo not prepended for docker/lsof."
fi
# Final check for DOCKER_CMD
if ! $DOCKER_CMD ps > /dev/null 2>&1; then
    echo "Error: Docker command '$DOCKER_CMD' is not working. Is Docker installed and the daemon running correctly?"
    exit 1
fi


# --- Sanity Checks & Cleanup ---
is_port_in_use() {
    local port_to_check=$1
    if $LSOF_CMD -Pi :"$port_to_check" -sTCP:LISTEN -t >/dev/null ; then
        return 0
    else
        return 1
    fi
}

EXISTING_CONTAINER_ID=$($DOCKER_CMD ps -a --filter "name=^/${ETCD_CONTAINER_NAME}$" --format "{{.ID}}")
if [ ! -z "$EXISTING_CONTAINER_ID" ]; then
    IS_RUNNING=$($DOCKER_CMD ps --filter "id=$EXISTING_CONTAINER_ID" --filter "status=running" --format "{{.ID}}")
    if [ "$DO_CLEANUP" = true ]; then
        echo "Cleanup requested: Stopping and removing container '$ETCD_CONTAINER_NAME' (ID: $EXISTING_CONTAINER_ID)..."
        $DOCKER_CMD stop "$EXISTING_CONTAINER_ID" >/dev/null
        $DOCKER_CMD rm "$EXISTING_CONTAINER_ID" >/dev/null
        echo "Container '$ETCD_CONTAINER_NAME' removed."
        EXISTING_CONTAINER_ID=""
    elif [ -z "$IS_RUNNING" ]; then
        echo "Container '$ETCD_CONTAINER_NAME' (ID: $EXISTING_CONTAINER_ID) exists but is not running. Removing it..."
        $DOCKER_CMD rm "$EXISTING_CONTAINER_ID" >/dev/null
        echo "Stopped container '$ETCD_CONTAINER_NAME' removed."
        EXISTING_CONTAINER_ID=""
    else
        echo "Container '$ETCD_CONTAINER_NAME' (ID: $EXISTING_CONTAINER_ID) is already running on your chosen ports."
        echo "Client URL should be http://localhost:${HOST_CLIENT_PORT}"
        echo "If you need to restart or ensure a clean state, run with the '--clean' argument."
        exit 0
    fi
fi

# --- Start etcd Container ---
echo "Checking required host ports for new container: $HOST_CLIENT_PORT (client), $HOST_PEER_PORT (peer)..."
if is_port_in_use $HOST_CLIENT_PORT; then
    echo "Error: Host port $HOST_CLIENT_PORT for etcd client is already in use by another process."
    exit 1
fi
if is_port_in_use $HOST_PEER_PORT; then
    echo "Error: Host port $HOST_PEER_PORT for etcd peer is already in use by another process."
    exit 1
fi
echo "Ports $HOST_CLIENT_PORT and $HOST_PEER_PORT are available on the host for the new container."

echo "Starting new etcd container '$ETCD_CONTAINER_NAME' with etcd image $ETCD_IMAGE..."
echo "Mapping host port $HOST_CLIENT_PORT to container port $ETCD_INTERNAL_CLIENT_PORT (client)"
echo "Mapping host port $HOST_PEER_PORT to container port $ETCD_INTERNAL_PEER_PORT (peer)"

# For etcd v3.3, the flags are slightly different for a single node,
# and we rely on its default v2 emulation via the gRPC gateway.
# The key is that the client port (2379 internally) should expose the v2 API via the gateway.
$DOCKER_CMD run -d \
    -p "${HOST_CLIENT_PORT}:${ETCD_INTERNAL_CLIENT_PORT}" \
    -p "${HOST_PEER_PORT}:${ETCD_INTERNAL_PEER_PORT}" \
    --name "$ETCD_CONTAINER_NAME" \
    "$ETCD_IMAGE" /usr/local/bin/etcd \
    --name s1 \
    --data-dir /etcd-data \
    --listen-client-urls "http://0.0.0.0:${ETCD_INTERNAL_CLIENT_PORT}" \
    --advertise-client-urls "http://0.0.0.0:${ETCD_INTERNAL_CLIENT_PORT}" \
    --listen-peer-urls "http://0.0.0.0:${ETCD_INTERNAL_PEER_PORT}" \
    --initial-advertise-peer-urls "http://0.0.0.0:${ETCD_INTERNAL_PEER_PORT}" \
    --initial-cluster "s1=http://0.0.0.0:${ETCD_INTERNAL_PEER_PORT}" \
    --initial-cluster-token "etcd-cluster-W2151443-v3.3-$(date +%s)" \
    --initial-cluster-state new
    # No explicit --enable-v2 flag typically needed for v3.3.x gRPC gateway for v2 path emulation,
    # as it was often on by default if the gateway was active.

echo "Waiting a few seconds for etcd to initialize..."
sleep 7 # Give it a bit more time

# --- Verification ---
if $DOCKER_CMD ps --filter "name=^/${ETCD_CONTAINER_NAME}$" --filter "status=running" --format "{{.ID}}" | grep -q .; then
    echo "---------------------------------------------------------------------"
    echo "etcd container '$ETCD_CONTAINER_NAME' (version $ETCD_IMAGE) started successfully."
    echo "Your applications should connect to etcd at: http://localhost:${HOST_CLIENT_PORT}"
    echo "This version *should* support the v2 API at /v2/keys/ on the client port."
    echo "---------------------------------------------------------------------"
    echo "To check etcd logs: $DOCKER_CMD logs $ETCD_CONTAINER_NAME"
    echo "Test v2 API with: curl -L http://localhost:${HOST_CLIENT_PORT}/v2/keys/mytestkey -XPUT -d value=testValue"
    echo "Test v3 API with etcdctl (if installed): ETCDCTL_API=3 etcdctl --endpoints=http://localhost:${HOST_CLIENT_PORT} endpoint health --write-out=table"
    echo "---------------------------------------------------------------------"
else
    echo "---------------------------------------------------------------------"
    echo "Error: etcd container '$ETCD_CONTAINER_NAME' failed to start or is not running."
    echo "Check Docker logs for details: $DOCKER_CMD logs $ETCD_CONTAINER_NAME"
    echo "---------------------------------------------------------------------"
    exit 1
fi
