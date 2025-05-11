#!/bin/bash
set -euo pipefail # Exit on error, unset variable, or pipe failure

# Simple dev server script for etcd and ZooKeeper using 'sudo docker'.
# Usage: ./scripts/dev_services.sh [start|stop|status|logs|restart|clean|seed_etcd] [etcd_port]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

ETCD_CONTAINER_NAME_BASE="concert_etcd_dev"
ETCD_IMAGE="quay.io/coreos/etcd:v3.5.13"
DEFAULT_ETCD_CLIENT_PORT=2379  # << UPDATED TO MATCH YOUR config.properties
ETCD_PEER_PORT_INTERNAL=2380  # Internal etcd peer port

ZK_CONTAINER_NAME="concert_zk_dev"
ZK_IMAGE="zookeeper:3.6.2" # Or your preferred ZK version like 3.8 or 3.9
ZK_CLIENT_PORT=2181

ACTION=$1
ARG2=$2 # Could be an alternative etcd port or a service name for specific actions

# Determine ETCD client port to use
ETCD_CLIENT_PORT_TO_USE=$DEFAULT_ETCD_CLIENT_PORT
if [[ "$ACTION" == "start" || "$ACTION" == "restart" ]] && [[ ! -z "$ARG2" && "$ARG2" =~ ^[0-9]+$ ]]; then
    ETCD_CLIENT_PORT_TO_USE=$ARG2
    echo "User specified etcd client port: $ETCD_CLIENT_PORT_TO_USE"
fi
# Adjust container name if custom port is used for etcd, to allow multiple instances for testing
ETCD_ADJUSTED_CONTAINER_NAME="${ETCD_CONTAINER_NAME_BASE}_${ETCD_CLIENT_PORT_TO_USE}"


if [ -z "$ACTION" ]; then
    echo "Usage: $0 [start|stop|status|logs|restart|clean|seed_etcd] [optional_etcd_port_for_start_restart]"
    exit 1
fi

# --- Function to check if a command exists ---
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# --- Function to stop and remove a container ---
cleanup_container() {
    local container_name=$1
    echo "Checking for existing container: $container_name"
    if sudo docker ps -a -f "name=^/${container_name}$" --format "{{.Names}}" | grep -q "^${container_name}$"; then
        echo "Stopping and removing existing container: $container_name..."
        sudo docker stop "${container_name}" > /dev/null 2>&1 || echo "Failed to stop ${container_name}, it might have already been stopped."
        sudo docker rm "${container_name}" > /dev/null 2>&1 || echo "Failed to remove ${container_name}, it might have already been removed."
        echo "Container $container_name removed/cleanup attempted."
    else
        echo "Container $container_name not found, no cleanup needed for it."
    fi
}

# --- Clean all relevant Docker containers ---
if [ "$ACTION" == "clean" ]; then
    echo "Cleaning up all related Docker containers..."
    # Attempt to clean based on the potentially adjusted name (if port was given)
    # And also clean any other etcd dev containers this script might have started with other ports
    # by iterating common ports or using a broader filter if necessary.
    # For now, just cleaning the specific one and the default base.
    DEFAULT_ETCD_ADJUSTED_CONTAINER_NAME="${ETCD_CONTAINER_NAME_BASE}_${DEFAULT_ETCD_CLIENT_PORT}"
    if [ "${ETCD_ADJUSTED_CONTAINER_NAME}" != "${DEFAULT_ETCD_ADJUSTED_CONTAINER_NAME}" ]; then
         cleanup_container "${DEFAULT_ETCD_ADJUSTED_CONTAINER_NAME}"
    fi
    cleanup_container "${ETCD_ADJUSTED_CONTAINER_NAME}"

    # More aggressive cleanup for any container starting with ETCD_CONTAINER_NAME_BASE
    echo "Aggressively cleaning any container starting with '${ETCD_CONTAINER_NAME_BASE}'..."
    MATCHING_ETCD_CONTAINERS=$(sudo docker ps -a --filter "name=^/${ETCD_CONTAINER_NAME_BASE}" --format "{{.Names}}")
    if [ -n "$MATCHING_ETCD_CONTAINERS" ]; then
        echo "$MATCHING_ETCD_CONTAINERS" | xargs -r sudo docker stop > /dev/null 2>&1
        echo "$MATCHING_ETCD_CONTAINERS" | xargs -r sudo docker rm > /dev/null 2>&1
        echo "All containers matching '${ETCD_CONTAINER_NAME_BASE}*' stopped and removed."
    else
        echo "No containers found matching '${ETCD_CONTAINER_NAME_BASE}*'."
    fi

    cleanup_container "${ZK_CONTAINER_NAME}"
    echo "Cleanup attempt complete."
    exit 0
fi


# --- Start Services ---
if [ "$ACTION" == "start" ]; then
    echo "--- Starting etcd (${ETCD_ADJUSTED_CONTAINER_NAME}) ---"
    cleanup_container "${ETCD_ADJUSTED_CONTAINER_NAME}" # Ensure clean start

    ETCD_CLIENT_URL_INTERNAL="http://0.0.0.0:2379" # etcd listens on this port *inside* the container
    # Peer port is ETCD_CLIENT_PORT_TO_USE + 1 on host, mapping to ETCD_PEER_PORT_INTERNAL inside container
    HOST_PEER_PORT=$((ETCD_CLIENT_PORT_TO_USE + 1))

    echo "Starting etcd container '${ETCD_ADJUSTED_CONTAINER_NAME}' on host client port ${ETCD_CLIENT_PORT_TO_USE}:2379 and host peer port ${HOST_PEER_PORT}:${ETCD_PEER_PORT_INTERNAL}"
    sudo docker run -d \
        -p "${ETCD_CLIENT_PORT_TO_USE}:2379" \
        -p "${HOST_PEER_PORT}:${ETCD_PEER_PORT_INTERNAL}" \
        --name "${ETCD_ADJUSTED_CONTAINER_NAME}" \
        "${ETCD_IMAGE}" \
        etcd \
        --name "etcd-node-for-${ETCD_CLIENT_PORT_TO_USE}" \
        --initial-advertise-peer-urls "http://0.0.0.0:${ETCD_PEER_PORT_INTERNAL}" \
        --listen-peer-urls "http://0.0.0.0:${ETCD_PEER_PORT_INTERNAL}" \
        --listen-client-urls "${ETCD_CLIENT_URL_INTERNAL}" \
        --advertise-client-urls "${ETCD_CLIENT_URL_INTERNAL}" \
        --initial-cluster "etcd-node-for-${ETCD_CLIENT_PORT_TO_USE}=http://0.0.0.0:${ETCD_PEER_PORT_INTERNAL}" \
        --initial-cluster-state new \
        --data-dir /etcd-data
    echo "Waiting for etcd to initialize..."
    sleep 5 # Give etcd more time to start

    echo ""
    echo "--- Starting ZooKeeper (${ZK_CONTAINER_NAME}) ---"
    cleanup_container "${ZK_CONTAINER_NAME}" # Ensure clean start

    echo "Starting ZooKeeper container '${ZK_CONTAINER_NAME}' on host port ${ZK_CLIENT_PORT}:2181"
    sudo docker run -d \
        -p "${ZK_CLIENT_PORT}:2181" \
        --name "${ZK_CONTAINER_NAME}" \
        "${ZK_IMAGE}"
    echo "Waiting for ZooKeeper to initialize..."
    sleep 5 # Give ZK time

    echo ""
    echo "Services startup initiated. Use '$0 status' to check."

# --- Stop Services ---
elif [ "$ACTION" == "stop" ]; then
    echo "--- Stopping etcd (${ETCD_ADJUSTED_CONTAINER_NAME}) ---"
    cleanup_container "${ETCD_ADJUSTED_CONTAINER_NAME}"
    echo "--- Stopping ZooKeeper (${ZK_CONTAINER_NAME}) ---"
    cleanup_container "${ZK_CONTAINER_NAME}"

# --- Status of Services ---
elif [ "$ACTION" == "status" ]; then
    echo "--- Status of etcd (${ETCD_ADJUSTED_CONTAINER_NAME}) ---"
    if sudo docker ps -f "name=^/${ETCD_ADJUSTED_CONTAINER_NAME}$" --format "{{.Names}}" | grep -q "^${ETCD_ADJUSTED_CONTAINER_NAME}$"; then
        sudo docker ps -f "name=^/${ETCD_ADJUSTED_CONTAINER_NAME}$"
        echo "Health check (may take a moment if etcd is still starting):"
        if ! sudo docker exec "${ETCD_ADJUSTED_CONTAINER_NAME}" etcdctl endpoint health --cluster; then
            echo "etcd health check failed. Container logs:"
            sudo docker logs --tail 20 "${ETCD_ADJUSTED_CONTAINER_NAME}"
        fi
    else
        echo "Container '${ETCD_ADJUSTED_CONTAINER_NAME}' not found or not running."
    fi
    echo ""
    echo "--- Status of ZooKeeper (${ZK_CONTAINER_NAME}) ---"
    if sudo docker ps -f "name=^/${ZK_CONTAINER_NAME}$" --format "{{.Names}}" | grep -q "^${ZK_CONTAINER_NAME}$"; then
        sudo docker ps -f "name=^/${ZK_CONTAINER_NAME}$"
        echo "Checking ZK status (ruok):"
        if command_exists nc; then
            echo "ruok" | nc -w 2 localhost "${ZK_CLIENT_PORT}" || echo "Failed to connect to ZK via nc, or ZK not healthy."
        else
            echo "Warning: 'nc' (netcat) command not found. Cannot perform ZK health check via 'ruok'."
        fi
    else
        echo "Container '${ZK_CONTAINER_NAME}' not found or not running."
    fi

# --- Logs of a Service ---
elif [ "$ACTION" == "logs" ]; then
    SERVICE_TO_LOG=$ARG2
    if [ -z "$SERVICE_TO_LOG" ]; then
        echo "Please specify which service logs to view: 'etcd' or 'zk'"
        echo "Example: $0 logs etcd  (uses etcd port ${ETCD_CLIENT_PORT_TO_USE} for container name)"
        echo "         $0 logs zk"
        exit 1
    fi
    if [ "$SERVICE_TO_LOG" == "etcd" ]; then
        echo "Tailing logs for etcd container '${ETCD_ADJUSTED_CONTAINER_NAME}'..."
        sudo docker logs -f "${ETCD_ADJUSTED_CONTAINER_NAME}"
    elif [ "$SERVICE_TO_LOG" == "zk" ]; then
        echo "Tailing logs for ZooKeeper container '${ZK_CONTAINER_NAME}'..."
        sudo docker logs -f "${ZK_CONTAINER_NAME}"
    else
        echo "Unknown service for logs: '$SERVICE_TO_LOG'. Use 'etcd' or 'zk'."
    fi

# --- Restart Services ---
elif [ "$ACTION" == "restart" ]; then
    echo "--- Restarting all services ---"
    ETCD_PORT_FOR_RESTART=$DEFAULT_ETCD_CLIENT_PORT
     # ARG2 here is the optional etcd_port
    if [[ ! -z "$ARG2" && "$ARG2" =~ ^[0-9]+$ ]]; then
        ETCD_PORT_FOR_RESTART=$ARG2
    fi
    # Call stop action (it will use ETCD_ADJUSTED_CONTAINER_NAME which is derived from ETCD_CLIENT_PORT_TO_USE)
    bash "$0" stop "$ETCD_CLIENT_PORT_TO_USE" # Pass current port to stop correctly
    bash "$0" start "$ETCD_PORT_FOR_RESTART"


# --- Seed etcd with some initial config keys ---
elif [ "$ACTION" == "seed_etcd" ]; then
    if ! sudo docker ps -f "name=^/${ETCD_ADJUSTED_CONTAINER_NAME}$" --format "{{.Names}}" | grep -q "^${ETCD_ADJUSTED_CONTAINER_NAME}$"; then
        echo "etcd container '${ETCD_ADJUSTED_CONTAINER_NAME}' is not running. Please start it first with: $0 start ${ETCD_CLIENT_PORT_TO_USE}"
        exit 1
    fi
    echo "Seeding etcd container '${ETCD_ADJUSTED_CONTAINER_NAME}' with initial config values..."

    # Robust path to config.properties relative to this script
    CONFIG_FILE_PATH="${SCRIPT_DIR}/../config.properties"
    if [ ! -f "$CONFIG_FILE_PATH" ]; then
        echo "Error: config.properties not found at expected location: $CONFIG_FILE_PATH"
        # Try searching in a common alternative location if the script was moved
        ALT_CONFIG_PATH="${SCRIPT_DIR}/../../src/main/resources/config.properties"
        if [ -f "$ALT_CONFIG_PATH" ]; then
             echo "Found config at alternative location: $ALT_CONFIG_PATH"
             CONFIG_FILE_PATH=$ALT_CONFIG_PATH
        else
            echo "Cannot find config.properties. Using default base path for etcd seeding."
            CONFIG_BASE_PATH_FROM_FILE="/config/concert_system_v2" # Fallback default
        fi
    fi

    if [ -f "$CONFIG_FILE_PATH" ]; then
        CONFIG_BASE_PATH_FROM_FILE=$(grep '^etcd.config.basepath=' "$CONFIG_FILE_PATH" | cut -d'=' -f2)
    fi


    # Ensure CONFIG_BASE_PATH_FROM_FILE is not empty
    if [ -z "$CONFIG_BASE_PATH_FROM_FILE" ]; then
        echo "Warning: Could not read 'etcd.config.basepath' from config.properties. Using default: /config/concert_system_v2"
        CONFIG_BASE_PATH_FROM_FILE="/config/concert_system_v2" # Fallback default
    fi

    echo "Using etcd config base path: ${CONFIG_BASE_PATH_FROM_FILE}"

    sudo docker exec "${ETCD_ADJUSTED_CONTAINER_NAME}" etcdctl put "${CONFIG_BASE_PATH_FROM_FILE}/transaction_timeout_ms" "10000"
    sudo docker exec "${ETCD_ADJUSTED_CONTAINER_NAME}" etcdctl put "${CONFIG_BASE_PATH_FROM_FILE}/etcd_service_ttl_seconds" "15"
    sudo docker exec "${ETCD_ADJUSTED_CONTAINER_NAME}" etcdctl put "${CONFIG_BASE_PATH_FROM_FILE}/some_other_dynamic_config" "initial_value"

    echo "etcd seeded. Verify with:"
    echo "sudo docker exec ${ETCD_ADJUSTED_CONTAINER_NAME} etcdctl get ${CONFIG_BASE_PATH_FROM_FILE}/ --prefix"

else
    echo "Unknown action: '$ACTION'. Use: start, stop, status, logs, restart, clean, or seed_etcd."
    exit 1
fi

exit 0
