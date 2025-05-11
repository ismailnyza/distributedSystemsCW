#!/bin/bash

# Simple dev server script for etcd and ZooKeeper using 'sudo docker'.
# Usage: ./scripts/dev_services.sh [start|stop|status|logs|restart|clean|seed_etcd] [etcd_port]

ETCD_CONTAINER_NAME="concert_etcd_dev"
ETCD_IMAGE="quay.io/coreos/etcd:v3.5.13"
DEFAULT_ETCD_CLIENT_PORT=6920 # Your chosen default
ETCD_PEER_PORT_INTERNAL=2380  # Internal etcd peer port

ZK_CONTAINER_NAME="concert_zk_dev"
ZK_IMAGE="zookeeper:3.6.2" # Or your preferred ZK version like 3.8 or 3.9
ZK_CLIENT_PORT=2181

ACTION=$1
ARG2=$2 # Could be an alternative etcd port or a service name for specific actions

# Determine ETCD client port to use
ETCD_CLIENT_PORT_TO_USE=$DEFAULT_ETCD_CLIENT_PORT
if [[ "$ACTION" == "start" || "$ACTION" == "restart" ]] && [[ "$ARG2" =~ ^[0-9]+$ ]]; then
    ETCD_CLIENT_PORT_TO_USE=$ARG2
    echo "User specified etcd client port: $ETCD_CLIENT_PORT_TO_USE"
fi
# Adjust container name if custom port is used for etcd, to allow multiple instances for testing
ETCD_ADJUSTED_CONTAINER_NAME="${ETCD_CONTAINER_NAME}_${ETCD_CLIENT_PORT_TO_USE}"


if [ -z "$ACTION" ]; then
    echo "Usage: $0 [start|stop|status|logs|restart|clean|seed_etcd] [optional_etcd_port_for_start_restart]"
    exit 1
fi

# --- Function to stop and remove a container ---
cleanup_container() {
    local container_name=$1
    echo "Checking for existing container: $container_name"
    if sudo docker ps -a -f name=^/${container_name}$ --format "{{.Names}}" | grep -q ${container_name}; then
        echo "Stopping and removing existing container: $container_name..."
        sudo docker stop ${container_name} > /dev/null 2>&1
        sudo docker rm ${container_name} > /dev/null 2>&1
        echo "Container $container_name removed."
    else
        echo "Container $container_name not found, no cleanup needed for it."
    fi
}

# --- Clean all relevant Docker containers and potentially volumes ---
if [ "$ACTION" == "clean" ]; then
    echo "Cleaning up all related Docker containers..."
    cleanup_container "${ETCD_ADJUSTED_CONTAINER_NAME}" # Cleans based on current port setting or default
    # If you want to clean etcd containers on ANY port this script might have started:
    # sudo docker ps -a -q --filter "name=^concert_etcd_dev" | xargs -r sudo docker stop > /dev/null 2>&1
    # sudo docker ps -a -q --filter "name=^concert_etcd_dev" | xargs -r sudo docker rm > /dev/null 2>&1
    cleanup_container "${ZK_CONTAINER_NAME}"
    echo "Cleanup attempt complete."
    # Add docker volume prune if you want to remove unused volumes, but be careful with this.
    # echo "To also prune unused Docker volumes, run: sudo docker volume prune"
    exit 0
fi


# --- Start Services ---
if [ "$ACTION" == "start" ]; then
    echo "--- Starting etcd (${ETCD_ADJUSTED_CONTAINER_NAME}) ---"
    cleanup_container "${ETCD_ADJUSTED_CONTAINER_NAME}" # Ensure clean start

    ETCD_CLIENT_URL_INTERNAL="http://0.0.0.0:2379" # etcd listens on this port *inside* the container
    ETCD_ADVERTISE_CLIENT_URL="http://0.0.0.0:2379" # How etcd advertises itself (can be different if complex networking)

    echo "Starting etcd container '${ETCD_ADJUSTED_CONTAINER_NAME}' on host port ${ETCD_CLIENT_PORT_TO_USE} -> container 2379"
    sudo docker run -d \
        -p ${ETCD_CLIENT_PORT_TO_USE}:2379 \
        -p $((ETCD_CLIENT_PORT_TO_USE + 1)):${ETCD_PEER_PORT_INTERNAL} \
        --name "${ETCD_ADJUSTED_CONTAINER_NAME}" \
        ${ETCD_IMAGE} \
        etcd \
        --name etcd-node-for-${ETCD_CLIENT_PORT_TO_USE} \
        --initial-advertise-peer-urls http://0.0.0.0:${ETCD_PEER_PORT_INTERNAL} \
        --listen-peer-urls http://0.0.0.0:${ETCD_PEER_PORT_INTERNAL} \
        --listen-client-urls ${ETCD_CLIENT_URL_INTERNAL} \
        --advertise-client-urls ${ETCD_ADVERTISE_CLIENT_URL} \
        --initial-cluster etcd-node-for-${ETCD_CLIENT_PORT_TO_USE}=http://0.0.0.0:${ETCD_PEER_PORT_INTERNAL} \
        --initial-cluster-state new \
        --data-dir /etcd-data
    sleep 3 # Give it a moment

    echo ""
    echo "--- Starting ZooKeeper (${ZK_CONTAINER_NAME}) ---"
    cleanup_container "${ZK_CONTAINER_NAME}" # Ensure clean start

    echo "Starting ZooKeeper container '${ZK_CONTAINER_NAME}' on host port ${ZK_CLIENT_PORT} -> container 2181"
    sudo docker run -d \
        -p ${ZK_CLIENT_PORT}:2181 \
        --name "${ZK_CONTAINER_NAME}" \
        ${ZK_IMAGE}
    sleep 3 # Give it a moment

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
    if sudo docker ps -f name=^/"${ETCD_ADJUSTED_CONTAINER_NAME}"$ --format "{{.Names}}" | grep -q "${ETCD_ADJUSTED_CONTAINER_NAME}"; then
        sudo docker ps -f name=^/"${ETCD_ADJUSTED_CONTAINER_NAME}"$
        echo "Health check:"
        sudo docker exec "${ETCD_ADJUSTED_CONTAINER_NAME}" etcdctl endpoint health --cluster
    else
        echo "Container '${ETCD_ADJUSTED_CONTAINER_NAME}' not found or not running."
    fi
    echo ""
    echo "--- Status of ZooKeeper (${ZK_CONTAINER_NAME}) ---"
    if sudo docker ps -f name=^/"${ZK_CONTAINER_NAME}"$ --format "{{.Names}}" | grep -q "${ZK_CONTAINER_NAME}"; then
        sudo docker ps -f name=^/"${ZK_CONTAINER_NAME}"$
        echo "Checking ZK status (ruok):"
        echo "ruok" | nc localhost ${ZK_CLIENT_PORT} || echo "Failed to connect to ZK via nc"
    else
        echo "Container '${ZK_CONTAINER_NAME}' not found or not running."
    fi

# --- Logs of a Service ---
elif [ "$ACTION" == "logs" ]; then
    SERVICE_TO_LOG=$ARG2
    if [ -z "$SERVICE_TO_LOG" ]; then
        echo "Please specify which service logs to view: 'etcd' or 'zk'"
        echo "Example: $0 logs etcd [uses etcd port ${ETCD_CLIENT_PORT_TO_USE}]"
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
    if [[ "$ARG2" =~ ^[0-9]+$ ]]; then
        ETCD_PORT_FOR_RESTART=$ARG2
    fi
    "$0" stop # This will use the current ETCD_ADJUSTED_CONTAINER_NAME (based on default or previous arg)
    "$0" start "$ETCD_PORT_FOR_RESTART"

# --- Seed etcd with some initial config keys ---
elif [ "$ACTION" == "seed_etcd" ]; then
    if ! sudo docker ps -f name=^/"${ETCD_ADJUSTED_CONTAINER_NAME}"$ --format "{{.Names}}" | grep -q "${ETCD_ADJUSTED_CONTAINER_NAME}"; then
        echo "etcd container '${ETCD_ADJUSTED_CONTAINER_NAME}' is not running. Please start it first with: $0 start [etcd_port]"
        exit 1
    fi
    echo "Seeding etcd container '${ETCD_ADJUSTED_CONTAINER_NAME}' with initial config values..."
    CONFIG_BASE_PATH_FROM_FILE=$(grep '^etcd.config.basepath=' src/main/resources/config.properties | cut -d'=' -f2)

    # Ensure CONFIG_BASE_PATH_FROM_FILE is not empty
    if [ -z "$CONFIG_BASE_PATH_FROM_FILE" ]; then
        echo "Error: Could not read 'etcd.config.basepath' from config.properties. Using default."
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
