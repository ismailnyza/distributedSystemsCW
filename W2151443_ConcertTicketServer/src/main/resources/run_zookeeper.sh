#!/bin/bash

ZK_CONTAINER_NAME="zookeeper-W2151443"
ZK_IMAGE="zookeeper:3.9" # Or your chosen version
HOST_CLIENT_PORT=2181  # Standard ZooKeeper client port

DOCKER_CMD="docker"
LSOF_CMD="lsof"
if [ "$EUID" -ne 0 ]; then
  if ! command -v docker > /dev/null || ! docker ps > /dev/null 2>&1; then
    echo "Warning: Docker command failed. Will try with sudo."
    DOCKER_CMD="sudo docker"
    LSOF_CMD="sudo lsof"
  fi
fi
if ! $DOCKER_CMD ps > /dev/null 2>&1; then
    echo "Error: Docker command '$DOCKER_CMD' is not working. Is Docker installed and the daemon running correctly?"
    exit 1
fi

is_port_in_use() {
    local port_to_check=$1
    if $LSOF_CMD -Pi :"$port_to_check" -sTCP:LISTEN -t >/dev/null ; then
        return 0
    else
        return 1
    fi
}

DO_CLEANUP=false
if [[ "$1" == "--clean" || "$1" == "clean" ]]; then
    DO_CLEANUP=true
fi

EXISTING_CONTAINER_ID=$($DOCKER_CMD ps -a --filter "name=^/${ZK_CONTAINER_NAME}$" --format "{{.ID}}")
if [ ! -z "$EXISTING_CONTAINER_ID" ]; then
    IS_RUNNING=$($DOCKER_CMD ps --filter "id=$EXISTING_CONTAINER_ID" --filter "status=running" --format "{{.ID}}")
    if [ "$DO_CLEANUP" = true ]; then
        echo "Cleanup requested: Stopping and removing container '$ZK_CONTAINER_NAME' (ID: $EXISTING_CONTAINER_ID)..."
        $DOCKER_CMD stop "$EXISTING_CONTAINER_ID" >/dev/null
        $DOCKER_CMD rm "$EXISTING_CONTAINER_ID" >/dev/null
        echo "Container '$ZK_CONTAINER_NAME' removed."
        EXISTING_CONTAINER_ID=""
    elif [ -z "$IS_RUNNING" ]; then
        echo "Container '$ZK_CONTAINER_NAME' (ID: $EXISTING_CONTAINER_ID) exists but is not running. Removing it..."
        $DOCKER_CMD rm "$EXISTING_CONTAINER_ID" >/dev/null
        echo "Stopped container '$ZK_CONTAINER_NAME' removed."
        EXISTING_CONTAINER_ID=""
    else
        echo "ZooKeeper container '$ZK_CONTAINER_NAME' (ID: $EXISTING_CONTAINER_ID) is already running."
        echo "Connect at: localhost:$HOST_CLIENT_PORT"
        exit 0
    fi
fi

if is_port_in_use $HOST_CLIENT_PORT; then
    echo "Error: Host port $HOST_CLIENT_PORT for ZooKeeper client is already in use."
    echo "Please free it or change HOST_CLIENT_PORT in the script."
    exit 1
fi

echo "Starting ZooKeeper container '$ZK_CONTAINER_NAME' on host port $HOST_CLIENT_PORT..."
$DOCKER_CMD run -d -p "${HOST_CLIENT_PORT}:2181" --name "$ZK_CONTAINER_NAME" "$ZK_IMAGE"

sleep 5 # Give ZooKeeper a moment to start

if $DOCKER_CMD ps --filter "name=^/${ZK_CONTAINER_NAME}$" --filter "status=running" --format "{{.ID}}" | grep -q .; then
    echo "---------------------------------------------------------------------"
    echo "ZooKeeper container '$ZK_CONTAINER_NAME' started successfully."
    echo "Your applications should connect to ZooKeeper at: localhost:${HOST_CLIENT_PORT}"
    echo "---------------------------------------------------------------------"
    echo "To check logs: $DOCKER_CMD logs $ZK_CONTAINER_NAME"
else
    echo "---------------------------------------------------------------------"
    echo "Error: ZooKeeper container '$ZK_CONTAINER_NAME' failed to start."
    echo "Check Docker logs for details: $DOCKER_CMD logs $ZK_CONTAINER_NAME"
    echo "---------------------------------------------------------------------"
    exit 1
fi