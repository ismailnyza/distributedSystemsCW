#!/bin/bash
set -euo pipefail # Exit on error, unset variable, or pipe failure

# Script to set up the full development environment:
# 1. Build the Java application.
# 2. Start etcd and ZooKeeper using dev_services.sh.
# 3. (Optionally) Seed etcd.
# 4. Start three instances of the ConcertNode application.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
#PROJECT_ROOT_DIR="$(cd "${SCRIPT_DIR}/../../.." && pwd)" # Assumes this script is in src/main/resources/scripts
PROJECT_ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)" # Go up one more level from src to untitled
DEV_SERVICES_SCRIPT="${SCRIPT_DIR}/dev_services.sh"      # dev_services.sh is in the same directory

# Default ports for etcd and application nodes
# This ETCD_PORT is what we tell dev_services.sh to use for the HOST mapping of etcd's client port.
# It MUST match what's in your application's config.properties (etcd.endpoints)
DEFAULT_ETCD_HOST_PORT=12379
NODE1_PORT=50051
NODE2_PORT=50052
NODE3_PORT=50053
LOGS_DIR="${PROJECT_ROOT_DIR}/logs" # Centralized logs directory

# --- Helper function to check if a command exists ---
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# --- Check for required tools ---
check_tool() {
    if ! command_exists "$1"; then
        echo "Error: Required command '$1' not found. Please install it and ensure it's in your PATH. Exiting."
        exit 1
    fi
}
check_tool "mvn"
check_tool "java"
check_tool "docker"
# 'nc' check is now in dev_services.sh, but you could also check here

# --- 1. Clean and Build the Maven Project ---
echo ">>> Building the Concert Ticket System application in ${PROJECT_ROOT_DIR}..."
cd "${PROJECT_ROOT_DIR}" || { echo "Failed to change to project root directory. Exiting."; exit 1; }

mvn clean package -DskipTests
if [ $? -ne 0 ]; then
    echo ">>> Maven build failed. Exiting."
    exit 1
fi

# Dynamic JAR path detection
APP_JAR_CANDIDATE=$(find "${PROJECT_ROOT_DIR}/target" -name "concert-ticket-system-v2-*.jar" ! -name "*-sources.jar" ! -name "*-javadoc.jar" -print -quit)
if [ -z "${APP_JAR_CANDIDATE}" ] || [ ! -f "${APP_JAR_CANDIDATE}" ]; then
    echo ">>> Error: Application JAR not found in ${PROJECT_ROOT_DIR}/target after build. Exiting."
    exit 1
fi
APP_JAR_PATH="${APP_JAR_CANDIDATE}"
echo ">>> Maven build successful. Using JAR: ${APP_JAR_PATH}"
echo ""

# --- 2. Start External Services (etcd and ZooKeeper) ---
echo ">>> Starting etcd and ZooKeeper services using host port ${DEFAULT_ETCD_HOST_PORT} for etcd..."
if [ ! -f "${DEV_SERVICES_SCRIPT}" ]; then
    echo ">>> Error: dev_services.sh not found at ${DEV_SERVICES_SCRIPT}. Exiting."
    exit 1
fi
bash "${DEV_SERVICES_SCRIPT}" restart "${DEFAULT_ETCD_HOST_PORT}"
if [ $? -ne 0 ]; then
    echo ">>> Failed to start etcd/ZooKeeper services using dev_services.sh. Exiting."
    exit 1
fi
echo ">>> etcd and ZooKeeper should be starting/started."
echo ""

# --- 3. (Optional) Seed etcd ---
# Uncomment to automatically seed etcd. Make sure dev_services.sh can find config.properties.
# echo ">>> Seeding etcd with initial configurations..."
# bash "${DEV_SERVICES_SCRIPT}" seed_etcd "${DEFAULT_ETCD_HOST_PORT}"
# if [ $? -ne 0 ]; then
#     echo ">>> Warning: Failed to seed etcd."
# fi
# echo ""

# --- 4. Start Application Nodes ---
echo ">>> Starting ConcertNode instances..."

# Check if screen or tmux is available for running background processes
TERMINAL_MULTIPLEXER=""
if command_exists screen; then
    TERMINAL_MULTIPLEXER="screen"
    echo "Using screen for background processes."
elif command_exists tmux; then
    TERMINAL_MULTIPLEXER="tmux"
    echo "Using tmux for background processes."
else
    echo "Neither screen nor tmux found. Will use nohup for background processes."
fi

mkdir -p "${LOGS_DIR}"

start_node() {
    local node_id=$1
    local grpc_port=$2
    local log_file="${LOGS_DIR}/node-${node_id}.log"

    echo "Starting ${node_id} on host localhost, gRPC port ${grpc_port}, logging to ${log_file}"
    # Command to run the Java application
    # Assumes config.properties uses http://localhost:${DEFAULT_ETCD_HOST_PORT}
    JAVA_CMD="java -Dconfig.file=${PROJECT_ROOT_DIR}/src/main/resources/config.properties -jar ${APP_JAR_PATH} ${node_id} localhost ${grpc_port}"

    # Kill existing process if any (more robust than just screen/tmux kill)
    # This is a bit aggressive for a dev script, but can prevent stale processes.
    # pgrep -f "java -jar .*/${APP_JAR_PATH_BASENAME}.*${node_id}" | xargs -r kill -9
    # sleep 1

    if [ "$TERMINAL_MULTIPLEXER" == "screen" ]; then
        if screen -ls | grep -q "\.${node_id}\."; then
            echo "Screen session for ${node_id} already exists. Killing it first."
            screen -S "${node_id}" -X quit
            sleep 1
        fi
        # Log to screen's own logfile AND redirect stdout/stderr for safety
        screen -L -Logfile "${log_file}" -dmS "${node_id}" bash -c "${JAVA_CMD} > '${log_file}' 2>&1"
        echo "   ${node_id} started in screen session '${node_id}'. Main log: ${log_file}"
        echo "   Attach with: screen -r ${node_id}"
    elif [ "$TERMINAL_MULTIPLEXER" == "tmux" ]; then
        if tmux has-session -t "${node_id}" 2>/dev/null; then
            echo "tmux session for ${node_id} already exists. Killing it first."
            tmux kill-session -t "${node_id}"
            sleep 1
        fi
        tmux new-session -d -s "${node_id}" "${JAVA_CMD} > '${log_file}' 2>&1"
        echo "   ${node_id} started in tmux session '${node_id}'. Main log: ${log_file}"
        echo "   Attach with: tmux attach -t ${node_id}"
    else
        # Fallback: Start in background, redirect output to log file
        # Ensure any previous nohup process for this node is killed
        # This is tricky with nohup, consider using PIDs. For simplicity here, it won't kill previous.
        nohup ${JAVA_CMD} > "${log_file}" 2>&1 &
        local pid=$!
        echo "${pid}" > "${LOGS_DIR}/node-${node_id}.pid"
        echo "   ${node_id} started in background (PID ${pid}). Log: ${log_file}"
    fi
}

# Start the three nodes
start_node "node-1" ${NODE1_PORT}
sleep 3 # Stagger starts slightly more
start_node "node-2" ${NODE2_PORT}
sleep 3
start_node "node-3" ${NODE3_PORT}

echo ""
echo ">>> All ConcertNode instances have been launched."
echo ">>> Check individual log files in the '${LOGS_DIR}/' directory."
echo ">>> Use '${DEV_SERVICES_SCRIPT} status ${DEFAULT_ETCD_HOST_PORT}' to check etcd/ZK."
echo ">>> To stop everything: "
echo "    1. Run '${DEV_SERVICES_SCRIPT} stop ${DEFAULT_ETCD_HOST_PORT}'"
echo "    2. Manually kill the Java processes or use screen/tmux commands to kill sessions (or use PIDs if nohup was used):"
echo "       If using screen: screen -S node-1 -X quit; screen -S node-2 -X quit; screen -S node-3 -X quit"
echo "       If using tmux: tmux kill-session -t node-1; tmux kill-session -t node-2; tmux kill-session -t node-3"
echo "       If using nohup: (example to kill based on PIDs saved)"
echo "         for pid_file in ${LOGS_DIR}/node-*.pid; do if [ -f \"\$pid_file\" ]; then kill \"\$(cat \"\$pid_file\")\"; rm \"\$pid_file\"; fi; done"
