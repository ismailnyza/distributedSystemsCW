#!/bin/bash

# Script to set up the full development environment:
# 1. Build the Java application.
# 2. Start etcd and ZooKeeper using dev_services.sh.
# 3. (Optionally) Seed etcd.
# 4. Start three instances of the ConcertNode application.

PROJECT_ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)" # Assumes this script is in a 'scripts' subdirectory
DEV_SERVICES_SCRIPT="${PROJECT_ROOT_DIR}/scripts/dev_services.sh"
APP_JAR_PATH="${PROJECT_ROOT_DIR}/target/concert-ticket-system-v2-1.0-SNAPSHOT.jar" # Adjust if version/name changes

# Default ports for etcd and application nodes
DEFAULT_ETCD_PORT=6920 # Match your dev_services.sh default if it uses this
NODE1_PORT=50051
NODE2_PORT=50052
NODE3_PORT=50053

# --- Helper function to check if a command exists ---
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# --- 1. Clean and Build the Maven Project ---
echo ">>> Building the Concert Ticket System application..."
cd "${PROJECT_ROOT_DIR}" || { echo "Failed to change to project root directory. Exiting."; exit 1; }
mvn clean package -DskipTests
if [ $? -ne 0 ]; then
    echo ">>> Maven build failed. Exiting."
    exit 1
fi
echo ">>> Maven build successful."
echo ""

# --- 2. Start External Services (etcd and ZooKeeper) ---
echo ">>> Starting etcd and ZooKeeper services..."
if [ ! -f "${DEV_SERVICES_SCRIPT}" ]; then
    echo ">>> Error: dev_services.sh not found at ${DEV_SERVICES_SCRIPT}. Exiting."
    exit 1
fi
# Use the default etcd port defined in this script for consistency
bash "${DEV_SERVICES_SCRIPT}" restart "${DEFAULT_ETCD_PORT}" # restart ensures a clean state
if [ $? -ne 0 ]; then
    echo ">>> Failed to start etcd/ZooKeeper services using dev_services.sh. Exiting."
    exit 1
fi
echo ">>> etcd and ZooKeeper should be starting/started."
echo ""

# --- 3. (Optional) Seed etcd ---
# You can uncomment this section if you want to automatically seed etcd on every full start.
# echo ">>> Seeding etcd with initial configurations..."
# bash "${DEV_SERVICES_SCRIPT}" seed_etcd "${DEFAULT_ETCD_PORT}"
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
elif command_exists tmux; then
    TERMINAL_MULTIPLEXER="tmux"
fi

start_node() {
    local node_id=$1
    local grpc_port=$2
    local log_file="${PROJECT_ROOT_DIR}/logs/node-${node_id}.log" # Log to a file

    mkdir -p "${PROJECT_ROOT_DIR}/logs"

    echo "Starting ${node_id} on port ${grpc_port}, logging to ${log_file}"
    # Command to run the Java application
    # Ensure your config.properties points to http://localhost:${DEFAULT_ETCD_PORT} for etcd
    # and the correct ZooKeeper address (usually localhost:2181 by default)
    JAVA_CMD="java -jar ${APP_JAR_PATH} ${node_id} localhost ${grpc_port}"

    if [ "$TERMINAL_MULTIPLEXER" == "screen" ]; then
        # Check if session already exists for this node
        if screen -ls | grep -q "\.${node_id}\."; then
            echo "Screen session for ${node_id} already exists. Killing it first."
            screen -S "${node_id}" -X quit
            sleep 1 # Give it a moment to die
        fi
        screen -dmS "${node_id}" -L -Logfile "${log_file}" bash -c "${JAVA_CMD}"
        echo "   ${node_id} started in screen session '${node_id}'. Tail log with: tail -f ${log_file}"
    elif [ "$TERMINAL_MULTIPLEXER" == "tmux" ]; then
        # Check if session already exists for this node
        if tmux has-session -t "${node_id}" 2>/dev/null; then
            echo "tmux session for ${node_id} already exists. Killing it first."
            tmux kill-session -t "${node_id}"
            sleep 1 # Give it a moment to die
        fi
        tmux new-session -d -s "${node_id}" "${JAVA_CMD} > ${log_file} 2>&1"
        echo "   ${node_id} started in tmux session '${node_id}'. Attach with: tmux attach -t ${node_id}"
        echo "   Or tail log with: tail -f ${log_file}"
    else
        # Fallback: Start in background, redirect output to log file
        nohup ${JAVA_CMD} > "${log_file}" 2>&1 &
        echo "   ${node_id} started in background (PID $!). Tail log with: tail -f ${log_file}"
    fi
}

# Start the three nodes
start_node "node-1" ${NODE1_PORT}
sleep 2 # Stagger starts slightly
start_node "node-2" ${NODE2_PORT}
sleep 2
start_node "node-3" ${NODE3_PORT}

echo ""
echo ">>> All ConcertNode instances have been launched."
echo ">>> Check individual log files in the '${PROJECT_ROOT_DIR}/logs/' directory."
echo ">>> Use './scripts/dev_services.sh status' to check etcd/ZK."
echo ">>> To stop everything: "
echo "    1. Run './scripts/dev_services.sh stop'"
echo "    2. Manually kill the Java processes or use screen/tmux commands to kill sessions:"
echo "       If using screen: screen -S node-1 -X quit; screen -S node-2 -X quit; screen -S node-3 -X quit"
echo "       If using tmux: tmux kill-session -t node-1; tmux kill-session -t node-2; tmux kill-session -t node-3"
echo "       If using nohup: pkill -f \"java -jar ${APP_JAR_PATH}\" (Be careful with pkill!)"
