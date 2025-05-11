#!/bin/bash

echo "WARNING: This script will stop ALL running Docker containers."
echo "It will then remove ALL stopped containers, ALL unused networks, ALL dangling images, AND ALL unused volumes."
echo "Data in unused volumes will be PERMANENTLY DELETED."
read -p "Are you absolutely sure you want to continue? (yes/no): " confirmation

if [[ "$confirmation" != "yes" ]]; then
    echo "Operation cancelled."
    exit 0
fi

DOCKER_CMD="docker"
# If you always need sudo for docker commands:
# DOCKER_CMD="sudo docker"
# Or, to be more robust based on your previous input:
if ! $DOCKER_CMD ps > /dev/null 2>&1; then
    echo "Docker command failed. Attempting with sudo..."
    DOCKER_CMD="sudo docker"
    if ! $DOCKER_CMD ps > /dev/null 2>&1; then
        echo "Error: Docker command failed even with sudo. Is Docker installed and the daemon running?"
        exit 1
    fi
fi


echo "Stopping all running Docker containers..."
RUNNING_CONTAINERS=$($DOCKER_CMD ps -q)
if [ -z "$RUNNING_CONTAINERS" ]; then
    echo "No running containers found."
else
    $DOCKER_CMD stop $RUNNING_CONTAINERS
    echo "All previously running containers stopped."
fi

echo "Running comprehensive Docker system prune (removes stopped containers, unused networks, dangling images)..."
# This combines container prune, network prune, and image prune (dangling only)
$DOCKER_CMD system prune -f

echo "Removing unused Docker volumes..."
$DOCKER_CMD volume prune -f

# Optionally, to remove ALL unused images (not just dangling ones), use:
# echo "Pruning ALL unused images (images not used by any existing container)..."
# $DOCKER_CMD image prune -a -f


echo "Docker aggressive cleanup complete."
echo "Ports previously held by stopped Docker containers should now be free."
echo "Unused volumes have been removed."
echo "Note: This does NOT stop processes running directly on your host OS that might be using ports."
