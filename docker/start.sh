#!/bin/sh

if [ $# -ne 2 ]; then
    echo "Usage: $0 CONTAINER_NAME DATA_DIR"
    exit 1
fi

if [ ! -d "$2" ]; then
    echo "Directory $2 does not exist"
    exit 1
fi

echo "Stopping and removing container (if applicable)"
docker stop "$1" || true
docker rm "$1" || true

echo "docker run:"
docker run --restart=always -d --name "$1" \
    --cap-drop all \
    -v "$2":/var/data/ethereum-etl \
    -it ethereum-etl

echo "docker ps-a:"
docker ps -a