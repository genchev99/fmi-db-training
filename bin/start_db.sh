#!/bin/bash

ROOT=$(dirname "$(dirname "$(realpath ${BASH_SOURCE[0]})")")

cd ${ROOT}

docker-compose up -d db
