#!/bin/bash

# Copyright (c) 2020 Bridg Inc. All rights reserved.
# @author Sohil Jain <sohil.jain@bridg.com>

CONTAINER="bridg-tranformer"
echo "Starting Container $CONTAINER"

docker rm -vf $CONTAINER
docker build --no-cache -t $CONTAINER .
docker run -t --name $CONTAINER $CONTAINER "$@"

