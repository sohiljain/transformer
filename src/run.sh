#!/bin/bash

# Copyright (c) 2020 Bridg Inc. All rights reserved.
# @author Sohil Jain <sohil.jain@bridg.com>

pipenv run python3 main/pgp_decrypt_upload.py
pipenv run python3 main/run_job_flow.py
