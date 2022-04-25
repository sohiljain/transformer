#!/bin/bash -e

if $(aws ecr get-login --no-include-email --region us-west-2 2>/dev/null); then
  echo "Login successful via the depreceated awc ecr get-login"
else
  ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
  aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin ${ACCOUNT}.dkr.ecr.us-west-2.amazonaws.com
  echo "Login successful via aws ecr get-login-password"
fi