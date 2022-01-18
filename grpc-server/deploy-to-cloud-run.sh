#!/usr/bin/env bash

#
# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -u
set -e

MAX_INSTANCES=2
IMAGE=gcr.io/${PROJECT_ID}/zip-resolver
SERVICE_NAME=zip-resolver

./gradlew jib --image=${IMAGE}
gcloud run deploy ${SERVICE_NAME} --max-instances ${MAX_INSTANCES} --use-http2 \
 --image ${IMAGE} --region ${REGION} --allow-unauthenticated

export GRPC_HOST=$(gcloud run services describe ${SERVICE_NAME} --region ${REGION} --format 'value(status.url.basename())') 
echo "gRPC server is available on ${GRPC_HOST}"
