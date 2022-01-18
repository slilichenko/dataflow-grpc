#
# Copyright 2021 Google LLC
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

set -e
set -u

JOB_NAME="zip-resolver"
MODE='streaming'

# EXPERIMENTS=enable_recommendations,enable_google_cloud_profiler,enable_google_cloud_heap_sampling
EXPERIMENTS=enable_recommendations
RUNNER=dataflow

if [ ${MODE} = 'streaming' ] ; then
  PARAMS="--enableStreamingEngine --diskSizeGb=30 --subscriptionId=${REQUEST_SUB}"
elif [ ${MODE} = 'batch' ]; then
  PARAMS="--fileList=${DATA_BUCKET}/*.json"
else
  echo "First parameter must be either 'streaming' or 'batch'";
  exit 1;
fi

set -x
./gradlew run -Pargs="--jobName=${JOB_NAME} \
 --project=${PROJECT_ID}\
 --region=${REGION}\
 --maxNumWorkers=10\
 --runner=${RUNNER}\
 --experiments=${EXPERIMENTS}\
 --grpcHost=${GRPC_HOST}\
 --outputBucket=${OUTPUT_BUCKET}\
 ${PARAMS}"

