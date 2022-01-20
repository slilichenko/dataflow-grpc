# Example of calling gRPC from Beam pipelines
This pipeline resolves US Postal Zip Codes into a Zip/State/City. 

Requests are received on Pub/Sub subscription as JSon payloads with a single `zip` attribute and
the lookup results are output to a Google Cloud Storage Bucket in CSV format.

# Setup environment and build artifact
* `export PROJECT_ID=<project where the buckets will be deployed>`
* Create the Pub/Sub subscription, output GCS bucket and other temporary buckets. 
The command below will run a Terraform script and set several variables to be used
in other scripts

    `source ./setup-env.sh`

* Build the gRPC artifacts and deploy to the local Maven repo

    `(cd grpc-core && ./gradlew build publishToMavenLocal)`

* Build the gRPC server and deploy to the local Maven repo

  `(cd grpc-server && ./gradlew build publishToMavenLocal)`
    
* Build (and test locally) the pipeline

  `(cd pipeline && ./gradlew build)`

# Deploy the gRPC server to Cloud Run
Steps below will build a server's Docker image, deploy it to the Container Registry,
deploy the image to Cloud Run and store the URL of the gRPC server in GRPC_HOST environment variable. 

  `(cd grpc-server && ./deploy-to-cloud-run.sh && cd ..)`

# Capture the gRPC server hostname

  `export GRPC_HOST=$(grpc-server/get-server-hostname.sh)`

# Deploy the pipeline

  `(cd pipeline && ./run-pipeline.sh)`
  
# Run the request generation pipeline
This pipeline generates randomly generated JSON payloads based on [this template](data-generator/event-generator-template.json). 
`rate` parameter defines the number of records per second.  

  `./start-request-generation.sh <rate>`

To test scaling of the main pipeline you can run multiple request pipelines.