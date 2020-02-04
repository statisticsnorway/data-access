# data-access
Contains services for accessing data storage from other dapla modules

#### Test it

1) Build the project: `mvn clean install`
2) Build the docker image: `docker build -t data-access .`
3) Start the container:
```
docker run -p 10140:10140 -v $(pwd)/../dapla-spark-plugin/secret:/secret -e DATA_ACCESS_SERVICE_ACCOUNT_KEY_FILE=/secret/gcs_sa_test.json -e DATA_ACCESS_SERVICE_DEFAULT_LOCATION=gs://dev-datalager-store data-access
```
4) Test the endpoints:
```
curl -i -X GET "http://localhost:10140/access/location?valuation=SENSITIVE&state=RAW"
curl -i -X GET "http://localhost:10140/access/token?userId=user&location=myBucket&privilege=READ"
```

#### Note about running unit-tests in IntelliJ

Add these as env variables:
```
DATA_ACCESS_SERVICE_ACCOUNT_KEY_FILE = /secret/gcs_sa_test.json
DATA_ACCESS_SERVICE_DEFAULT_LOCATION = gs://dev-datalager-store
```

And add these as vm options:
```
--add-exports=io.grpc/io.opencensus.trace.propagation=ALL-UNNAMED
--add-exports=io.grpc/io.opencensus.trace=ALL-UNNAMED
--add-exports=io.grpc/io.opencensus.trace.export=ALL-UNNAMED
--add-exports=io.grpc/io.opencensus.common=ALL-UNNAMED
```
