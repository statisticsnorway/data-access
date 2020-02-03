# data-access
Contains services for accessing data storage from other dapla modules

#### Test it

1) Build the project: `mvn clean install`
2) Build the docker image: `docker build -t data-access .`
3) Start the container: `docker run -p 10130:10130 data-access`
4) Test the endpoint: `curl -i -X GET "http://localhost:10130/access/location?userId=user&privilege=READ"`

#### Add these as vm options in order to run unit-tests in IntelliJ:
```
--add-exports=io.grpc/io.opencensus.trace.propagation=ALL-UNNAMED
--add-exports=io.grpc/io.opencensus.trace=ALL-UNNAMED
--add-exports=io.grpc/io.opencensus.trace.export=ALL-UNNAMED
--add-exports=io.grpc/io.opencensus.common=ALL-UNNAMED
```
