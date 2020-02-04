FROM alpine:latest as build

RUN apk --no-cache add curl tar gzip binutils

#
# Install JDK
#
RUN curl https://cdn.azul.com/zulu/bin/zulu13.28.11-ca-jdk13.0.1-linux_musl_x64.tar.gz -o /jdk.tar.gz
RUN mkdir -p /jdk
RUN tar xzf /jdk.tar.gz --strip-components=1 -C /jdk
ENV PATH=/jdk/bin:$PATH
ENV JAVA_HOME=/jdk

#
# Build stripped JVM
#
RUN ["jlink", "--strip-debug", "--no-header-files", "--no-man-pages", "--compress=2", "--module-path", "/jdk/jmods", "--output", "/linked",\
 "--add-modules", "java.base,java.management,jdk.unsupported,java.sql,jdk.zipfs,jdk.naming.dns,java.desktop,java.net.http,jdk.crypto.cryptoki"]

#
# Build Application image
#
FROM alpine:latest

#
# Resources from build image
#
COPY --from=build /linked /jdk/
COPY target/libs /app/lib/
COPY target/data-access-service*.jar /app/lib/
COPY target/classes/logback.xml /app/conf/
COPY target/classes/logback-bip.xml /app/conf/
COPY target/classes/application.yaml /app/conf/

ENV PATH=/jdk/bin:$PATH

WORKDIR /app

EXPOSE 10140
EXPOSE 10148

CMD ["java", "--add-exports=io.grpc/io.opencensus.trace=com.google.api.client", \
"--add-exports=io.grpc/io.opencensus.trace.export=com.google.api.client", \
"--add-exports=io.grpc/io.opencensus.trace.propagation=com.google.api.client", \
"--add-exports=io.grpc/io.opencensus.common=com.google.api.client", \
"--add-exports=io.grpc/io.opencensus.trace=opencensus.contrib.http.util", \
"--add-exports=io.grpc/io.opencensus.trace.propagation=opencensus.contrib.http.util", \
"--add-exports=io.grpc/io.opencensus.trace.propagation=opencensus.contrib.http.util", \
"-p", "/app/lib", "-m", "no.ssb.dapla.data.access/no.ssb.dapla.data.access.Application"]
