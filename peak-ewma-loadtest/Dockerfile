# Load-test image (#94): backend and loadgen in one jar; the manifests choose the main class.
#   ./mvnw -pl peak-ewma-loadtest -am package -DskipTests
#   docker build -t peak-ewma-loadtest:dev peak-ewma-loadtest
FROM eclipse-temurin:25-jre
WORKDIR /app
COPY target/loadtest.jar /app/loadtest.jar
