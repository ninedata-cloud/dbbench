# Build stage
FROM maven:3.9-eclipse-temurin-17 AS builder

WORKDIR /app
COPY pom.xml .
COPY src ./src

RUN mvn clean package -DskipTests

# Runtime stage
FROM eclipse-temurin:17-jre

LABEL maintainer="NineData <support@ninedata.cloud>"
LABEL description="NineData DBBench - TPC-C Database Benchmark Tool"

# Copy the built jar
COPY --from=builder /app/target/dbbench-0.8.0.jar /app/dbbench.jar

# Expose web UI port
EXPOSE 1929

# Environment variables
ENV JAVA_OPTS="-Xms512m -Xmx1024m"
ENV DB_TYPE=mysql
ENV DB_JDBC_URL=jdbc:mysql://host.docker.internal:3306/tpcc
ENV DB_USERNAME=root
ENV DB_PASSWORD=
ENV DB_POOL_SIZE=50
ENV BENCHMARK_WAREHOUSES=10
ENV BENCHMARK_TERMINALS=50
ENV BENCHMARK_DURATION=60
ENV BENCHMARK_LOAD_CONCURRENCY=4
ENV BENCHMARK_CSV_LOAD=false

WORKDIR /app
CMD ["sh", "-c", "java $JAVA_OPTS -jar /app/dbbench.jar"]
