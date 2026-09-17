# Custom Flink image: base + Kafka/Iceberg/JDBC/S3 connectors + a thin SQL-runner main class
# that executes a mounted .sql file via TableEnvironment (community "flink-sql-runner" pattern --
# https://github.com/ververica/flink-sql-runner-example). Built/pushed once, then referenced by
# both FlinkDeployments below via spec.image.
FROM flink:1.19-scala_2.12-java17

ENV FLINK_VERSION=1.19.1
ENV ICEBERG_VERSION=1.6.1
ENV HADOOP_VERSION=3.3.4

RUN wget -P /opt/flink/lib/ \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.19/${ICEBERG_VERSION}/iceberg-flink-runtime-1.19-${ICEBERG_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar \
      https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

COPY sql-runner/target/sql-runner.jar /opt/flink/usrlib/sql-runner.jar
