FROM apache/spark:3.5.1-python3

USER root
ENV TZ=UTC PYTHONUNBUFFERED=1

# Connector jars baked in at build time, so containers start without
# resolving --packages from Maven on every run.
ARG MAVEN=https://repo1.maven.org/maven2
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir --upgrade pip==24.0 \
 && pip3 install --no-cache-dir -r /tmp/requirements.txt \
 && pip3 install --no-cache-dir --no-deps delta-spark==3.2.0

# Named volumes inherit ownership from the image path on first mount.
RUN mkdir -p /checkpoints /lake /app && chown -R spark:spark /checkpoints /lake /app
COPY --chown=spark:spark *.py /app/
WORKDIR /app
USER spark
