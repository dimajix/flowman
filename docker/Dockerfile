FROM eclipse-temurin:11-jdk AS build
MAINTAINER k.kupferschmidt@dimajix.de

ARG DIST_FILE
ARG BUILD_SPARK_VERSION
ARG BUILD_HADOOP_VERSION

ENV FLOWMAN_HOME=/opt/flowman
ENV SPARK_HOME=/opt/spark

USER root

# Download and install Spark
RUN curl -sL --retry 3 "https://archive.apache.org/dist/spark/spark-${BUILD_SPARK_VERSION}/spark-${BUILD_SPARK_VERSION}-bin-hadoop${BUILD_HADOOP_VERSION}.tgz" \
    | tar xz -C /opt \
    && ln -s /opt/spark-${BUILD_SPARK_VERSION}-bin-hadoop${BUILD_HADOOP_VERSION} ${SPARK_HOME} \
    && chown -R root:root $SPARK_HOME

# Copy config files
COPY bin/ /opt/docker/bin/
COPY libexec/ /opt/docker/libexec/

# Copy and install Repository
COPY $DIST_FILE /tmp/repo/flowman-dist.tar.gz
COPY conf/ /tmp/repo/conf
RUN tar -C /opt --owner=root --group=root -xzf /tmp/repo/flowman-dist.tar.gz \
    && chown -R root:root /opt/flowman* \
    && ln -s /opt/flowman* ${FLOWMAN_HOME} \
    # Copy configuration
    && cp -a /tmp/repo/conf/* ${FLOWMAN_HOME}/conf \
    # Fix permissions for copied resources
    && chmod a+r ${FLOWMAN_HOME}/conf/* \
    && chmod a+rx /opt/docker/bin/* /opt/docker/libexec/* \
    # Create new "flowman" user and group
    && groupadd -g 2000 flowman \
    && useradd -m -u 2000 -g 2000 -s /bin/bash flowman \
    # Copy examples to flowmans home
    && cp -a /opt/flowman/examples ~flowman \
    && chown -R flowman:flowman ~flowman/examples \
    # Create new var directory with write permissions for "flowman" user
    && mkdir ${FLOWMAN_HOME}/var \
    && chown flowman:flowman ${FLOWMAN_HOME}/var \
    # Install missing Hadoop dependencies, which are required for S3, DeltaLake etc
    && ${FLOWMAN_HOME}/bin/install-hadoop-dependencies \
    && rm -rf ~/.m2 \
    && chown -R root:root $SPARK_HOME/jars


FROM eclipse-temurin:11-jdk AS final

ENV FLOWMAN_HOME=/opt/flowman
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$FLOWMAN_HOME/bin

USER root

# Update OS
RUN apt-get update && apt-get upgrade --yes && apt clean

RUN groupadd -g 2000 flowman \
    && useradd -m -u 2000 -g 2000 -s /bin/bash flowman

COPY --from=build $SPARK_HOME $SPARK_HOME
COPY --from=build /opt/docker /opt/docker
COPY --from=build $FLOWMAN_HOME $FLOWMAN_HOME
COPY --from=build /home/flowman /home/flowman

USER flowman
WORKDIR /home/flowman
ENTRYPOINT ["/opt/docker/bin/entrypoint.sh"]
CMD bash
