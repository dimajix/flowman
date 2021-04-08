FROM openjdk:8
MAINTAINER k.kupferschmidt@dimajix.de

ARG DIST_FILE
ARG BUILD_SPARK_VERSION
ARG BUILD_HADOOP_VERSION

USER root

ENV FLOMAN_HOME=/opt/flowman
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$FLOMAN_HOME/bin

# Download and install Spark
RUN curl -sL --retry 3 "http://ftp.fau.de/apache/spark/spark-${BUILD_SPARK_VERSION}/spark-${BUILD_SPARK_VERSION}-bin-hadoop${BUILD_HADOOP_VERSION}.tgz" \
    | tar xz -C /opt \
    && ln -s /opt/spark-${BUILD_SPARK_VERSION}-bin-hadoop${BUILD_HADOOP_VERSION} ${SPARK_HOME} \
    && chown -R root:root $SPARK_HOME

# Copy config files
COPY bin/ /opt/docker/bin/
COPY libexec/ /opt/docker/libexec/


# Copy and install Repository
COPY $DIST_FILE /tmp/repo/flowman-dist.tar.gz
COPY conf/ /tmp/repo/conf
RUN tar -C /opt --owner=root --group=root -xzf /tmp/repo/flowman-dist.tar.gz && \
    ln -s /opt/flowman* ${FLOMAN_HOME} && \
    cp -a /tmp/repo/conf/* ${FLOMAN_HOME}/conf && \
    chown -R root:root /opt/flowman* && \
    rm -rf /tmp/repo && \
    chmod a+x /opt/docker/bin/*


ENTRYPOINT ["/opt/docker/bin/entrypoint.sh"]
CMD bash
