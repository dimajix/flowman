FROM ${docker.base-image.repository}:${docker.base-image.version}
MAINTAINER k.kupferschmidt@dimajix.de

USER root

ENV FLOMAN_HOME=/opt/flowman
ENV PATH=$PATH:$FLOMAN_HOME/bin

# Copy config files
COPY bin/ /opt/docker/bin/
COPY libexec/ /opt/docker/libexec/


# Copy and install Repository
COPY flowman-dist-${project.version}-bin.tar.gz /tmp/repo/
COPY conf/ /tmp/repo/conf
RUN tar -C /opt --owner=root --group=root -xzf /tmp/repo/flowman-dist-${project.version}-bin.tar.gz && \
    ln -s /opt/flowman* /opt/flowman && \
    cp -a /tmp/repo/conf/* /opt/flowman/conf && \
    chown -R root:root /opt/flowman* && \
    rm -rf /tmp/repo && \
    chmod a+x /opt/docker/bin/*


ENTRYPOINT ["/opt/docker/bin/entrypoint.sh"]
CMD bash
