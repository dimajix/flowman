# klause project image for Flowman
#
# Extends the klause base image (Fedora) with the tools needed to build and
# test Flowman: a JDK supported by Flowman/Spark (Fedora itself only ships
# current JDKs like 25), Apache Maven and xmlstarlet for the integration tests
# in tests/. Node.js/npm are already in the base image; the Maven build of
# flowman-server-ui downloads its own Node.js anyway.
#
# Rebuild with "klause --klause-build", then recreate the container with
# "klause --klause-rm".

ARG KLAUSE_IMAGE=klause:latest
FROM ${KLAUSE_IMAGE}

USER root

# Feature release of Eclipse Temurin. Flowman 1.3 with Spark 3.5 builds and
# tests with 17; profiles for old Spark versions (2.4, EMR, Synapse) need 8.
ARG JDK_VERSION=17
ARG MAVEN_VERSION=3.9.16

RUN dnf -y install --setopt=install_weak_deps=False xmlstarlet \
    && dnf clean all

# Latest GA build of the requested JDK, verified against the checksum from
# the Adoptium API
RUN set -eu; \
    case "$(uname -m)" in \
        x86_64)  arch=x64 ;; \
        aarch64) arch=aarch64 ;; \
        *) echo "unsupported architecture $(uname -m)" >&2; exit 1 ;; \
    esac; \
    meta=$(curl -fsSL "https://api.adoptium.net/v3/assets/latest/${JDK_VERSION}/hotspot?architecture=${arch}&image_type=jdk&os=linux&vendor=eclipse"); \
    link=$(printf '%s' "$meta" | jq -r '.[0].binary.package.link'); \
    sum=$(printf '%s' "$meta" | jq -r '.[0].binary.package.checksum'); \
    curl -fsSL -o /tmp/jdk.tar.gz "$link"; \
    echo "$sum  /tmp/jdk.tar.gz" | sha256sum -c -; \
    mkdir -p /opt/java/jdk-${JDK_VERSION}; \
    tar -xzf /tmp/jdk.tar.gz -C /opt/java/jdk-${JDK_VERSION} --strip-components=1; \
    rm /tmp/jdk.tar.gz; \
    ln -s jdk-${JDK_VERSION} /opt/java/current; \
    for f in /opt/java/current/bin/*; do ln -s "$f" /usr/local/bin/; done

RUN set -eu; \
    url="https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz"; \
    curl -fsSL -o /tmp/maven.tar.gz "$url"; \
    echo "$(curl -fsSL "$url.sha512" | cut -d' ' -f1)  /tmp/maven.tar.gz" | sha512sum -c -; \
    mkdir -p /opt/maven; \
    tar -xzf /tmp/maven.tar.gz -C /opt/maven --strip-components=1; \
    rm /tmp/maven.tar.gz; \
    ln -s /opt/maven/bin/mvn /usr/local/bin/mvn

# klause sets its own PATH on exec, so the binaries are linked into
# /usr/local/bin above. Other variables of the image are kept.
ENV JAVA_HOME=/opt/java/current \
    MAVEN_HOME=/opt/maven \
    MAVEN_OPTS="-Xmx4g -Xss4m"
