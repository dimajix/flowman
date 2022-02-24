# Running Flowman in Docker

Flowman can also be run inside Docker, especially when working in local mode (i.e. without a cluster). It is also
possible to run Flowman in Docker in Spark distributed processing mode, but this requires more configuration options
to forward all required ports etc.

## Running Locally

We publish Flowman Docker images on [Docker Hub](https://hub.docker.com/repository/docker/dimajix/flowman),
which are good enough for local work. You can easily start a Flowman session in Docker as follows:

```shell
docker run --rm -ti dimajix/flowman:0.21.0-oss-spark3.1-hadoop3.2 bash
```

Then once the Docker image has started you will be presented with a bash prompt. Then you can easily build the
weather example of Flowman via
```shell
cd /opt/flowman
flowexec -f examples/weather job build main
```
