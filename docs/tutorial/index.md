# Flowman Tutorial

Welcome to this tutorial for developing data transformation applications with Flowman. In a sequence of multiple
lessons, you will start learning the basic concepts of Flowman and later get known to more advanced features of
Flowman. All lessons will use a subset of a publicly available data set about weather data. This data is taken from
[Global Hourly - Integrated Surface Database (ISD)](https://www.ncdc.noaa.gov/isd).

You will find the source code of this tutorial [on GitHub](https://github.com/dimajix/flowman-tutorial)

## Preparations
The simplest way to follow the tutorial is to use the provided `docker-compose.yml` which will start a Docker container
containing Flowman with all lessons mounted as a volume.


### Start MS SQL Server
For some lessons, you will need an MS SQL Server. This is provided as a simple docker container and can be started
as follows:

```shell
docker-compose up -d sqlserver
```

### Start Flowman Container
Once the MS SQL Server is up, you need to start the Flowman container, which will mount all lessons in a volume
under `/home/flowman/lessons`. All `README.md` files assume that you changed into the directory `/home/flowman`.

```shell
docker-compose run --rm flowman bash

cd /home/flowman
```
