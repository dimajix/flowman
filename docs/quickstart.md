# Preparation

In order to run the example, you need to have valid access credentials to AWS, since we will be using some data
stored in S3.

## Install Spark

First you need to install Apache Spark. 


## Install Flowman



## This assumes that we are still in the directory "playground"
```shell
export SPARK_HOME=$(pwd)/spark
```

## Copy default namespace
```shell
cd flowman
cp conf/default-namespace.yml.template conf/default-namespace.yml
cp conf/flowman-env.sh.template conf/flowman-env.sh

export AWS_ACCESS_KEY_ID=<your aws access key>
export AWS_SECRET_ACCESS_KEY=<your aws secret key>
```


# Flowman Shell

## Start interactive Flowman shell
```shell
bin/flowshell -f examples/weather
```

## Project Stuff
```
flowman:weather> relation list
flowman:weather> relation show stations-raw
flowman:weather> relation show measurements-raw -p year=2011
```

## Job Stuff
```
flowman:weather> job enter main year=2011
flowman:weather/main> mapping list
flowman:weather/main> mapping show measurements-raw
flowman:weather/main> mapping show measurements-extracted
flowman:weather/main> mapping show stations-raw
flowman:weather/main> job leave
```

## Running Job
```
flowman:weather> job build main year=2011
```

## Inspect Results
```
flowman:weather> relation show stations
flowman:weather> relation show measurements
flowman:weather> relation show aggregates -p year=2011
```

## Hiostory
```
flowman:weather> history job search
flowman:weather> history target search -J 1
```

## Quitting
```
flowman:weather> quit
```

# Flowman Execution
```shell
bin/flowexec -f examples/weather job build main year=2014
```
