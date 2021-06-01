@echo off

rem Set Flowman directories
if "x%FLOWMAN_HOME%"=="x" (
	call :NORMALIZEPATH FLOWMAN_HOME %~dp0\..
)
if "x%FLOWMAN_CONF_DIR%"=="x" (
    set FLOWMAN_CONF_DIR=%FLOWMAN_HOME%\conf
)

rem Load environment file if present
if exist "%FLOWMAN_CONF_DIR%\flowman-env.cmd" (
    call "%FLOWMAN_CONF_DIR%\flowman-env.cmd"
)

if exist "%HADOOP_HOME%\etc\hadoop\hadoop-env.cmd" (
    call "%HADOOP_HOME%\etc\hadoop\hadoop-env.cmd"
)

rem Set basic Spark options
if "x%SPARK_SUBMIT%"=="x" (
    set SPARK_SUBMIT="%SPARK_HOME%\bin\spark-submit.cmd"
)
if "x%SPARK_OPTS%"=="x" (
    set SPARK_OPTS=
)
if "x%SPARK_DRIVER_JAVA_OPTS%"=="x" (
    set SPARK_DRIVER_JAVA_OPTS=-server
)
if "x%SPARK_EXECUTOR_JAVA_OPTS%"=="x" (
    set SPARK_EXECUTOR_JAVA_OPTS=-server
)


rem Add Optional settings to SPARK_OPTS
if not "x%KRB_PRINCIPAL%" == "x" (
    set SPARK_OPTS="--principal %KRB_PRINCIPAL% --keytab %KRB_KEYTAB% %SPARK_OPTS%"
)
if not "x%YARN_QUEUE%" == "x" (
    set SPARK_OPTS=--queue %YARN_QUEUE% %SPARK_OPTS%
)
if not "x%SPARK_MASTER%" == "x" (
    set SPARK_OPTS=--master %SPARK_MASTER% %SPARK_OPTS%
)
if not "x%SPARK_EXECUTOR_CORES%" == "x" (
    set SPARK_OPTS=--executor-cores %SPARK_EXECUTOR_CORES% %SPARK_OPTS%
)
if not "x%SPARK_EXECUTOR_MEMORY%" == "x" (
    set SPARK_OPTS=--executor-memory %SPARK_EXECUTOR_MEMORY% %SPARK_OPTS%
)
if not "x%SPARK_DRIVER_CORES%" == "x" (
    set SPARK_OPTS=--driver-cores %SPARK_DRIVER_CORES% %SPARK_OPTS%
)
if not "x%SPARK_DRIVER_MEMORY%" == "x" (
    set SPARK_OPTS=--driver-memory %SPARK_DRIVER_MEMORY% %SPARK_OPTS%
)


:: ========== FUNCTIONS ==========
exit /b

:NORMALIZEPATH
  set %~1=%~f2
  goto :eof

