@echo off

call "%~dp0../libexec/flowman-common.cmd"

SET LF=^

SETLOCAL

SET APP_NAME=flowman-tools
SET APP_VERSION=${project.version}
SET APP_MAIN=com.dimajix.flowman.tools.shell.Shell

SET APP_JAR="%FLOWMAN_HOME%\lib\%APP_NAME%-%APP_VERSION%.jar"
SET LIB_JARS="${flowman-tools.classpath}"


rem Collect all libraries
set r=%LIB_JARS%
set LIB_JARS=
:loop
for /F "tokens=1* delims=," %%a in (%r%) do (
  if "x%LIB_JARS%" == "x" (
	set LIB_JARS="%FLOWMAN_HOME%/lib/%%a"
  ) else (
	set LIB_JARS=%LIB_JARS%,"%FLOWMAN_HOME%\lib\%%a"
  )
  set r="%%b"
)
if not %r% == "" goto :loop


%SPARK_SUBMIT% --driver-java-options "%SPARK_DRIVER_JAVA_OPTS%" --conf spark.execution.extraJavaOptions="%SPARK_EXECUTOR_JAVA_OPTS%" --class %APP_MAIN% %SPARK_OPTS% --jars %LIB_JARS% %APP_JAR% %*
