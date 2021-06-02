rem Collect all libraries

SETLOCAL


SET APP_JAR="%FLOWMAN_HOME%\lib\%APP_JAR%"

set r=%LIB_JARS%
set LIB_JARS=
:loop
for /F "tokens=1* delims=," %%a in (%r%) do (
  if "x%LIB_JARS%" == "x" (
	set LIB_JARS="%FLOWMAN_HOME%\%%a"
  ) else (
	set LIB_JARS=%LIB_JARS%,"%FLOWMAN_HOME%\%%a"
  )
  set r="%%b"
)
if not %r% == "" goto :loop


%SPARK_SUBMIT% --driver-java-options "%SPARK_DRIVER_JAVA_OPTS%" --conf spark.execution.extraJavaOptions="%SPARK_EXECUTOR_JAVA_OPTS%" --class %APP_MAIN% %SPARK_OPTS% --jars %LIB_JARS% %APP_JAR% %*
