@echo off

rem This is the entry point for running SparkR. To avoid polluting the
rem environment, it just launches a new cmd to do the real work.

rem The outermost quotes are used to prevent Windows command line parse error rem when there are some quotes in parameters
cmd /V /E /C ""%~dp0flowshell2.cmd" %*"
