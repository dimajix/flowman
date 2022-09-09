@echo off

call "%~dp0../libexec/flowman-common.cmd"

SET APP_NAME=flowman-tools
SET APP_VERSION=@project.version@
SET APP_MAIN=com.dimajix.flowman.tools.shell.Shell

SET APP_JAR=%APP_NAME%-%APP_VERSION%.jar
SET LIB_JARS="@flowman-tools.classpath@"

call "%~dp0../libexec/flowman-launch.cmd" %*
