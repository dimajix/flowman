# Test Suite for Azure Synapse

export AZURE_TENANT_ID=
export AZURE_CLIENT_ID=
export AZURE_USERNAME=
export AZURE_PASSWORD=

Jar File: abfss://flowman@dimajixspark.dfs.core.windows.net/synapse-test-1.0-SNAPSHOT.jar
Main class: com.dimajix.flowman.tools.exec.Driver
Arguments: -B -f flow job build main --force
