## Build for Custom Spark Version

Per default, dataflow will be built for a fairly recent Spark version (2.2.1 as of this writing). But of course you can 
also build for a different version

        mvn install -Dspark.version=2.2.1