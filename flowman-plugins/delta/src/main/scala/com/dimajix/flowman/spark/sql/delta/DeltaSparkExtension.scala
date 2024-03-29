package com.dimajix.flowman.spark.sql.delta

import io.delta.sql.DeltaSparkSessionExtension
import org.apache.spark.sql.SparkSession

import com.dimajix.flowman.config.Configuration
import com.dimajix.flowman.spi.SparkExtension


class DeltaSparkExtension extends SparkExtension {
    /**
     * Hook for extending a Spark session before it is built
     *
     * @param builder
     * @return
     */
    override def register(builder: SparkSession.Builder, config: Configuration): SparkSession.Builder = {
        builder.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .withExtensions(new DeltaSparkSessionExtension)
    }

    /**
     * Hook for extending an existing Spark session
     *
     * @param session
     * @return
     */
    override def register(session: SparkSession, config: Configuration): SparkSession = {
        session
    }
}
