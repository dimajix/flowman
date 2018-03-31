package com.dimajix.flowman.sources.local

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession


object implicits {
    implicit class DataFrameHolder(df:DataFrame) {
        def writeLocal : DataFrameWriter = new DataFrameWriter(df)
    }
    implicit class SparkSessionHolder(spark:SparkSession) {
        def readLocal : DataFrameReader = new DataFrameReader(spark)
    }
}
