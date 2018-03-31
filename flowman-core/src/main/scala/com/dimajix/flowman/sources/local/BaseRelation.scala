package com.dimajix.flowman.sources.local

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.StructType


abstract class BaseRelation {
    def sqlContext: SQLContext
    def schema: StructType

    def read(): DataFrame
    def write(df:DataFrame, mode:SaveMode)
}
