package com.dimajix.flowman.sources.spark

import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.TableScan
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType


class SequenceFileRelation(val context: SQLContext, val files: String) extends BaseRelation with TableScan {
    override def sqlContext: SQLContext = null

    override def schema: StructType = DataTypes.createStructType(Array(
        DataTypes.createStructField("key", DataTypes.StringType, false),
        DataTypes.createStructField("value", DataTypes.StringType, false)
    ))

    override def buildScan: RDD[Row] = {
        val input = context.sparkContext.sequenceFile(files, classOf[Text], classOf[Text])
        input.map(t => RowFactory.create(t._1.toString, t._2.toString))
    }
}
