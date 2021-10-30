package com.dimajix.spark.sql.streaming

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.streaming.LongOffset
import org.apache.spark.sql.execution.streaming.Offset
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.execution.streaming.StreamingExecutionRelation
import org.apache.spark.sql.types.StructType

import com.dimajix.spark.sql.DataFrameBuilder
import com.dimajix.spark.sql.DataFrameUtils


object StreamingUtils {
    def createSingleTriggerStreamingDF(triggerDF: DataFrame, offset:Long=0): DataFrame = {
        require(!triggerDF.isStreaming)

        val spark = triggerDF.sparkSession

        // A streaming Source that generate only on trigger and returns the given Dataframe as batch
        val source = new Source() {
            override def schema: StructType = triggerDF.schema
            override def getOffset: Option[Offset] = Some(LongOffset(offset))
            override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
                val logicalPlan = LogicalRDD(
                    triggerDF.schema.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()),
                    triggerDF.queryExecution.toRdd,
                    isStreaming = true)(spark)
                DataFrameBuilder.ofRows(spark, logicalPlan)
            }
            override def stop(): Unit = {}
        }
        DataFrameBuilder.ofRows(spark, StreamingExecutionRelation(source, spark))
    }
}
