package com.dimajix.dataflow.spec.model
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import com.dimajix.dataflow.execution.Executor
import com.dimajix.dataflow.spec.model.Relation.Partition


class NullRelation extends Relation {
    /**
      * Reads data from the relation, possibly from specific partitions
      *
      * @param executor
      * @param schema
      * @param partition
      * @return
      */
    override def read(executor: Executor, schema: StructType, partition: Seq[Partition]): DataFrame = {
        val rdd = executor.spark.sparkContext.emptyRDD[Row]
        executor.spark.createDataFrame(rdd, schema)
    }

    /**
      * Writes data into the relation, possibly into a specific partition
      *
      * @param executor
      * @param df
      * @param partition
      */
    override def write(executor: Executor, df: DataFrame, partition: Partition, mode: String): Unit = {
    }

    override def create(executor: Executor): Unit = {
    }
    override def destroy(executor: Executor): Unit = {
    }
    override def migrate(executor: Executor): Unit = {
    }
}
