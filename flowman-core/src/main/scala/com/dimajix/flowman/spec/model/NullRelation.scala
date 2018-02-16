package com.dimajix.flowman.spec.model
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.schema.SingleValue
import com.dimajix.flowman.spec.schema.FieldValue


class NullRelation extends Relation {
    /**
      * Reads data from the relation, possibly from specific partitions
      *
      * @param executor
      * @param schema
      * @param partitions
      * @return
      */
    override def read(executor:Executor, schema:StructType, partitions:Map[String,FieldValue] = Map()) : DataFrame = {
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
    override def write(executor:Executor, df:DataFrame, partition:Map[String,SingleValue], mode:String) : Unit = {
    }

    override def create(executor: Executor): Unit = {
    }
    override def destroy(executor: Executor): Unit = {
    }
    override def migrate(executor: Executor): Unit = {
    }
}
