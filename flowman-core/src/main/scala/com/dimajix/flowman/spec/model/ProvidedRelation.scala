package com.dimajix.flowman.spec.model

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.schema.FieldValue
import com.dimajix.flowman.spec.schema.SingleValue


class ProvidedRelation extends BaseRelation {
    @JsonProperty(value="table") private var _table: String = _

    def table(implicit context:Context) : String = context.evaluate(_table)

    /**
      * Reads data from the relation, possibly from specific partitions
      *
      * @param executor
      * @param schema
      * @param partitions
      * @return
      */
    override def read(executor:Executor, schema:StructType, partitions:Map[String,FieldValue] = Map()) : DataFrame = {
        implicit val context = executor.context
        executor.spark.table(table)
    }

    /**
      * Writes data into the relation, possibly into a specific partition
      *
      * @param executor
      * @param df
      * @param partition
      */
    override def write(executor:Executor, df:DataFrame, partition:Map[String,SingleValue], mode:String) : Unit = {
        throw new UnsupportedOperationException("Writing into provided tables not supported")
    }

    override def create(executor: Executor): Unit = {
        throw new UnsupportedOperationException("Creating provided tables not supported")
    }
    override def destroy(executor: Executor): Unit = {
        throw new UnsupportedOperationException("Destroying provided tables not supported")
    }
    override def migrate(executor: Executor): Unit = {
        throw new UnsupportedOperationException("Migrating provided tables not supported")
    }

}
