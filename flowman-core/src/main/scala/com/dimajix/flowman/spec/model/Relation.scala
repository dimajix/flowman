package com.dimajix.flowman.spec.model

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.schema.FieldValue
import com.dimajix.flowman.spec.schema.SingleValue
import com.dimajix.flowman.spi.Scanner


object Relation {
    def subtypes : Seq[(String,Class[_ <: Relation])] = Scanner.relations
}

/**
  * Interface class for declaring relations (for sources and sinks) as part of a model
  */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "jdbc", value = classOf[JdbcRelation]),
    new JsonSubTypes.Type(name = "hive", value = classOf[HiveRelation]),
    new JsonSubTypes.Type(name = "file", value = classOf[FileRelation]),
    new JsonSubTypes.Type(name = "null", value = classOf[NullRelation])
))
abstract class Relation {
    /**
      * Reads data from the relation, possibly from specific partitions
      *
      * @param executor
      * @param schema - the schema to read. If none is specified, all available columns will be read
      * @param partitions - List of partitions. If none are specified, all the data will be read
      * @return
      */
    def read(executor:Executor, schema:StructType, partitions:Map[String,FieldValue] = Map()) : DataFrame

    /**
      * Writes data into the relation, possibly into a specific partition
      * @param executor
      * @param df - dataframe to write
      * @param partition - destination partition
      */
    def write(executor:Executor, df:DataFrame, partition:Map[String,SingleValue] = Map(), mode:String = "OVERWRITE") : Unit
    def create(executor:Executor) : Unit
    def destroy(executor:Executor) : Unit
    def migrate(executor:Executor) : Unit
}
