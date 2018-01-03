package com.dimajix.dataflow.spec.model

import java.util.ServiceLoader

import scala.collection.JavaConversions._

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import com.dimajix.dataflow.execution.Context
import com.dimajix.dataflow.spec.model.Relation.Partition
import com.dimajix.dataflow.spi.RelationProvider


class Field {
    @JsonProperty(value="name") private var _name: String = _
    @JsonProperty(value="type") private var _type: String = _
    @JsonProperty(value="description") private var _description: String = _
}


object Relation {
    type Partition = Row

    def getProviders() = {
        val loader = ServiceLoader.load(classOf[RelationProvider])
        loader.iterator().toSeq
    }
}

/**
  * Interface class for declaring relations (for sources and sinks) as part of a model
  */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "jdbc", value = classOf[JdbcRelation]),
    new JsonSubTypes.Type(name = "hive", value = classOf[HiveRelation]),
    new JsonSubTypes.Type(name = "file", value = classOf[FileRelation])
))
abstract class Relation {
    /**
      * Reads data from the relation, possibly from specific partitions
      *
      * @param context
      * @param schema
      * @param partition
      * @return
      */
    def read(context:Context, schema:StructType, partition:Seq[Partition] = Seq()) : DataFrame

    /**
      * Writes data into the relation, possibly into a specific partition
      * @param context
      * @param df
      * @param partition
      */
    def write(context:Context, df:DataFrame, partition:Partition = null) : Unit
    def create(context:Context) : Unit
    def destroy(context:Context) : Unit
    def migrate(context: Context) : Unit
}
