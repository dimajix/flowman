package com.dimajix.dataflow.spec.model

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructType

import com.dimajix.dataflow.execution.Context
import com.dimajix.dataflow.execution.Executor
import com.dimajix.dataflow.spec.ValueOrRange
import com.dimajix.dataflow.spi.Scanner
import com.dimajix.dataflow.util.SchemaUtils


class Field {
    @JsonProperty(value="name", required = true) private var _name: String = _
    @JsonProperty(value="type", required = false) private var _type: String = _
    @JsonProperty(value="description", required = false) private var _description: String = _

    def name(implicit context: Context) : String = context.evaluate(_name)
    def dtype(implicit context: Context) : DataType = SchemaUtils.mapType(context.evaluate(_type))
    def description(implicit context: Context) : String = context.evaluate(_description)
}


object Relation {
    class Value
    case class RangeValue(start:Any, end:Any) extends Value
    case class ArrayValue(values:Array[Any]) extends Value
    case class SingleValue(value:Any) extends Value

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
    import com.dimajix.dataflow.spec.model.Relation.Value
    import com.dimajix.dataflow.spec.model.Relation.SingleValue

    /**
      * Reads data from the relation, possibly from specific partitions
      *
      * @param executor
      * @param schema - the schema to read. If none is specified, all available columns will be read
      * @param partitions - List of partitions. If none are specified, all the data will be read
      * @return
      */
    def read(executor:Executor, schema:StructType, partitions:Map[String,Value] = Map()) : DataFrame

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
