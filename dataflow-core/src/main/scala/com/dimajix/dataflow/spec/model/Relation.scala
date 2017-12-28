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


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "jdbc", value = classOf[JdbcRelation]),
    new JsonSubTypes.Type(name = "hive", value = classOf[HiveRelation]),
    new JsonSubTypes.Type(name = "file", value = classOf[FileRelation])
))
abstract class Relation {
    @JsonProperty(value="database") private var _database: String = _
    @JsonProperty(value="namespace") private var _namespace: String = _
    @JsonProperty(value="entity") private var _entity: String = _
    @JsonProperty(value="external") private var _external: String = _
    @JsonProperty(value="options") private var _options:Map[String,String] = Map()
    @JsonProperty(value="schema") private var _schema: Seq[Field] = Seq()
    @JsonProperty(value="defaults", required=false) private var _defaultValues:Map[String,String] = Map()

    def external(implicit context:Context) : Boolean = context.evaluate(_external).toBoolean
    def namespace(implicit context:Context) : String = context.evaluate(_namespace)
    def entity(implicit context:Context) : String = context.evaluate(_entity)
    def options(implicit context: Context) : Map[String,String] = _options.mapValues(context.evaluate)
    def database(implicit context: Context) : String = context.evaluate(_database)
    def defaultValues(implicit context: Context) : Map[String,String] = _defaultValues.mapValues(context.evaluate)

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
