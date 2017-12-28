package com.dimajix.dataflow.spec.flow

import scala.collection.JavaConversions._
import java.util.ServiceLoader

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.jsontype.NamedType
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

import com.dimajix.dataflow.execution.Context
import com.dimajix.dataflow.spi.MappingProvider


object Mapping {
    def getProviders() = {
        val loader = ServiceLoader.load(classOf[MappingProvider])
        loader.iterator().toSeq
    }
}


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "aggregate", value = classOf[AggregateMapping]),
    new JsonSubTypes.Type(name = "alias", value = classOf[AliasMapping]),
    new JsonSubTypes.Type(name = "read", value = classOf[InputMapping]),
    new JsonSubTypes.Type(name = "write", value = classOf[OutputMapping]),
    new JsonSubTypes.Type(name = "repartition", value = classOf[RepartitionMapping]),
    new JsonSubTypes.Type(name = "sort", value = classOf[SortMapping]),
    new JsonSubTypes.Type(name = "extend", value = classOf[SqlMapping]),
    new JsonSubTypes.Type(name = "filter", value = classOf[FilterMapping]),
    new JsonSubTypes.Type(name = "project", value = classOf[ProjectMapping]),
    new JsonSubTypes.Type(name = "sql", value = classOf[ExtendMapping]),
    new JsonSubTypes.Type(name = "union", value = classOf[UnionMapping])
))
abstract class Mapping {
    @JsonProperty("broadcast") private[spec] var _broadcast:String = "false"
    @JsonProperty("cache") private[spec] var _cache:String = "NONE"

    def broadcast(implicit context: Context) : Boolean = context.evaluate(_broadcast).toBoolean
    def cache(implicit context: Context) : StorageLevel = StorageLevel.fromString(context.evaluate(_cache))

    /**
      * Executes this Transform and returns a corresponding DataFrame
      * @param context
      * @return
      */
    def execute(implicit context:Context) : DataFrame

    /**
      * Returns the dependencies (i.e. names of tables in the Dataflow model)
      * @param context
      * @return
      */
    def dependencies(implicit context:Context) : Array[String]
}
