package com.dimajix.dataflow.spec.flow

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.storage.StorageLevel

import com.dimajix.dataflow.execution.Context

/**
  * Common base implementation for the MappingType interface
  */
abstract class BaseMapping extends Mapping {
    @JsonProperty("broadcast") private[spec] var _broadcast:String = "false"
    @JsonProperty("cache") private[spec] var _cache:String = "NONE"

    def broadcast(implicit context: Context) : Boolean = context.evaluate(_broadcast).toBoolean
    def cache(implicit context: Context) : StorageLevel = StorageLevel.fromString(context.evaluate(_cache))
}
