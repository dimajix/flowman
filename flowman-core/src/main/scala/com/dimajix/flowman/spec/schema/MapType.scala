package com.dimajix.flowman.spec.schema

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.types.DataType


case class MapType(
      @JsonProperty(value="keyType") keyType:FieldType,
      @JsonProperty(value="valueType") valueType:FieldType
) extends ContainerType {
    override def sparkType : DataType = {
        org.apache.spark.sql.types.MapType(keyType.sparkType, valueType.sparkType)
    }

    override def parse(value:String) : Any = ???
    override def interpolate(value: FieldValue, granularity:String) : Iterable[Any] = ???
}
