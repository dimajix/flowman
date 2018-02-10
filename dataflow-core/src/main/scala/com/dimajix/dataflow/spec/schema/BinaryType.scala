package com.dimajix.dataflow.spec.schema

import org.apache.spark.sql.types.DataType

import com.dimajix.dataflow.execution.Context


case object BinaryType extends FieldType {
    override def sparkType(implicit context: Context) : DataType = org.apache.spark.sql.types.BinaryType
    override def parse(value:String) : Any = ???
    override def interpolate(value: FieldValue, granularity:String) : Iterable[Any] = ???
}
