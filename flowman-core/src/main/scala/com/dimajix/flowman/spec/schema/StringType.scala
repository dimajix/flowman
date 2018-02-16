package com.dimajix.flowman.spec.schema

import org.apache.spark.sql.types.DataType

import com.dimajix.flowman.execution.Context


case object StringType extends FieldType {
    override def sparkType(implicit context: Context) : DataType = org.apache.spark.sql.types.StringType
    override def parse(value:String) : Any = value
    override def interpolate(value: FieldValue, granularity:String) : Iterable[Any] = {
        value match {
            case SingleValue(v) => Seq(v)
            case ArrayValue(values) => values.toSeq
            case RangeValue(start,end) => ???
        }
    }
}
