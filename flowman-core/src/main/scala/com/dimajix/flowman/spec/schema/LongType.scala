package com.dimajix.flowman.spec.schema

import org.apache.spark.sql.types.DataType

import com.dimajix.flowman.execution.Context


case object LongType extends FieldType {
    override def sparkType(implicit context: Context) : DataType = org.apache.spark.sql.types.LongType
    override def parse(value:String) : Any = value.toLong
    override def interpolate(value: FieldValue, granularity:String) : Iterable[Any] = {
        value match {
            case SingleValue(v) => Seq(parse(v))
            case ArrayValue(values) => values.map(parse)
            case RangeValue(start,end) => {
                if (granularity != null && granularity.nonEmpty)
                    start.toLong until end.toLong by granularity.toLong
                else
                    start.toLong until end.toLong
            }
        }
    }
}
