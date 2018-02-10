package com.dimajix.dataflow.spec.schema

import org.apache.spark.sql.types.DataType

import com.dimajix.dataflow.execution.Context


case object ShortType extends FieldType {
    override def sparkType(implicit context: Context) : DataType = org.apache.spark.sql.types.ShortType
    override def parse(value:String) : Any = value.toShort
    override def interpolate(value: FieldValue, granularity:String) : Iterable[Any] = {
        value match {
            case SingleValue(v) => Seq(parse(v))
            case ArrayValue(values) => values.map(parse)
            case RangeValue(start,end) => {
                if (granularity != null && granularity.nonEmpty)
                    start.toShort until end.toShort by granularity.toShort map(_.toShort)
                else
                    start.toShort until end.toShort map(_.toShort)
            }
        }
    }
}
