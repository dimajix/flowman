package com.dimajix.flowman.spec.schema

import org.apache.spark.sql.types.DataType

import com.dimajix.flowman.execution.Context


case object ByteType extends FieldType {
    override def sparkType(implicit context: Context) : DataType = org.apache.spark.sql.types.ByteType
    override def parse(value:String) : Any = value.toByte
    override def interpolate(value: FieldValue, granularity:String) : Iterable[Any] = {
        value match {
            case SingleValue(v) => Seq(parse(v))
            case ArrayValue(values) => values.map(parse)
            case RangeValue(start,end) => {
                if (granularity != null && granularity.nonEmpty)
                    start.toByte until end.toByte by granularity.toByte map(_.toByte)
                else
                    start.toByte until end.toByte map(_.toByte)
            }
        }
    }
}
