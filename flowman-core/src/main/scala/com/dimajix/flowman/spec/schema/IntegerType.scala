package com.dimajix.flowman.spec.schema

import org.apache.spark.sql.types.DataType

import com.dimajix.flowman.execution.Context


case object IntegerType extends FieldType {
    override def sparkType(implicit context: Context) : DataType = org.apache.spark.sql.types.IntegerType
    override def parse(value:String) : Any = value.toInt
    override def interpolate(value: FieldValue, granularity:String) : Iterable[Any] = {
        value match {
            case SingleValue(v) => Seq(parse(v))
            case ArrayValue(values) => values.map(parse)
            case RangeValue(start,end) => {
                if (granularity != null && granularity.nonEmpty)
                    start.toInt until end.toInt by granularity.toInt
                else
                    start.toInt until end.toInt
            }
        }
    }
}
