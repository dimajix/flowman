package com.dimajix.dataflow.spec.schema

import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.CalendarInterval

import com.dimajix.dataflow.execution.Context


case object CalendarIntervalType extends FieldType {
    override def sparkType(implicit context: Context) : DataType = org.apache.spark.sql.types.CalendarIntervalType
    override def parse(value:String) : Any = CalendarInterval.fromString(value)
    override def interpolate(value: FieldValue, granularity:String) : Iterable[Any] = {
        value match {
            case SingleValue(v) => Seq(CalendarInterval.fromString(v))
            case ArrayValue(values) => values.map(CalendarInterval.fromString)
            case RangeValue(start,end) => ???
        }
    }
}
