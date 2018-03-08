package com.dimajix.flowman.spec.schema

import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.CalendarInterval


case object CalendarIntervalType extends FieldType {
    override def sparkType : DataType = org.apache.spark.sql.types.CalendarIntervalType

    override def parse(value:String) : Any = CalendarInterval.fromString(value)
    override def interpolate(value: FieldValue, granularity:String) : Iterable[Any] = {
        value match {
            case SingleValue(v) => Seq(CalendarInterval.fromString(v))
            case ArrayValue(values) => values.map(CalendarInterval.fromString)
            case RangeValue(start,end) => ???
        }
    }
}
