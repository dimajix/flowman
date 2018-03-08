package com.dimajix.flowman.spec.schema

import org.apache.spark.sql.types.DataType


case class VarcharType(length: Int) extends FieldType {
    override def sparkType : DataType = org.apache.spark.sql.types.VarcharType(length)
    override def sqlType : String = s"varchar($length)"

    override def parse(value:String) : Any = value
    override def interpolate(value: FieldValue, granularity:String) : Iterable[Any] = {
        value match {
            case SingleValue(v) => Seq(v)
            case ArrayValue(values) => values.toSeq
            case RangeValue(start,end) => ???
        }
    }
}
