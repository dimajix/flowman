package com.dimajix.flowman.spec.schema

import java.sql.Timestamp
import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneOffset

import org.apache.spark.sql.types.DataType


case object TimestampType extends FieldType {
    override def sparkType : DataType = org.apache.spark.sql.types.TimestampType

    override def parse(value:String) : Any = new Timestamp(LocalDateTime.parse(value).toEpochSecond(ZoneOffset.UTC)*1000l)
    override def interpolate(value: FieldValue, granularity:String) : Iterable[Any] = {
        value match {
            case SingleValue(v) => Seq(parse(v))
            case ArrayValue(values) => values.map(parse)
            case RangeValue(start,end) => {
                val startDate = LocalDateTime.parse(start).toEpochSecond(ZoneOffset.UTC)
                val endDate = LocalDateTime.parse(end).toEpochSecond(ZoneOffset.UTC)
                val step = if (granularity != null && granularity.nonEmpty)
                    Duration.parse(granularity).getSeconds
                else
                    1
                startDate until endDate by step map(x => new Timestamp(x * 1000l))
            }
        }
    }
}
