package com.dimajix.flowman.spec.schema

import java.sql.Date
import java.time.Duration
import java.time.LocalDate
import java.time.temporal.ChronoUnit

import org.apache.spark.sql.types.DataType


case object DurationType extends FieldType {
    override def sparkType : DataType = ???

    /**
      * Parses a String into a java.time.Duration object.
      * @param value
      * @param granularity
      * @return
      */
    override def parse(value:String, granularity: String) : Duration = {
        if (granularity != null && granularity.nonEmpty) {
            val secs = Duration.parse(value).getSeconds
            val step = Duration.parse(granularity).getSeconds
            Duration.ofSeconds(secs / step * step)
        }
        else {
            Duration.parse(value)
        }
    }

    /**
      * Parses a FieldValue into a sequence of java.sql.Timestamp objects. The fields have to be of format
      * "yyyy-MM-dd HH:mm:ss"
      * @param value
      * @param granularity
      * @return
      */
    override def interpolate(value: FieldValue, granularity:String) : Iterable[Duration] = {
        value match {
            case SingleValue(v) => Seq(parse(v, granularity))
            case ArrayValue(values) => values.map(parse(_, granularity))
            case RangeValue(start,end,step) => {
                val startDuration = Duration.parse(start).getSeconds
                val endDuration = Duration.parse(end).getSeconds

                val result = if (step != null && step.nonEmpty) {
                    val range = startDuration.until(endDuration).by(Duration.parse(step).getSeconds)
                    if (granularity != null && granularity.nonEmpty) {
                        val mod = Duration.parse(granularity).getSeconds
                        range.map(_ / mod * mod).distinct
                    }
                    else {
                        range
                    }
                }
                else if (granularity != null && granularity.nonEmpty) {
                    val mod = Duration.parse(granularity).getSeconds
                    (startDuration / mod * mod).until(endDuration / mod * mod).by(mod)
                }
                else {
                    startDuration.until(endDuration)
                }
                result.map(x => Duration.ofSeconds(x))
            }
        }
    }
}
