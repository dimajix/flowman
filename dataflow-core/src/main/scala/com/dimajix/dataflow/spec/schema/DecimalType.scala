package com.dimajix.dataflow.spec.schema

import org.apache.spark.sql.types.DataType

import com.dimajix.dataflow.execution.Context


object DecimalType {
    val MAX_PRECISION = 38
    val MAX_SCALE = 38
    val SYSTEM_DEFAULT: DecimalType = DecimalType(MAX_PRECISION, 18)
    val USER_DEFAULT: DecimalType = DecimalType(10, 0)
}
case class DecimalType(precision: Int, scale: Int) extends FieldType {
    override def dtype(implicit context: Context) : DataType = org.apache.spark.sql.types.DecimalType(precision, scale)
}
