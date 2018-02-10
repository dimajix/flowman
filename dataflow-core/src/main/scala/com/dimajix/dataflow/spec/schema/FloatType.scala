package com.dimajix.dataflow.spec.schema

import org.apache.spark.sql.types.DataType

import com.dimajix.dataflow.execution.Context


case object FloatType extends FieldType {
    override def dtype(implicit context: Context) : DataType = org.apache.spark.sql.types.FloatType
}
