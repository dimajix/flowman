package com.dimajix.spark

import org.apache.spark.sql.Column

import com.dimajix.spark.expressions.CreateNullableStruct


object functions {
    @scala.annotation.varargs
    def nullable_struct(cols: Column*): Column = new Column(CreateNullableStruct(cols.map(_.expr)))
}
