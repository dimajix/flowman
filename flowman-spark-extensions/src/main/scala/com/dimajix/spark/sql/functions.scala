package com.dimajix.spark.sql

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.util.LongAccumulator

import com.dimajix.spark.expressions.CreateNullableStruct
import com.dimajix.spark.sql.catalyst.plans.logical.CountRecords


object functions {
    @scala.annotation.varargs
    def nullable_struct(cols: Column*): Column = new Column(CreateNullableStruct(cols.map(_.expr)))

    def count_records(df:DataFrame, counter:LongAccumulator) : DataFrame =
        DataFrameUtils.ofRows(df.sparkSession, CountRecords(df.queryExecution.logical, counter))
}
