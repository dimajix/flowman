/*
 * Copyright (C) 2021 The Flowman Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dimajix.spark.sql

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkShim
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
import org.apache.spark.util.LongAccumulator

import com.dimajix.spark.sql.catalyst.plans.logical.CountRecords
import com.dimajix.spark.sql.expressions.CreateNullableStruct


class functions
object functions {
    @scala.annotation.varargs
    def nullable_struct(cols: Column*): Column = new Column(CreateNullableStruct(cols.map(_.expr)))

    def count_records(df:DataFrame, counter:LongAccumulator) : DataFrame =
        DataFrameBuilder.ofRows(df.sparkSession, CountRecords(df.queryExecution.logical, counter))


    type FunctionDescription = (FunctionIdentifier, ExpressionInfo, FunctionBuilder)
    def wrap(fn:(Column) => Column, name:String) : FunctionDescription = {
        (
            FunctionIdentifier(name),
            new ExpressionInfo(classOf[functions].getCanonicalName, name),
            exprs => fn(new Column(exprs(0))).expr
        )
    }
    def wrap(fn:(Column, Column) => Column, name:String) : FunctionDescription = {
        (
            FunctionIdentifier(name),
            new ExpressionInfo(classOf[functions].getCanonicalName, name),
            exprs => fn(new Column(exprs(0)), new Column(exprs(1))).expr
        )
    }
    def wrap(fn:(Column, Column, Column) => Column, name:String) : FunctionDescription = {
        (
            FunctionIdentifier(name),
            new ExpressionInfo(classOf[functions].getCanonicalName, name),
            exprs => fn(new Column(exprs(0)), new Column(exprs(1)), new Column(exprs(2))).expr
        )
    }
    def wrap(fn:(Column, Column, Column, Column) => Column, name:String) : FunctionDescription = {
        (
            FunctionIdentifier(name),
            new ExpressionInfo(classOf[functions].getCanonicalName, name),
            exprs => fn(new Column(exprs(0)), new Column(exprs(1)), new Column(exprs(2)), new Column(exprs(3))).expr
        )
    }

    def register(spark:SparkSession, fn:FunctionDescription) : Unit = {
        val (id, info, builder) = fn
        SparkShim.functionRegistry(spark).registerFunction(id, info, builder)
    }
    def register(spark:SparkSession, fn:(Column) => Column, name:String) : Unit = {
        register(spark, wrap(fn, name))
    }
    def register(spark:SparkSession, fn:(Column, Column) => Column, name:String) : Unit = {
        register(spark, wrap(fn, name))
    }
    def register(spark:SparkSession, fn:(Column, Column, Column) => Column, name:String) : Unit = {
        register(spark, wrap(fn, name))
    }
    def register(spark:SparkSession, fn:(Column, Column, Column, Column) => Column, name:String) : Unit = {
        register(spark, wrap(fn, name))
    }
}
