/*
 * Copyright 2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.transforms

import scala.collection.immutable.ListMap

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

import com.dimajix.common.MapIgnoreCase
import com.dimajix.common.SetIgnoreCase
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.SchemaUtils
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.StructType


object StackTransformer {
    def apply(
        nameColumn:String,
        valueColumn:String,
        stackColumns:Seq[String],
        dropNulls:Boolean
    ) : StackTransformer = {
        new StackTransformer(
            nameColumn,
            valueColumn,
            ListMap(stackColumns.map(c => c -> c):_*),
            dropNulls
        )
    }
}
final case class StackTransformer(
    nameColumn:String,
    valueColumn:String,
    stackColumns:ListMap[String,String],
    dropNulls:Boolean = true
) extends Transformer {
    /**
     * Transform a Spark DataFrame
     *
     * @param df
     * @return
     */
    override def transform(df: DataFrame): DataFrame = {
        val result = stackColumns.toSeq.map { case(column,value) =>
                df.withColumn(nameColumn, lit(value))
                    .withColumn(valueColumn, df(column))
                    .drop(stackColumns.keys.toSeq:_*)
            }.reduce(_.union(_))

        if (dropNulls)
            result.filter(result(valueColumn).isNotNull)
        else
            result
    }

    /**
     * Transform a Flowman schema
     *
     * @param schema
     * @return
     */
    override def transform(schema: StructType): StructType = {
        val fieldsByName = MapIgnoreCase(schema.fields.map(f => f.name -> f))
        val stackColumnNames = SetIgnoreCase(stackColumns.keySet)
        val commonType = stackColumns.keys.map(fieldsByName)
            .map(_.ftype)
            .reduce((l,r) => SchemaUtils.coerce(l,r))
        val fields = schema.fields.filterNot(f => stackColumnNames.contains(f.name)) :+
            Field(nameColumn, StringType, nullable=false) :+
            Field(valueColumn, commonType, nullable=true)

        StructType(fields)
    }
}
