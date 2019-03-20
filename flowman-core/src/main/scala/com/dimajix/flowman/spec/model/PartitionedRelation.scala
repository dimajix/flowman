/*
 * Copyright 2018 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.model

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.spec.schema.PartitionField
import com.dimajix.flowman.spec.schema.PartitionSchema
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue


trait PartitionedRelation { this:Relation =>
    @JsonProperty(value = "partitions", required = false) private var _partitions: Seq[PartitionField] = Seq()

    def partitions(implicit context: Context): Seq[PartitionField] = _partitions

    /**
      * Applies a partition filter using the given partition values
      * @param df
      * @param partitions
      * @param context
      * @return
      */
    protected def filterPartition(df:DataFrame, partitions: Map[String, FieldValue])(implicit context: Context) : DataFrame = {
        val partitionSchema = PartitionSchema(this.partitions)

        def applyPartitionFilter(df: DataFrame, partitionName: String, partitionValue: FieldValue): DataFrame = {
            val field = partitionSchema.get(partitionName)
            val values = field.interpolate(partitionValue).toSeq
            df.filter(df(partitionName).isin(values: _*))
        }

        partitions.foldLeft(df)((df, pv) => applyPartitionFilter(df, pv._1, pv._2))
    }

    /**
      * Adds partition columns to an existing DataFrame
      *
      */
    protected def addPartition(df:DataFrame, partition:Map[String,SingleValue])(implicit context: Context) : DataFrame = {
        val partitionSchema = PartitionSchema(this.partitions)

        def addPartitioColumn(df: DataFrame, partitionName: String, partitionValue: SingleValue): DataFrame = {
            val field = partitionSchema.get(partitionName)
            val value = field.parse(partitionValue.value)
            df.withColumn(partitionName, lit(value))
        }

        partition.foldLeft(df)((df, pv) => addPartitioColumn(df, pv._1, pv._2))
    }
}
