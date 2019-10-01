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

import java.util.Locale

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.spec.schema.PartitionField
import com.dimajix.flowman.spec.schema.PartitionFieldSpec
import com.dimajix.flowman.spec.schema.PartitionSchema
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue


trait PartitionedRelation { this:Relation =>
    def partitions : Seq[PartitionField]

    /**
      * Applies a partition filter using the given partition values
      * @param df
      * @param partitions
      * @return
      */
    protected def filterPartition(df:DataFrame, partitions: Map[String, FieldValue]) : DataFrame = {
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
    protected def addPartition(df:DataFrame, partition:Map[String,SingleValue]) : DataFrame = {
        val partitionSchema = PartitionSchema(this.partitions)

        def addPartitioColumn(df: DataFrame, partitionName: String, partitionValue: SingleValue): DataFrame = {
            val field = partitionSchema.get(partitionName)
            val value = field.parse(partitionValue.value)
            df.withColumn(partitionName, lit(value))
        }

        partition.foldLeft(df)((df, pv) => addPartitioColumn(df, pv._1, pv._2))
    }

    protected def requireAllPartitionKeys(map: Map[String,_]) : Unit = {
        val partitionKeys = partitions.map(_.name.toLowerCase(Locale.ROOT)).toSet
        val valueKeys = map.keys.map(_.toLowerCase(Locale.ROOT)).toSet
        valueKeys.foreach(key => if (!partitionKeys.contains(key)) throw new IllegalArgumentException(s"Specified partition '$key' not defined in relation '$identifier'"))
        partitionKeys.foreach(key => if (!valueKeys.contains(key)) throw new IllegalArgumentException(s"Value for partition '$key' missing for relation '$identifier'"))
    }

    protected def requireValidPartitionKeys(map: Map[String,_]) : Unit = {
        val partitionKeys = partitions.map(_.name.toLowerCase(Locale.ROOT)).toSet
        val valueKeys = map.keys.map(_.toLowerCase(Locale.ROOT)).toSet
        valueKeys.foreach(key => if (!partitionKeys.contains(key)) throw new IllegalArgumentException(s"Specified partition '$key' not defined in relation '$identifier'"))
    }
}



trait PartitionedRelationSpec { this: RelationSpec =>
    @JsonProperty(value = "partitions", required = false) protected var partitions: Seq[PartitionFieldSpec] = Seq()
}
