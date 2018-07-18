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

package com.dimajix.flowman.spec.schema

import org.apache.hadoop.fs.Path

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.types.CharType
import com.dimajix.flowman.types.VarcharType
import com.dimajix.flowman.types.DateType
import com.dimajix.flowman.types.DoubleType
import com.dimajix.flowman.types.FloatType
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.LongType
import com.dimajix.flowman.types.ShortType
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.TimestampType


object PartitionSchema {
    def apply(fields:Seq[PartitionField]) : PartitionSchema = new PartitionSchema(fields)
}


/**
  * Helper class for working with partitioned relations. The class provides convenience methods for creating the
  * correct Hive partition specification and for creating a Hive compatible path.
  * @param fields
  */
class PartitionSchema(fields:Seq[PartitionField]) {
    /**
      * Returns the list of partition names
      * @return
      */
    def names : Seq[String] = fields.map(_.name)

    /**
      * Parses a given partition
      * @param partition
      * @return
      */
    def parse(partition:Map[String,SingleValue])(implicit context:Context) : Seq[(PartitionField,Any)] = {
        fields.map(field => (field, field.parse(partition.getOrElse(field.name, throw new IllegalArgumentException(s"Missing value for partition '$field.name'")).value)))
    }

    /**
      * Constructs a partition path from the specified partitions.
      * @param root
      * @param partition
      * @return
      */
    def partitionPath(root:Path, partition:Map[String,SingleValue])(implicit context:Context) : Path = {
        parse(partition)
            .map(nv => nv._1.name + "=" + nv._2)
            .foldLeft(root)((path, segment) => new Path(path, segment))
    }

    /**
      * Constructs a SQL partition specification from the given partisions
      * @param partition
      * @param context
      * @return
      */
    def partitionSpec(partition:Map[String,SingleValue])(implicit context:Context) : String = {
        val partitions = fields.map(p => (p.name, p)).toMap
        val partitionValues = partition.map(kv => {
            val p = partitions.getOrElse(kv._1, throw new IllegalArgumentException(s"Column '${kv._1}' not defined as partition column"))
            p.ftype match {
                case IntegerType => kv._1 + "=" + p.parse(kv._2.value)
                case LongType => kv._1 + "=" + p.parse(kv._2.value)
                case ShortType => kv._1 + "=" + p.parse(kv._2.value)
                case FloatType => kv._1 + "=" + p.parse(kv._2.value)
                case DoubleType => kv._1 + "=" + p.parse(kv._2.value)
                case DateType => kv._1 + "='" + p.parse(kv._2.value) + "'"
                case StringType => kv._1 + "='" + kv._2.value + "'"
                case CharType(_) => kv._1 + "='" + kv._2.value + "'"
                case VarcharType(_) => kv._1 + "='" + kv._2.value + "'"
                case TimestampType => kv._1 + "=" + TimestampType.parse(kv._2.value, p.granularity).toEpochSeconds()
            }
        })
        s"PARTITION(${partitionValues.mkString(",")})"
    }
}
