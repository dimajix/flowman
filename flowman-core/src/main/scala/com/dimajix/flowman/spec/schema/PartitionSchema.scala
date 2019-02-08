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
import com.dimajix.flowman.types._
import com.dimajix.flowman.util.UtcTimestamp


object PartitionSchema {
    def apply(fields:Seq[PartitionField]) : PartitionSchema = new PartitionSchema(fields)
}


/**
  * Helper class for working with partitioned relations. The class provides convenience methods for creating the
  * correct Hive partition specification and for creating a Hive compatible path.
  * @param fields
  */
class PartitionSchema(fields:Seq[PartitionField]) {
    private val partitionsByName = fields.map(p => (p.name, p)).toMap

    /**
      * Returns the list of partition names
      * @return
      */
    def names : Seq[String] = fields.map(_.name)

    def get(name:String) : PartitionField = {
        partitionsByName.getOrElse(name, throw new IllegalArgumentException(s"Partition ${name}e not defined"))
    }

    /**
      * Parses a given partition
      * @param partition
      * @return
      */
    def parse(partition:Map[String,SingleValue])(implicit context:Context) : Seq[(PartitionField,Any)] = {
        fields.map(field => (field, field.parse(partition.getOrElse(field.name, throw new IllegalArgumentException(s"Missing value for partition '${field.name}'")).value)))
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
      * Constructs a SQL partition specification from the given partitions
      * @param partition
      * @param context
      * @return
      */
    def partitionSpec(partition:Map[String,SingleValue])(implicit context:Context) : String = {
        val partitions = fields.map(p => (p.name, p)).toMap
        val partitionValues = partition.map(kv => {
            val p = partitions.getOrElse(kv._1, throw new IllegalArgumentException(s"Column '${kv._1}' not defined as partition column"))
            p.spec(kv._2.value)
        })
        s"PARTITION(${partitionValues.mkString(",")})"
    }

    /**
      * Resolves the given map of partition values to a map of interpolates values
      * @param partitions
      * @param context
      * @return
      */
    def resolve(partitions: Map[String, FieldValue])(implicit context:Context) : Map[String,Iterable[Any]] = {
        partitions.map(kv => (kv._1, get(kv._1).interpolate(kv._2)))
    }
}
