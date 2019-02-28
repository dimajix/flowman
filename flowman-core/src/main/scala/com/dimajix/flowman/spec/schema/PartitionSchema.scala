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

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path

import com.dimajix.flowman.catalog.PartitionSpec
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
class PartitionSchema(val fields:Seq[PartitionField]) {
    private val partitionsByName = fields.map(p => (p.name, p)).toMap

    /**
      * Returns the list of partition names
      * @return
      */
    def names : Seq[String] = fields.map(_.name)

    def get(name:String) : PartitionField = {
        partitionsByName.getOrElse(name, throw new IllegalArgumentException(s"Partition $name not defined"))
    }

    /**
      * Parses a given partition and returns a PartitionSpec
      * @param partition
      * @return
      */
    def spec(partition:Map[String,SingleValue])(implicit context:Context) : PartitionSpec = {
        val map = fields
            .map(field => (field, partition.getOrElse(field.name, throw new IllegalArgumentException(s"Missing value for partition '${field.name}'")).value))
            .map{ case (field,value) => (field.name, field.parse(value)) }
            .toMap
        PartitionSpec(map)
    }

    /**
      * Interpolates the given map of partition values to a map of interpolates values
      * @param partitions
      * @param context
      * @return
      */
    def interpolate(partitions: Map[String, FieldValue])(implicit context:Context) : Iterable[PartitionSpec] = {
        val values = fields.map { field =>
            field.name -> field.interpolate(partitions(field.name))
        }

        def recurse(head:Seq[(String,Any)], tail:Seq[(String,Iterable[Any])]) : Iterable[PartitionSpec] = {
            tail match {
                case th :: tt => th._2.flatMap(elem => recurse(head :+ (th._1, elem), tt))
                case Seq() => Some(PartitionSpec(head.toMap))
            }
        }

        recurse(Seq(), values)
    }
}
