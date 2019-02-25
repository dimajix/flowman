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

package com.dimajix.flowman.catalog

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils

import com.dimajix.flowman.types.TimestampType
import com.dimajix.flowman.util.UtcTimestamp


object PartitionSpec {
    def apply(values:Seq[(String,Any)]) : PartitionSpec = new PartitionSpec(values.toMap)
}


case class PartitionSpec(values:Map[String,Any]) {
    def toSeq : Seq[(String,Any)] = values.toSeq
    def toMap : Map[String,Any] = values

    /**
      * Returns true if the partition specification is empty
      * @return
      */
    def isEmpty : Boolean = values.isEmpty

    /**
      * Returns true if the partition specification contains values
      * @return
      */
    def nonEmpty : Boolean = values.nonEmpty

    /**
      * Applies a mapping function to the values and returns a map from column name to value
      * @param fn
      * @tparam T
      * @return
      */
    def mapValues[T](fn:(Any) => T) : Map[String,T] = values.mapValues(fn)

    /**
      * Returns a Hadoop path constructed from the partition values
      * @param root
      * @return
      */
    def path(root:Path, columns:Seq[String]) : Path = {
        columns
            .map(col => (col, values(col)))
            .map(nv => ExternalCatalogUtils.getPartitionPathString(nv._1, nv._2.toString))
            .foldLeft(root)((path, segment) => new Path(path, segment))
    }

    /**
      * Creates a SQL PARTITION expression
      * @param columns
      * @return
      */
    def expr(columns:Seq[String]) : String = {
        val partitionValues = columns
            .map(field => fieldSpec(field, values.getOrElse(field, throw new IllegalArgumentException(s"Column $field not defined"))))
        s"PARTITION(${partitionValues.mkString(",")})"
    }

    /**
      * Returns a SQL condition to be used in a WHERE clause
      * @return
      */
    def condition : String = {
        values.map{ case (key,value) =>  fieldSpec(key,value) }.mkString(" AND ")
    }

    private def escapeSql(value: String): String = {
        if (value == null) null
        else StringUtils.replace(value, "'", "''")
    }
    private def fieldSpec(name: String, value:Any) : String = {
        value match {
            case s:String => name + "='" + escapeSql(s) + "'"
            case ts:UtcTimestamp => name + "=" + ts.toEpochSeconds()
            case v:Any => name + "=" + v
        }
    }
}
