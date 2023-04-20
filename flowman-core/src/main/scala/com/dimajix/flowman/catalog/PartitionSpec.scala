/*
 * Copyright (C) 2018 The Flowman Authors
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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils

import com.dimajix.common.MapIgnoreCase
import com.dimajix.flowman.jdbc.HiveDialect
import com.dimajix.flowman.types.FieldValue


object PartitionSpec {
    def apply() : PartitionSpec = new PartitionSpec(MapIgnoreCase())
    def apply(values:Seq[(String,Any)]) : PartitionSpec = new PartitionSpec(MapIgnoreCase(values))
    def apply(values:Map[String,Any]) : PartitionSpec = new PartitionSpec(MapIgnoreCase(values))
}


final case class PartitionSpec(values:MapIgnoreCase[Any]) {
    def toSeq : Seq[(String,Any)] = values.toSeq
    def toMap : Map[String,Any] = values.toMap

    def apply(name:String) : Any = values(name)

    def keys : Iterable[String] = values.keys

    def get(name:String) : Option[Any] = values.get(name)

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
    def mapValues[T](fn:(Any) => T) : MapIgnoreCase[T] = values.mapValues(fn)

    /**
      * Returns a Hadoop path constructed from the partition values
      * @param root
      * @return
      */
    def path(root:Path, columns:Seq[String]) : Path = {
        columns
            .map(col => values.getKeyValue(col))
            .map { case (key,value) =>
                val str = FieldValue.asString(value)
                ExternalCatalogUtils.getPartitionPathString(key, str)
            }
            .foldLeft(root)((path, segment) => new Path(path, segment))
    }

    /**
     * Returns a nice and human readable representation of the partition spec
     * @return
     */
    def spec : String = {
        def str(any:Any) : String = {
            any match {
                case s:String => "'" + s + "'"
                case _ => any.toString
            }
        }

        values.map(kv => kv._1 + "=" + str(kv._2)).mkString("(",", ",")")
    }

    /**
     * Returns a valid SQL predicate as a string
     * @return
     */
    def predicate : String = {
        values.map { case (k, v) => k + "=" + HiveDialect.literal(v) }.mkString(" AND ")
    }

    /**
     * Returns a map with filter conditions to be used with the Hive Metastore catalog
     * @return
     */
    def catalogPartition : Map[String,String] = mapValues(FieldValue.asString).toMap

    override def toString: String = {
        "PartitionSpec" + spec
    }
}
