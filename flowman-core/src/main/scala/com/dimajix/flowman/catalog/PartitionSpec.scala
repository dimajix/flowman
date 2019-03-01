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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils

import com.dimajix.common.MapIgnoreCase


object PartitionSpec {
    def apply(values:Seq[(String,Any)]) : PartitionSpec = new PartitionSpec(MapIgnoreCase(values))
    def apply(values:Map[String,Any]) : PartitionSpec = new PartitionSpec(MapIgnoreCase(values))
}


case class PartitionSpec(values:MapIgnoreCase[Any]) {
    def toSeq : Seq[(String,Any)] = values.toSeq
    def toMap : Map[String,Any] = values.toMap

    def apply(name:String) : Any = values(name)

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
            .map(nv => ExternalCatalogUtils.getPartitionPathString(nv._1, nv._2.toString))
            .foldLeft(root)((path, segment) => new Path(path, segment))
    }
}
