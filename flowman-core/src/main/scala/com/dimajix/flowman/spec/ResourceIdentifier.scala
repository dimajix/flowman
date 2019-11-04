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

package com.dimajix.flowman.spec

import java.net.URL

import scala.annotation.tailrec

import org.apache.hadoop.fs.Path

import com.dimajix.flowman.hadoop.GlobPattern


object ResourceIdentifier {
    def ofFile(file:Path) = ResourceIdentifier("file", file.toString)
    def ofLocal(file:Path) = ResourceIdentifier("local", file.toString)
    def ofHiveDatabase(database:String) = ResourceIdentifier("hiveDatabase", database)
    def ofHiveTable(table:String) = ResourceIdentifier("hiveTable", table)
    def ofHiveTable(table:String, database:Option[String]) = ResourceIdentifier("hiveTable", database.map(_ + ".").getOrElse("") + table)
    def ofHivePartition(table:String, database:Option[String], partition:Map[String,Any]) = ResourceIdentifier("hiveTablePartition", database.map(_ + ".").getOrElse("") + table, partition.map { case(k,v) => k -> v.toString })
    def ofURL(url:URL) = ResourceIdentifier("url", url.toString)
}

case class ResourceIdentifier(category:String, name:String, partition:Map[String,String] = Map()) {
    private lazy val globPattern = GlobPattern(name)

    def isEmpty : Boolean = name.isEmpty
    def nonEmpty : Boolean = name.nonEmpty

    /**
      * Create new ResourceIdentifiers by exploding the powerset of all partitions
      * @return
      */
    def explodePartitions() : Seq[ResourceIdentifier] = {
        @tailrec
        def pwr(t: Set[String], ps: Set[Set[String]]): Set[Set[String]] =
            if (t.isEmpty) ps
            else pwr(t.tail, ps ++ (ps map (_ + t.head)))

        val ps = pwr(partition.keySet, Set(Set.empty[String])) //Powerset of ∅ is {∅}
        ps.toSeq.map(keys => copy(partition = partition.filterKeys(keys.contains)))
    }

    /**
      * Returns true if this ResourceIdentifier is either equal to the other one or if it describes a resource which
      * actually contains the other one.
      * @param other
      * @return
      */
    def contains(other:ResourceIdentifier) : Boolean = {
        category == other.category &&
            containsName(other) &&
            containsPartition(other)
    }

    private def containsName(other:ResourceIdentifier) : Boolean = {
        // Test simple case: Perfect match
        if (name == other.name) {
            true
        }
        // Test if wildcards do match
        else if (globPattern.hasWildcard) {
            globPattern.matches(other.name)
        }
        else {
            false
        }
    }

    /**
      * Check that the current partition also holds the partition of the other resource. This is the case if all
      * partition values are also set in the other resource
      * @param other
      * @return
      */
    private def containsPartition(other:ResourceIdentifier) : Boolean = {
        partition.forall(p => other.partition.get(p._1).contains(p._2))
    }
}
