/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.model

import java.io.File
import java.net.URL
import java.util.Locale
import java.util.regex.Pattern

import scala.annotation.tailrec

import org.apache.hadoop.fs.Path

import com.dimajix.flowman.catalog.TableIdentifier
import com.dimajix.flowman.fs.GlobPattern


object ResourceIdentifier {
    def ofFile(file:Path): GlobbingResourceIdentifier =
        GlobbingResourceIdentifier("file", file.toString)
    def ofLocal(file:Path): GlobbingResourceIdentifier =
        GlobbingResourceIdentifier("local", new Path(file.toUri.getPath).toString)
    def ofLocal(file:File): GlobbingResourceIdentifier =
        GlobbingResourceIdentifier("local", new Path(file.toURI.getPath).toString)
    def ofHiveDatabase(database:String): RegexResourceIdentifier =
        RegexResourceIdentifier("hiveDatabase", database, caseSensitive=false)
    def ofHiveTable(table:TableIdentifier): RegexResourceIdentifier =
        ofHiveTable(table.table, table.space.headOption)
    def ofHiveTable(table:String): RegexResourceIdentifier =
        RegexResourceIdentifier("hiveTable", table, caseSensitive=false)
    def ofHiveTable(table:String, database:Option[String]): RegexResourceIdentifier =
        RegexResourceIdentifier("hiveTable", fqTable(table, database), caseSensitive=false)
    def ofHivePartition(table:TableIdentifier, partition:Map[String,Any]): RegexResourceIdentifier =
        ofHivePartition(table.table, table.space.headOption, partition)
    def ofHivePartition(table:String, partition:Map[String,Any]): RegexResourceIdentifier =
        RegexResourceIdentifier("hiveTablePartition", table, partition.map { case(k,v) => k -> v.toString }, caseSensitive=false)
    def ofHivePartition(table:String, database:Option[String], partition:Map[String,Any]): RegexResourceIdentifier =
        RegexResourceIdentifier("hiveTablePartition", fqTable(table, database), partition.map { case(k,v) => k -> v.toString }, caseSensitive=false)
    def ofJdbcDatabase(database:String): RegexResourceIdentifier =
        RegexResourceIdentifier("jdbcDatabase", database)
    def ofJdbcTable(table:TableIdentifier): RegexResourceIdentifier =
        ofJdbcTable(table.table, table.space.headOption)
    def ofJdbcTable(table:String, database:Option[String]): RegexResourceIdentifier =
        RegexResourceIdentifier("jdbcTable", fqTable(table, database))
    def ofJdbcTable(table:String): RegexResourceIdentifier =
        RegexResourceIdentifier("jdbcTable", table)
    def ofJdbcQuery(query:String): SimpleResourceIdentifier =
        SimpleResourceIdentifier("jdbcQuery", "<sql_query>")
    def ofJdbcTablePartition(table:TableIdentifier, partition:Map[String,Any]): RegexResourceIdentifier =
        ofJdbcTablePartition(table.table, table.space.headOption, partition)
    def ofJdbcTablePartition(table:String, database:Option[String], partition:Map[String,Any]): RegexResourceIdentifier =
        RegexResourceIdentifier("jdbcTablePartition", fqTable(table, database), partition.map { case(k,v) => k -> v.toString })
    def ofURL(url:URL): RegexResourceIdentifier =
        RegexResourceIdentifier("url", url.toString)

    private def fqTable(table:String, database:Option[String]) : String = database.filter(_.nonEmpty).map(_ + ".").getOrElse("") + table
}


/**
 * A ResourceIdentifier is used to identify a physical resource which is either produced or consumed by a
 * target during a lifecycle phase. ResourceIdentifiers therefore play a crucial role in determining the correct
 * execution order of all targets
 */
abstract class ResourceIdentifier extends Product with Serializable {
    val category:String
    val name:String
    val partition:Map[String,String]

    final def isEmpty : Boolean = name.isEmpty
    final def nonEmpty : Boolean = name.nonEmpty

    /**
     * Provides a nice textual representation of the ResourceIdentifier used for console output
     * @return
     */
    def text : String = s"$category:$name[${partition.map(kv => kv._1 + "=" + kv._2).mkString(",")}]"

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
        ps.toSeq.map(keys => withPartition(partition.filterKeys(keys.contains)))
    }

    /**
     * Makes a copy of this resource with a different partition
     * @param partition
     * @return
     */
    def withPartition(partition:Map[String,String]) : ResourceIdentifier

    /**
      * Returns true if this ResourceIdentifier is either equal to the other one or if it describes a resource which
      * actually contains the other one.
      * @param other
      * @return
      */
    final def contains(other:ResourceIdentifier) : Boolean = {
        category == other.category &&
            containsName(other) &&
            containsPartition(other)
    }

    final def intersects(other:ResourceIdentifier) : Boolean = {
        this.contains(other) || other.contains(this)
    }

    protected def containsName(other:ResourceIdentifier) : Boolean = {
        name == other.name
    }

    /**
      * Check that the current partition also holds the partition of the other resource. This is the case if all
      * partition values are also set in the other resource
      * @param other
      * @return
      */
    protected def containsPartition(other:ResourceIdentifier) : Boolean = {
        partition.forall(p => other.partition.get(p._1).contains(p._2))
    }
}


/**
 * This is the simplest ResourceIdentifier, which simply performs exact matches of the resource name
 * @param category
 * @param name
 * @param partition
 */
final case class SimpleResourceIdentifier(override val category:String, override val name:String, override val partition:Map[String,String] = Map())
extends ResourceIdentifier
{
    override def withPartition(partition:Map[String,String]) : ResourceIdentifier = copy(partition=partition)
}


/**
 * This ResourceIdentifier performs matches using globbing logic in order to detect if another resource is contained
 * within this resource.  Globbing only makes sense for file based resources, for other types ypu should either use the
 * SimpleResourceIdentifier or the RegexResourceIdentifier
 * @param category
 * @param name
 * @param partition
 */
final case class GlobbingResourceIdentifier(override val category:String, override val name:String, override val partition:Map[String,String] = Map())
extends ResourceIdentifier
{
    private lazy val globPattern = GlobPattern(name)

    override def withPartition(partition:Map[String,String]) : ResourceIdentifier = copy(partition=partition)

    override protected def containsName(other:ResourceIdentifier) : Boolean = {
        @tailrec
        def isParent(parent:Path, child:Path) : Boolean = {
            if (parent == child) {
                true
            }
            else {
                val c2 = child.getParent
                if (c2 != null)
                    isParent(parent, c2)
                else
                    false
            }
        }

        // Test simple case: Perfect match
        if (name == other.name) {
            true
        }
        // Test if this is parent
        else if (isParent(new Path(name), new Path(other.name))) {
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
}


/**
 * The RegexResourceIdentifier performs matches against other resources using a regular expression. This can be useful
 * for table names or similar resources.
 * @param category
 * @param name
 * @param partition
 */
final case class RegexResourceIdentifier(override val category:String, override val name:String, override val partition:Map[String,String] = Map(), caseSensitive:Boolean=true)
extends ResourceIdentifier
{
    private lazy val regex = {
        val opts =
            if (caseSensitive) Pattern.DOTALL
            else Pattern.DOTALL | Pattern.CASE_INSENSITIVE
        Pattern.compile(name, opts)
    }

    override def withPartition(partition:Map[String,String]) : ResourceIdentifier = copy(partition=partition)

    override protected def containsName(other:ResourceIdentifier) : Boolean = {
        // Test simple case: Perfect match
        if (caseSensitive && name == other.name) {
            true
        }
        else if (!caseSensitive && name.toLowerCase(Locale.ROOT) == other.name.toLowerCase(Locale.ROOT)) {
            true
        }
        // Test if wildcards do match
        else {
            regex.matcher(other.name).matches
        }
    }
}
