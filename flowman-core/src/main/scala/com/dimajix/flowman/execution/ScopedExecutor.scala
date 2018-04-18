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

package com.dimajix.flowman.execution

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import com.dimajix.flowman.spec.TableIdentifier


/**
  * This special Executor is meant to be used with Jobs. It mainly encapsulates a new context
  *
  * @param _parent
  * @param _context
  */
class ScopedExecutor(_parent:Executor, _context:Context) extends Executor {
    /**
      * Returns (or lazily creates) a SparkSession of this Executor. The SparkSession will be derived from the global
      * SparkSession, but a new derived session with a separate namespace will be created.
      *
      * @return
      */
    override def spark: SparkSession = _parent.spark

    /**
      * Returns true if a SparkSession is already available
      *
      * @return
      */
    override def sparkRunning: Boolean = _parent.sparkRunning

    /**
      * Returns the Context associated with this Executor. This context will be used for looking up
      * databases and relations and for variable substition
      *
      * @return
      */
    override def context: Context = _context

    /**
      * Returns a map of all temporary tables created by this executor
      *
      * @return
      */
    override def tables: Map[TableIdentifier, DataFrame] = _parent.tables

    /**
      * Returns a named table created by an executor. If a project is specified, Executors for other projects
      * will be searched as well
      *
      * @param identifier
      * @return
      */
    override def getTable(identifier: TableIdentifier): DataFrame = _parent.getTable(identifier)

    /**
      * Creates an instance of a table of a Dataflow, or retrieves it from cache
      *
      * @param identifier
      */
    override def instantiate(identifier: TableIdentifier): DataFrame = _parent.instantiate(identifier)
    override def cleanup(): Unit = _parent.cleanup()

    /**
      * Creates a new Executor which uses a different context
      *
      * @param context
      * @return
      */
    override def withContext(context:Context) : Executor = new ScopedExecutor(_parent, context)
}
