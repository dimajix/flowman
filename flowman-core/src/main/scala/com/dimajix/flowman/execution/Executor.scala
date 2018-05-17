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


abstract class Executor {
    def session: Session

    /**
      * Returns (or lazily creates) a SparkSession of this Executor. The SparkSession will be derived from the global
      * SparkSession, but a new derived session with a separate namespace will be created.
      *
      * @return
      */
    def spark: SparkSession

    /**
      * Returns true if a SparkSession is already available
      * @return
      */
    def sparkRunning: Boolean

    /**
      * Returns the Context associated with this Executor. This context will be used for looking up
      * databases and relations and for variable substition
      *
      * @return
      */
    def context : Context

    /**
      * Returns a map of all temporary tables created by this executor
      *
      * @return
      */
    def tables : Map[TableIdentifier,DataFrame]

    /**
      * Returns a named table created by an executor. If a project is specified, Executors for other projects
      * will be searched as well
      *
      * @param identifier
      * @return
      */
    def getTable(identifier: TableIdentifier) : DataFrame

    /**
      * Creates an instance of a table of a Dataflow, or retrieves it from cache
      *
      * @param identifier
      */
    def instantiate(identifier: TableIdentifier) : DataFrame

    /**
      * Releases any temporary tables
      */
    def cleanup() : Unit
}
