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

import org.apache.spark.sql.SparkSession
import org.slf4j.Logger


abstract class AbstractExecutor(_session:Session, _context:Context) extends Executor {
    protected val logger:Logger

    override def session: Session = _session

    /**
      * Returns the Context associated with this Executor. This context will be used for looking up
      * databases and relations and for variable substition
      *
      * @return
      */
    override def context: Context = _context

    /**
      * Returns the appropriate runner
      *
      * @return
      */
    override def runner: Runner = session.runner

    /**
      * Returns (or lazily creates) a SparkSession of this Executor. The SparkSession will be derived from the global
      * SparkSession, but a new derived session with a separate namespace will be created.
      *
      * @return
      */
    override def spark: SparkSession = _session.spark

    /**
      * Returns true if a SparkSession is already available
      * @return
      */
    override def sparkRunning: Boolean = _session.sparkRunning
}
