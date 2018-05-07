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


abstract class AbstractExecutor(_context:Context, sessionFactory:() => Option[SparkSession]) extends Executor {
    protected val logger:Logger
    private var _session:Option[SparkSession] = None

    /**
      * Returns the Context associated with this Executor. This context will be used for looking up
      * databases and relations and for variable substition
      *
      * @return
      */
    override def context: Context = _context

    /**
      * Returns (or lazily creates) a SparkSession of this Executor. The SparkSession will be derived from the global
      * SparkSession, but a new derived session with a separate namespace will be created.
      *
      * @return
      */
    override def spark: SparkSession = {
        if (_session.isEmpty) {
            logger.info("Creating new local Spark session for executor")
            _session = sessionFactory()
            //_session.foreach(session => context.config.foreach(kv => session.conf.set(kv._1, kv._2)))
        }
        _session.get
    }

    /**
      * Returns true if a SparkSession is already available
      * @return
      */
    override def sparkRunning: Boolean = _session.nonEmpty

    /**
      * Creates a new Executor which uses a different context
      *
      * @param context
      * @return
      */
    override def withContext(context:Context) : Executor = new ScopedExecutor(this, context)
}
