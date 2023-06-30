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

package com.dimajix.flowman.spec.history

import java.sql.SQLRecoverableException
import java.sql.SQLTransientException

import com.dimajix.flowman.execution.StateStoreException
import com.dimajix.flowman.jdbc.SlickUtils



object JdbcStateStore {
    case class Connection(
        url:String,
        driver:String,
        user:Option[String] = None,
        password:Option[String] = None,
        properties: Map[String,String] = Map()
    )
}


case class JdbcStateStore(connection:JdbcStateStore.Connection, retries:Int=3, timeout:Int=1000) extends RepositoryStateStore {


    /**
      * Performs some a task with a JDBC session, also automatically performing retries and timeouts
      *
      * @param query
      * @tparam T
      * @return
      */
    override protected def withRepository[T](query: JdbcStateRepository => T) : T = {
        def retry[T](n:Int)(fn: => T) : T = {
            try {
                fn
            } catch {
                case e @(_:SQLRecoverableException|_:SQLTransientException) if n > 1 =>
                    logger.warn("Retrying after error while executing SQL: {}", e.getMessage)
                    Thread.sleep(timeout)
                    retry(n - 1)(fn)
                case ex:Throwable =>
                    throw new StateStoreException("Error accessing JDBC state store repository", ex)
            }
        }

        retry(retries) {
            ensureTables()
            query(repository)
        }
    }

    private var tablesCreated:Boolean = false
    private lazy val repository = new JdbcStateRepository(connection, SlickUtils.getProfile(connection.driver))

    private def ensureTables() : Unit = {
        // Create Database if not exists
        if (!tablesCreated) {
            repository.create()
            tablesCreated = true
        }
    }

}
