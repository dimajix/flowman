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

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.history.StateStore


object JdbcStateStore {
    case class Connection(
        url:String,
        driver:String,
        user:Option[String] = None,
        password:Option[String] = None,
        properties: Map[String,String] = Map()
    )

    def apply(context: Context, connection: Connection): JdbcStateStore = {
        JdbcStateStore(
            StateStore.Properties(context, "jdbc"),
            connection
        )
    }
}


case class JdbcStateStore(
    override val instanceProperties:StateStore.Properties,
    connection:JdbcStateStore.Connection,
    retries:Int=3,
    timeout:Int=1000
) extends RepositoryStateStore {
    override lazy val repository : StateRepository = new RetryingStateRepository(
            new JdbcStateRepository(connection, retries, timeout)
        )
}
