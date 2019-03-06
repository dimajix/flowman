/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.state

import com.fasterxml.jackson.annotation.JsonProperty
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.ConnectionIdentifier
import com.dimajix.flowman.spec.connection.JdbcConnection
import com.dimajix.flowman.state.JdbcStateStore
import com.dimajix.flowman.state.StateStore


object JdbcStateStoreProvider {
    def apply(connection:String, retries:Int=3, timeout:Int=1000) : JdbcStateStoreProvider = {
        val runner = new JdbcStateStoreProvider
        runner._connection = connection
        runner._retries = retries.toString
        runner._timeout = timeout.toString
        runner
    }
}


class JdbcStateStoreProvider extends StateStoreProvider {
    private val logger = LoggerFactory.getLogger(classOf[JdbcStateStoreProvider])

    @JsonProperty(value="connection", required=true) private var _connection:String = ""
    @JsonProperty(value="retries", required=false) private var _retries:String = "3"
    @JsonProperty(value="timeout", required=false) private var _timeout:String = "1000"

    def connection(implicit context: Context) : ConnectionIdentifier = ConnectionIdentifier.parse(context.evaluate(_connection))
    def retries(implicit context: Context) : Int = context.evaluate(_retries).toInt
    def timeout(implicit context: Context) : Int = context.evaluate(_timeout).toInt

    override def createStateStore(session: Session): StateStore = {
        implicit val context = session.context
        val con = context.getConnection(this.connection).asInstanceOf[JdbcConnection]
        val connection = new JdbcStateStore.Connection(
            con.url,
            con.driver,
            con.username,
            con.password,
            con.properties
        )
        new JdbcStateStore(connection, retries, timeout)
    }
}
