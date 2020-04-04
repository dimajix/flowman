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

package com.dimajix.flowman.spec.history

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.history.JdbcStateStore
import com.dimajix.flowman.history.StateStore
import com.dimajix.flowman.model.ConnectionIdentifier
import com.dimajix.flowman.spec.connection.JdbcConnection


object JdbcHistorySpec {
    def apply(connection:String, retries:Int=3, timeout:Int=1000) : JdbcHistorySpec = {
        val runner = new JdbcHistorySpec
        runner._connection = connection
        runner._retries = retries.toString
        runner._timeout = timeout.toString
        runner
    }
}


class JdbcHistorySpec extends HistorySpec {
    @JsonProperty(value="connection", required=true) private var _connection:String = ""
    @JsonProperty(value="retries", required=false) private var _retries:String = "3"
    @JsonProperty(value="timeout", required=false) private var _timeout:String = "1000"

    override def instantiate(context: Context): StateStore = {
        val conId = ConnectionIdentifier.parse(context.evaluate(_connection))
        val retries = context.evaluate(_retries).toInt
        val timeout = context.evaluate(_timeout).toInt

        val con = context.getConnection(conId).asInstanceOf[JdbcConnection]
        val connection = JdbcStateStore.Connection(
            con.url,
            con.driver,
            con.username,
            con.password,
            con.properties
        )
        new JdbcStateStore(connection, retries, timeout)
    }
}
