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

package com.dimajix.flowman.spec.connection

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.BaseConnection
import com.dimajix.flowman.model.Connection


case class JdbcConnection(
    instanceProperties:Connection.Properties,
    driver:String,
    url:String,
    username:String,
    password:String,
    properties:Map[String,String] = Map()
) extends BaseConnection {
}



object JdbcConnectionSpec {
    /**
      * Convenience constructor mainly used in unit tests
      * @param driver
      * @param url
      * @param username
      * @param password
      * @param properties
      * @return
      */
    def apply(driver:String, url:String, username:String, password:String, properties:Map[String,String] = Map()) : JdbcConnectionSpec = {
        val con = new JdbcConnectionSpec()
        con.driver = driver
        con.url = url
        con.username = username
        con.password = password
        con.properties = properties
        con
    }
}

class JdbcConnectionSpec extends ConnectionSpec  {
    @JsonProperty(value="driver", required=true) private var driver:String = ""
    @JsonProperty(value="url", required=true) private var url:String = ""
    @JsonProperty(value="username", required=false) private var username:String = _
    @JsonProperty(value="password", required=false) private var password:String = _
    @JsonProperty(value="properties", required=false) private var properties:Map[String,String] = Map()

    /**
      * Creates the instance of the specified JdbcConnection with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): JdbcConnection = {
        JdbcConnection(
            instanceProperties(context),
            context.evaluate(driver),
            context.evaluate(url),
            context.evaluate(username),
            context.evaluate(password),
            context.evaluate(properties)
        )
    }
}
