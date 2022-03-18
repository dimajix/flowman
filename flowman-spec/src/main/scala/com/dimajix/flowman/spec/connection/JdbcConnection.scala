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

package com.dimajix.flowman.spec.connection

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.BaseConnection
import com.dimajix.flowman.model.Connection


case class JdbcConnection(
    instanceProperties:Connection.Properties,
    url:String,
    driver:String,
    username:Option[String] = None,
    password:Option[String] = None,
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
        con.url = url
        con.driver = driver
        con.username = Some(username)
        con.password = Some(password)
        con.properties = properties
        con
    }
}

class JdbcConnectionSpec extends ConnectionSpec  {
    @JsonProperty(value="url", required=true) private var url:String = ""
    @JsonProperty(value="driver", required=true) private var driver:String = ""
    @JsonProperty(value="username", required=false) private var username:Option[String] = None
    @JsonProperty(value="password", required=false) private var password:Option[String] = None
    @JsonProperty(value="properties", required=false) private var properties:Map[String,String] = Map()

    /**
      * Creates the instance of the specified JdbcConnection with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context, props:Option[Connection.Properties] = None): JdbcConnection = {
        JdbcConnection(
            instanceProperties(context, props),
            context.evaluate(url),
            context.evaluate(driver),
            username.map(context.evaluate),
            password.map(context.evaluate),
            context.evaluate(properties)
        )
    }
}
