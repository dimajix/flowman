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

package com.dimajix.flowman.spec.connection

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.flowman.execution.Context


object JdbcConnection {
    def apply(driver:String, url:String, username:String, password:String, properties:Map[String,String] = Map()) : Connection = {
        val connection = new JdbcConnection
        connection._driver = driver
        connection._url = url
        connection._username = username
        connection._password = password
        connection._properties = properties
        connection
    }
}


class JdbcConnection extends Connection  {
    @JsonProperty(value="driver", required=true) private var _driver:String = ""
    @JsonProperty(value="url", required=true) private var _url:String = ""
    @JsonProperty(value="username", required=false) private var _username:String = _
    @JsonProperty(value="password", required=false) private var _password:String = _
    @JsonProperty(value="properties", required=false) private var _properties:Map[String,String] = Map()

    def driver(implicit context: Context) : String = context.evaluate(_driver)
    def url(implicit context: Context) : String = context.evaluate(_url)
    def username(implicit context: Context) : String  = context.evaluate(_username)
    def password(implicit context: Context) : String = context.evaluate(_password)
    def properties(implicit context: Context) : Map[String,String] = _properties.mapValues(context.evaluate)
}
