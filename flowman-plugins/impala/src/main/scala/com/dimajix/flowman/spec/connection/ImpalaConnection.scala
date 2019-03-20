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

import com.dimajix.flowman.annotation.ConnectionType
import com.dimajix.flowman.catalog.ImpalaExternalCatalog
import com.dimajix.flowman.execution.Context


@ConnectionType(kind = "impala")
class ImpalaConnection extends Connection {
    @JsonProperty(value="username", required=false) private var _username:String = _
    @JsonProperty(value="password", required=false) private var _password:String = _
    @JsonProperty(value="properties", required=false) private var _properties:Map[String,String] = Map()
    @JsonProperty(value="driver", required=false) private var _driver:String = ImpalaExternalCatalog.IMPALA_DEFAULT_DRIVER
    @JsonProperty(value="host", required=false) private var _host:String = "localhost"
    @JsonProperty(value="port", required=false) private var _port:String = ImpalaExternalCatalog.IMPALA_DEFAULT_PORT.toString

    def username(implicit context: Context) : String  = context.evaluate(_username)
    def password(implicit context: Context) : String = context.evaluate(_password)
    def properties(implicit context: Context) : Map[String,String] = _properties.mapValues(context.evaluate)
    def driver(implicit context: Context) : String = context.evaluate(_driver)
    def host(implicit context: Context) : String = context.evaluate(_host)
    def port(implicit context: Context) : Int = context.evaluate(_port).toInt
}
