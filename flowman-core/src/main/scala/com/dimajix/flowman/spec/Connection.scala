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

package com.dimajix.flowman.spec

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.util.StdConverter

import com.dimajix.flowman.execution.Context


object Connection {
    class NameResolver extends StdConverter[Map[String,Connection],Map[String,Connection]] {
        override def convert(value: Map[String,Connection]): Map[String,Connection] = {
            value.foreach(kv => kv._2._name = kv._1)
            value
        }
    }

    def apply(driver:String, url:String, username:String, password:String, properties:Map[String,String] = Map()) : Connection = {
        val connection = new Connection
        connection._driver = driver
        connection._url = url
        connection._username = username
        connection._password = password
        connection._properties = properties
        connection
    }
}

class Connection {
    @JsonIgnore private var _name:String = ""
    @JsonProperty(value="driver", required=true) private var _driver:String = _
    @JsonProperty(value="url", required=true) private var _url:String = _
    @JsonProperty(value="host", required=true) private var _host:String = _
    @JsonProperty(value="port", required=true) private var _port:String = _
    @JsonProperty(value="keyFile", required=true) private var _keyFile:String = _
    @JsonProperty(value="keyPassword", required=true) private var _keyPassword:String = _
    @JsonProperty(value="username", required=true) private var _username:String = _
    @JsonProperty(value="password", required=true) private var _password:String = _
    @JsonProperty(value="properties") private var _properties:Map[String,String] = Map()

    def name : String = _name
    def driver(implicit context: Context) : String = context.evaluate(_driver)
    def url(implicit context: Context) : String = context.evaluate(_url)
    def host(implicit context: Context) : String = context.evaluate(_host)
    def port(implicit context: Context) : Int = Option(context.evaluate(_port)).map(_.toInt).getOrElse(0)
    def keyFile(implicit context: Context) : String = context.evaluate(_keyFile)
    def keyPassword(implicit context: Context) : String = context.evaluate(_keyPassword)
    def username(implicit context: Context) : String  = context.evaluate(_username)
    def password(implicit context: Context) : String = context.evaluate(_password)
    def properties(implicit context: Context) : Map[String,String] = _properties.mapValues(context.evaluate)
}
