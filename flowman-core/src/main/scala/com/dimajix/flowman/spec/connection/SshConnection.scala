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

import java.io.File

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.flowman.execution.Context

class SshConnection extends Connection {
    @JsonProperty(value="host", required=true) private var _host:String = _
    @JsonProperty(value="port", required=false) private var _port:String = _
    @JsonProperty(value="keyFile", required=false) private var _keyFile:String = _
    @JsonProperty(value="keyPassword", required=false) private var _keyPassword:String = _
    @JsonProperty(value="username", required=true) private var _username:String = _
    @JsonProperty(value="password", required=false) private var _password:String = _
    @JsonProperty(value="knownHosts", required=false) private var _knownHosts:String = _

    def host(implicit context: Context) : String = context.evaluate(_host)
    def port(implicit context: Context) : Int = Option(context.evaluate(_port)).map(_.toInt).getOrElse(22)
    def keyFile(implicit context: Context) : File = Option(context.evaluate(_keyFile)).filter(_.nonEmpty).map(f => new File(f)).orNull
    def keyPassword(implicit context: Context) : String = context.evaluate(_keyPassword)
    def username(implicit context: Context) : String  = context.evaluate(_username)
    def password(implicit context: Context) : String = context.evaluate(_password)
    def knownHosts(implicit context: Context) : File = Option(context.evaluate(_knownHosts)).filter(_.nonEmpty).map(f => new File(f)).orNull
}
