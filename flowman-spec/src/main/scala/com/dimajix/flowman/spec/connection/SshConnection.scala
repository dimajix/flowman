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

package com.dimajix.flowman.spec.connection

import java.io.File

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.BaseConnection
import com.dimajix.flowman.model.Connection


case class SshConnection(
    instanceProperties:Connection.Properties,
    host:String,
    port:Int,
    keyFile:Option[File] = None,
    keyPassword:Option[String] = None,
    username:String = "",
    password:Option[String] = None,
    knownHosts:Option[File] = None
)
extends BaseConnection {}


class SshConnectionSpec extends ConnectionSpec {
    @JsonProperty(value="host", required=true) private var host:String = _
    @JsonProperty(value="port", required=false) private var port:String = "22"
    @JsonProperty(value="keyFile", required=false) private var keyFile:Option[String] = None
    @JsonProperty(value="keyPassword", required=false) private var keyPassword:Option[String] = None
    @JsonProperty(value="username", required=true) private var username:String = _
    @JsonProperty(value="password", required=false) private var password:Option[String] = None
    @JsonProperty(value="knownHosts", required=false) private var knownHosts:Option[String] = None

    /**
      * Creates the instance of the specified Connection with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context, properties:Option[Connection.Properties] = None): SshConnection = {
        SshConnection(
            instanceProperties(context, properties),
            context.evaluate(host),
            context.evaluate(port).toInt,
            context.evaluate(keyFile).filter(_.nonEmpty).map(f => new File(f)),
            context.evaluate(keyPassword),
            context.evaluate(username),
            context.evaluate(password),
            context.evaluate(knownHosts).filter(_.nonEmpty).map(f => new File(f))
        )
    }
}
