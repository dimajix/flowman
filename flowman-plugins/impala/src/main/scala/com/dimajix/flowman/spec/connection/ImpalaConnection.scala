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

import com.dimajix.flowman.catalog.ImpalaExternalCatalog
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.BaseConnection
import com.dimajix.flowman.model.Connection
import com.dimajix.flowman.spec.annotation.ConnectionType


case class ImpalaConnection(
    instanceProperties:Connection.Properties,
    username:Option[String],
    password:Option[String],
    properties:Map[String,String],
    driver:String,
    host:String,
    port:Int
) extends BaseConnection {
}



@ConnectionType(kind = "impala")
class ImpalaConnectionSpec extends ConnectionSpec {
    @JsonProperty(value="username", required=false) private var username:Option[String] = None
    @JsonProperty(value="password", required=false) private var password:Option[String] = None
    @JsonProperty(value="properties", required=false) private var properties:Map[String,String] = Map()
    @JsonProperty(value="driver", required=false) private var driver:String = ImpalaExternalCatalog.IMPALA_DEFAULT_DRIVER
    @JsonProperty(value="host", required=false) private var host:String = "localhost"
    @JsonProperty(value="port", required=false) private var port:String = ImpalaExternalCatalog.IMPALA_DEFAULT_PORT.toString


    /**
      * Creates an instance of this specification and performs the interpolation of all variables
      *
      * @param context
      * @return
      */
    override def instantiate(context: Context): Connection = {
        ImpalaConnection(
            instanceProperties(context),
            username.map(context.evaluate),
            password.map(context.evaluate),
            context.evaluate(properties),
            context.evaluate(driver),
            context.evaluate(host),
            context.evaluate(port).toInt
        )
    }
}
