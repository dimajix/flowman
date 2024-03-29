/*
 * Copyright (C) 2019 The Flowman Authors
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

package com.dimajix.flowman.server

import java.io.File

import com.dimajix.flowman.Tool
import com.dimajix.flowman.common.Logging
import com.dimajix.flowman.common.ToolConfig
import com.dimajix.flowman.model.Namespace
import com.dimajix.flowman.server.rest.Server
import com.dimajix.flowman.server.rest.Server


object Application {
    def main(args: Array[String]) : Unit = {
        java.lang.System.setProperty("akka.http.server.remote-address-header", "true")

        Logging.init()

        val server = new Application()
        val result = server.run()
        System.exit(if (result) 0 else 1)
    }

}


class Application extends Tool {
    override protected def loadNamespace() : Namespace = {
        // TODO: Fall back to resources if nothing is found
        val ns = ToolConfig.confDirectory
            .map(confDir => new File(confDir, "history-server.yml"))
            .filter(_.isFile)
            .orElse(
                ToolConfig.confDirectory
                    .map(confDir => new File(confDir, "default-namespace.yml"))
                    .filter(_.isFile)
            )
            .map(file => Namespace.read.file(file))
            .getOrElse(Namespace.read.default())

        // Load all plugins from Namespace
        ns.plugins.foreach(pluginManager.load)
        ns
    }

    def run() : Boolean = {
        val session = createSession(
            sparkMaster = "",
            sparkName = "Flowman History Server",
            disableSpark = true
        )

        val conf = new Configuration(session.flowmanConf)
        val server = new Server(conf, session)
        server.run()

        true
    }
}
