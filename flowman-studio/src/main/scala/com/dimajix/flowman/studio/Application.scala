/*
 * Copyright 2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.studio

import com.dimajix.flowman.tools.Logging
import com.dimajix.flowman.tools.Tool


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
    def run() : Boolean = {
        val session = createSession(
            sparkMaster = "",
            sparkName = "flowman-server",
            disableSpark = true
        )

        //val project = loadProject(new Path(options.projectFile))

        true
    }
}
