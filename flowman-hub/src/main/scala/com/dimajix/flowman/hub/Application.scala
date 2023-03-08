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

package com.dimajix.flowman.hub

import com.dimajix.flowman.common.Logging
import com.dimajix.flowman.hub.rest.Server


object Application {
    def main(args: Array[String]) : Unit = {
        Logging.init()

        val server = new Application()
        val result = server.run()
        System.exit(if (result) 0 else 1)
    }

}


class Application {
    def run() : Boolean = {
        val conf = Configuration.loadDefaults()

        val server = new Server(conf)
        server.run()

        true
    }
}
