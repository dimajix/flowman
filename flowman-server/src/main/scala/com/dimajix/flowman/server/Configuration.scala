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

import com.dimajix.flowman.config.FlowmanConf


object Configuration {
    val SERVER_BIND_HOST = "flowman.history.bind.host"
    val SERVER_BIND_PORT = "flowman.history.bind.port"
    val SERVER_BIND_METRICS_PORT = "flowman.history.bind.metrics-port"
    val SERVER_REQUEST_TIMEOUT = "flowman.history.request.timeout"
    val SERVER_IDLE_TIMEOUT = "flowman.history.idle.timeout"
    val SERVER_BIND_TIMEOUT = "flowman.history.bind.timeout"
    val SERVER_LINGER_TIMEOUT = "flowman.history.linger.timeout"
}

class Configuration(conf: FlowmanConf) {
    import Configuration._

    def getBindHost() : String = conf.get(SERVER_BIND_HOST, "0.0.0.0")
    def getBindPort() : Int = conf.get(SERVER_BIND_PORT, "8080").toInt
    def getBindMetricsPort() : Int = conf.get(SERVER_BIND_METRICS_PORT, "8081").toInt

    def getRequestTimeout() : Int = conf.get(SERVER_REQUEST_TIMEOUT, "20").toInt
    def getIdleTimeout() : Int = conf.get(SERVER_IDLE_TIMEOUT, "60").toInt
    def getBindTimeout() : Int = conf.get(SERVER_BIND_TIMEOUT, "1").toInt
    def getLingerTimeout() : Int = conf.get(SERVER_LINGER_TIMEOUT, "60").toInt
}
