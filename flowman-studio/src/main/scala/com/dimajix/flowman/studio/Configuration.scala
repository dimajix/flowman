/*
 * Copyright 2019-2022 Kaya Kupferschmidt
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

import java.io.FileInputStream
import java.net.URL
import java.util.Properties

import org.apache.hadoop.fs.Path


object Configuration {
    val SERVER_BIND_HOST = "studio.server.bind.host"
    val SERVER_BIND_PORT = "studio.server.bind.port"
    val SERVER_REQUEST_TIMEOUT = "studio.server.request.timeout"
    val SERVER_IDLE_TIMEOUT = "studio.server.idle.timeout"
    val SERVER_BIND_TIMEOUT = "studio.server.bind.timeout"
    val SERVER_LINGER_TIMEOUT = "studio.server.linger.timeout"
    val WORKSPACE_ROOT = "studio.server.workspace.root"
    val STUDIO_ID = "studio.server.id"
    val HUB_URL = "studio.hub.url"
    val HUB_SECRET = "studio.hub.secret"

    private def defaultProperties() : Properties = {
        val loader = Thread.currentThread.getContextClassLoader
        val url = loader.getResource("com/dimajix/flowman/studio/flowman-studio.properties")
        val properties = new Properties()
        properties.load(url.openStream())
        properties
    }

    /**
     * Load a Configuration from a Properties file
     * @param filename
     * @return
     */
    def load(filename:String) : Configuration= {
        val properties = defaultProperties()
        properties.load(new FileInputStream(filename))
        new Configuration(properties)
    }

    /**
     * Loads built-in default configuration
     * @return
     */
    def loadDefaults() : Configuration = {
        val properties = defaultProperties()
        new Configuration(properties)
    }
}

class Configuration(properties: Properties) {
    import Configuration._

    def setBindHost(host: String) : Configuration = {
        properties.setProperty(SERVER_BIND_HOST, host)
        this
    }
    def setBindPort(port: Int) : Configuration = {
        properties.setProperty(SERVER_BIND_PORT, port.toString)
        this
    }
    def setStudioId(id: String) : Configuration = {
        properties.setProperty(STUDIO_ID, id)
        this
    }
    def setHubUrl(url: String) : Configuration = {
        properties.setProperty(HUB_URL, url)
        this
    }
    def setHubSecret(secret: String) : Configuration = {
        properties.setProperty(HUB_SECRET, secret)
        this
    }
    def setWorkspaceRoot(root: String) : Configuration = {
        properties.setProperty(WORKSPACE_ROOT, root)
        this
    }

    def getBindHost() : String = properties.getProperty(SERVER_BIND_HOST, "0.0.0.0")
    def getBindPort() : Int = properties.getProperty(SERVER_BIND_PORT, "8080").toInt

    def getRequestTimeout() : Int = properties.getProperty(SERVER_REQUEST_TIMEOUT, "20").toInt
    def getIdleTimeout() : Int = properties.getProperty(SERVER_IDLE_TIMEOUT, "60").toInt
    def getBindTimeout() : Int = properties.getProperty(SERVER_BIND_TIMEOUT, "1").toInt
    def getLingerTimeout() : Int = properties.getProperty(SERVER_LINGER_TIMEOUT, "60").toInt

    def getStudioId() : String = properties.getProperty(STUDIO_ID, "")

    def getHubUrl() : Option[URL] = {
        Some(properties.getProperty(HUB_URL, "http://localhost:8080").trim)
            .filter(_.nonEmpty)
            .map (new URL(_))
    }
    def getHubSecret() : Option[String] = {
        Some(properties.getProperty(HUB_SECRET, "").trim)
            .filter(_.nonEmpty)
    }
    def getWorkspaceRoot() : Path = {
        val path = Some(properties.getProperty(HUB_URL, "").trim)
            .filter(_.nonEmpty)
            .getOrElse(".")
        new Path(path)
    }
}
