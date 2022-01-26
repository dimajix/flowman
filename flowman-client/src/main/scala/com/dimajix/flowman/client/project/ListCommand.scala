/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.client.project

import java.net.URI

import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.CloseableHttpClient
import org.kohsuke.args4j.Argument
import org.slf4j.LoggerFactory

import com.dimajix.flowman.client.Command


class ListCommand extends Command {
    private val logger = LoggerFactory.getLogger(classOf[ListCommand])

    @Argument(usage = "specifies the workspace to list all projects", metaVar = "<workspace>", required = false)
    var workspace: String = "default"

    override def execute(httpClient:CloseableHttpClient, baseUri:URI) : Boolean = {
        val request = new HttpGet(baseUri.resolve(s"workspace/$workspace/project"))
        query(httpClient, request)
        true
    }
}
