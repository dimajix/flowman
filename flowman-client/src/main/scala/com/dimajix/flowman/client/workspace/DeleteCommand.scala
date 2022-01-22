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

package com.dimajix.flowman.client.workspace

import java.net.URI

import scala.Console.out

import org.apache.http.client.methods.HttpDelete
import org.apache.http.impl.client.CloseableHttpClient
import org.kohsuke.args4j.Argument
import org.slf4j.LoggerFactory

import com.dimajix.flowman.client.Command


class DeleteCommand extends Command {
    private val logger = LoggerFactory.getLogger(classOf[DeleteCommand])

    @Argument(usage = "specifies the workspace to delete", metaVar = "<workspace>", required = true)
    var workspace: String = ""

    override def execute(httpClient:CloseableHttpClient, baseUri:URI) : Boolean = {
        val request = new HttpDelete(baseUri.resolve(s"workspace/$workspace"))
        query(httpClient, request)
        true
    }
}
