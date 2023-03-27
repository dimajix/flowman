/*
 * Copyright (C) 2022 The Flowman Authors
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

package com.dimajix.flowman.spec.hook

import java.net.URL

import scala.util.control.NonFatal
import scala.util.matching.Regex

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.Hook
import com.dimajix.common.text.ConsoleColors.yellow


case class RestHook(
    instanceProperties: Hook.Properties,
    condition: Map[String,Seq[Regex]],
    url: String
) extends ConditionalBaseHook(condition) {
    override protected def invoke(vars: Map[String, String], env: Map[String, AnyRef]): Unit = {
        val url = context.environment.evaluate(this.url, env)
        try {
            val niceUrl = {
                val u = new URL(url)
                val result = new StringBuffer()
                result.append(u.getProtocol)
                result.append(":")
                if (u.getAuthority != null && u.getAuthority.nonEmpty) {
                    result.append("//")
                    result.append(u.getAuthority)
                }

                if (u.getPath != null) {
                    result.append(u.getPath)
                }
                result
            }
            logger.info(s"Invoking external web-hook: $niceUrl with extra args $vars")
            val httpClient = HttpClients.createDefault()
            val httpGet = new HttpGet(url)
            httpClient.execute(httpGet)
        }
        catch {
            case NonFatal(ex) => logger.warn(yellow(s"Could not post status to url '$url': ${ex.toString}"))
        }
    }
}


class RestHookSpec extends HookSpec {
    @JsonProperty(value="when", required=false) private var condition:Map[String,Seq[String]] = Map.empty
    @JsonProperty(value="url", required=false) private var url:String = ""

    override def instantiate(context: Context, properties:Option[Hook.Properties] = None): RestHook = {
        RestHook(
            instanceProperties(context, properties),
            condition.map { case(k,v) => k -> v.map(_.r) },
            url
        )
    }
}
