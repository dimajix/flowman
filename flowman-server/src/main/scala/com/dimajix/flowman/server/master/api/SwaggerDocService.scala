/*
 * Copyright 2018 Kaya Kupferschmidt
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

package com.dimajix.flowman.server.master.api

import com.github.swagger.akka.SwaggerHttpService
import com.github.swagger.akka.model.Info
import io.swagger.models.auth.BasicAuthDefinition


object SwaggerDocService extends SwaggerHttpService  {
    override def apiClasses = Set(
        classOf[ActionService],
        classOf[ModelService],
        classOf[MappingService],
        classOf[ProjectService],
        classOf[NamespaceService])
    override def host = ""
    override def basePath: String = "/api/v1/"
    override def apiDocsPath: String = "api-docs"
    override def info = Info(version = "1.0")
    // override val externalDocs = Some(new ExternalDocs("Core Docs", "http://acme.com/docs"))
    override def securitySchemeDefinitions = Map("basicAuth" -> new BasicAuthDefinition())
    override val unwantedDefinitions = Seq("Function1", "Function1RequestContextFutureRouteResult")
}
