package com.dimajix.flowman.server.rest

import com.github.swagger.akka.SwaggerHttpService
import com.github.swagger.akka.model.Info
import io.swagger.models.auth.BasicAuthDefinition


object SwaggerDocService extends SwaggerHttpService  {
    override def apiClasses = Set(
        classOf[NamespaceService],
        classOf[ProjectService],
        classOf[JobHistoryService],
        classOf[TargetHistoryService]
    )
    override def host = ""
    override def basePath: String = "/api/"
    override def apiDocsPath: String = "swagger"
    override def info = Info(version = "1.0")
    // override val externalDocs = Some(new ExternalDocs("Core Docs", "http://acme.com/docs"))
    // override def securitySchemeDefinitions = Map("basicAuth" -> new BasicAuthDefinition())
    override val unwantedDefinitions = Seq()
}
