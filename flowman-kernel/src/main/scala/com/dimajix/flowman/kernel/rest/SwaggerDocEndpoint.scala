package com.dimajix.flowman.kernel.rest

import com.github.swagger.akka.SwaggerHttpService
import com.github.swagger.akka.model.Info
import io.swagger.models.auth.BasicAuthDefinition


object SwaggerDocEndpoint extends SwaggerHttpService  {
    override def apiClasses = Set(
        classOf[ShutdownEndpoint],
        classOf[PingEndpoint],
        classOf[NamespaceEndpoint],
        classOf[SessionEndpoint],
        classOf[JobEndpoint]
    )
    override def host = ""
    override def basePath: String = "/api/"
    override def apiDocsPath: String = "swagger"
    override def info = Info(version = "1.0")
    // override val externalDocs = Some(new ExternalDocs("Core Docs", "http://acme.com/docs"))
    // override def securitySchemeDefinitions = Map("basicAuth" -> new BasicAuthDefinition())
    override val unwantedDefinitions = Seq()
}
