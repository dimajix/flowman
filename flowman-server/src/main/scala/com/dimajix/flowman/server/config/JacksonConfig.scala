package com.dimajix.flowman.server.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import javax.ws.rs.ext.ContextResolver
import javax.ws.rs.ext.Provider


@Provider
class JacksonConfig extends ContextResolver[ObjectMapper] {
    private val objectMapper = new ObjectMapper()
        .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
        .registerModule(new DefaultScalaModule)
        .registerModule(new JavaTimeModule)

    override def getContext(`type`: Class[_]): ObjectMapper = objectMapper
}
