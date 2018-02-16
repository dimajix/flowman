package com.dimajix.flowman

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonTypeInfo.As
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id
import com.fasterxml.jackson.annotation.JsonTypeName
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.jsontype.NamedType
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.scalatest.FlatSpec
import org.scalatest.Matchers


@JsonTypeInfo(use = Id.NAME, include = As.PROPERTY, property = "type")
@JsonSubTypes(Array(new Type(name = "annotated", value = classOf[AnnotatedFoo])))
abstract class Foo {}
class AnnotatedFoo extends Foo {
    override def toString = "AnnotatedFoo"
}
@JsonTypeName("registered")
class RegisteredFoo extends Foo {
    override def toString = "RegisteredFoo"
}
class UnnamedRegisteredFoo extends Foo {
    override def toString = "UnnamedRegisteredFoo"
}
class FooWrapper {
    @JsonProperty var foo:Foo = _
}



class JacksonSubtypeTest extends FlatSpec with Matchers {
    private val WRAPPED_ANNOTATED_FOO ="""{"foo": {"type":"annotated"}}"""
    private val WRAPPED_REGISTERED_FOO ="""{"foo": {"type":"registered"}}"""
    private val ANNOTATED_FOO = "{\"type\": \"annotated\"}"
    private val REGISTERED_FOO = "{\"type\": \"registered\"}"
    private val UNNAMED_REGISTERED_FOO = "{\"type\": \"unnamed_registered\"}"

    "Jackson" should "deserialize annotated classes" in {
        val mapper = new ObjectMapper()
        val annoFoo = mapper.readValue(ANNOTATED_FOO, classOf[Foo])
        annoFoo should not be (null)
    }
    it should "deserialize wrapped annotated classes" in {
        val mapper = new ObjectMapper()
        val annoFoo = mapper.readValue(WRAPPED_ANNOTATED_FOO, classOf[FooWrapper])
        annoFoo.foo should not be (null)
    }

    it should "deserialize registered classes" in {
        val onlyRegMapper = new ObjectMapper()
        onlyRegMapper.registerSubtypes(classOf[RegisteredFoo])
        val regFoo = onlyRegMapper.readValue(REGISTERED_FOO, classOf[Foo])
        regFoo should not be (null)
    }
    it should "deserialize wrapped registered classes" in {
        val mapper = new ObjectMapper()
        mapper.registerSubtypes(classOf[RegisteredFoo])
        val regFoo = mapper.readValue(WRAPPED_REGISTERED_FOO, classOf[FooWrapper])
        regFoo.foo should not be (null)
    }

    it should "deserialize annotated and registered classes" in {
        val mapper = new ObjectMapper()
        mapper.registerSubtypes(classOf[RegisteredFoo])
        val annoFoo = mapper.readValue(ANNOTATED_FOO, classOf[Foo])
        annoFoo should not be (null)
        val regFoo = mapper.readValue(REGISTERED_FOO, classOf[Foo])
        regFoo should not be (null)
    }

    it should "deserialize annotated and explicitly named registered classes" in {
        val mapper = new ObjectMapper()
        mapper.registerModule(DefaultScalaModule)
        mapper.registerSubtypes(new NamedType(classOf[UnnamedRegisteredFoo], "unnamed_registered"))
        val annoFoo = mapper.readValue(UNNAMED_REGISTERED_FOO, classOf[Foo])
        annoFoo should not be (null)
    }
}
