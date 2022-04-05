package com.dimajix.flowman.tools.schema.impl

import scala.collection.JavaConverters._

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.BeanProperty
import com.fasterxml.jackson.databind.JavaType
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.introspect.AnnotatedClassResolver
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.ObjectNode
import com.kjetland.jackson.jsonSchema.JsonSchemaConfig
import com.kjetland.jackson.jsonSchema.JsonSchemaGenerator
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle


class MyJsonSchemaGenerator(rootObjectMapper: ObjectMapper, config: JsonSchemaConfig = JsonSchemaConfig.vanillaJsonSchemaDraft4)
  extends JsonSchemaGenerator(rootObjectMapper) {

  override def generateJsonSchema[T <: Any](javaType: JavaType, title: Option[String], description: Option[String]): JsonNode = {

    val rootNode = JsonNodeFactory.instance.objectNode()

    // Specify that this is a v4 json schema
    rootNode.put("$schema", "http://json-schema.org/draft-04/schema#")
    //rootNode.put("id", "http://my.site/myschema#")

    // Add schema title
    title.orElse {
      Some(generateTitleFromPropertyName(javaType.getRawClass.getSimpleName))
    }.flatMap {
      title =>
        // Skip it if specified to empty string
        if (title.isEmpty) None else Some(title)
    }.map {
      title =>
        rootNode.put("title", title)
      // If root class is annotated with @JsonSchemaTitle, it will later override this title
    }

    // Maybe set schema description
    description.map {
      d =>
        rootNode.put("description", d)
      // If root class is annotated with @JsonSchemaDescription, it will later override this description
    }


    val definitionsHandler = new DefinitionsHandler
    val rootVisitor = new PatchedJsonFormatVisitorWrapper(rootObjectMapper, node = rootNode, definitionsHandler = definitionsHandler, currentProperty = None)

    rootObjectMapper.acceptJsonFormatVisitor(javaType, rootVisitor)

    definitionsHandler.getFinalDefinitionsNode().foreach {
      definitionsNode =>
        rootNode.set("definitions", definitionsNode)
        ()
    }

    rootNode

  }


  class PatchedJsonFormatVisitorWrapper(
    objectMapper: ObjectMapper,
    level: Int = 0,
    node: ObjectNode = JsonNodeFactory.instance.objectNode(),
    definitionsHandler: DefinitionsHandler,
    currentProperty: Option[BeanProperty]
  ) extends MyJsonFormatVisitorWrapper(objectMapper, level, node, definitionsHandler, currentProperty) {
    override def expectObjectFormat(_type: JavaType) = {
      val subTypes: List[Class[_]] = extractSubTypes(_type)

      // Check if we have subtypes
      if (subTypes.nonEmpty) {
        // We have subtypes
        //l(s"polymorphism - subTypes: $subTypes")

        val definitionInfo: DefinitionInfo = definitionsHandler.getOrCreateDefinition(_type) { node =>
          val anyOfArrayNode = JsonNodeFactory.instance.arrayNode()
          node.set("oneOf", anyOfArrayNode)

          subTypes.foreach {
            subType: Class[_] =>
              l(s"polymorphism - subType: $subType")
              val definitionInfo: DefinitionInfo = definitionsHandler.getOrCreateDefinition(objectMapper.constructType(subType)) {
                objectNode =>

                  val childVisitor = createChild(objectNode, currentProperty = None)
                  objectMapper.acceptJsonFormatVisitor(tryToReMapType(subType), childVisitor)

                  None
              }

              val thisOneOfNode = JsonNodeFactory.instance.objectNode()
              thisOneOfNode.put("$ref", definitionInfo.ref.get)

              // If class is annotated with JsonSchemaTitle, we should add it
              Option(subType.getDeclaredAnnotation(classOf[JsonSchemaTitle])).map(_.value()).foreach {
                title =>
                  thisOneOfNode.put("title", title)
              }

              anyOfArrayNode.add(thisOneOfNode)

          }

          None
        }

        definitionInfo.ref.foreach {
          r =>
            // Must add ref to def at "this location"
            node.put("$ref", r)
        }

        //definitionInfo.jsonObjectFormatVisitor.orNull

        null // Returning null to stop jackson from visiting this object since we have done it manually
      }
      else {
        super.expectObjectFormat(_type)
      }
    }

    override def createChild(childNode: ObjectNode, currentProperty: Option[BeanProperty]): MyJsonFormatVisitorWrapper = {
      new PatchedJsonFormatVisitorWrapper(objectMapper, level + 1, node = childNode, definitionsHandler = definitionsHandler, currentProperty = currentProperty)
    }

    private def extractSubTypes(_type: JavaType): List[Class[_]] = {
      val ac = AnnotatedClassResolver.resolve(objectMapper.getDeserializationConfig, _type, objectMapper.getDeserializationConfig)

      Option(ac.getAnnotation(classOf[JsonTypeInfo])).map {
        jsonTypeInfo: JsonTypeInfo =>

          jsonTypeInfo.use() match {
            case JsonTypeInfo.Id.NAME =>
              // First we try to resolve types via manually finding annotations (if success, it will preserve the order), if not we fallback to use collectAndResolveSubtypesByClass()
              val subTypes: List[Class[_]] = Option(_type.getRawClass.getDeclaredAnnotation(classOf[JsonSubTypes])).toList.flatMap {
                ann: JsonSubTypes =>
                  // We found it via @JsonSubTypes-annotation
                  ann.value().map {
                    t: JsonSubTypes.Type => t.value()
                  }.toList
              } ++ {
                // We did not find it via @JsonSubTypes-annotation (Probably since it is using mixin's) => Must fallback to using collectAndResolveSubtypesByClass
                val resolvedSubTypes = objectMapper.getSubtypeResolver.collectAndResolveSubtypesByClass(objectMapper.getDeserializationConfig, ac).asScala.toList
                resolvedSubTypes.map(_.getType)
                  .filter(c => _type.getRawClass.isAssignableFrom(c) && _type.getRawClass != c)
              }

              subTypes.distinct

            case _ =>
              // Just find all subclasses
              config.subclassesResolver.getSubclasses(_type.getRawClass)
          }

      }.getOrElse(List())
    }
  }
}
