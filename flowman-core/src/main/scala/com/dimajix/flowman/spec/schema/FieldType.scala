package com.dimajix.flowman.spec.schema

import java.util.Locale

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import org.apache.spark.sql.types.DataType

import com.dimajix.flowman.execution.Context


@JsonDeserialize(using=classOf[FieldTypeDeserializer])
abstract class FieldType {
    /**
      * The Spark type to use
      *
      * @param context
      * @return
      */
    def sparkType(implicit context: Context) : DataType

    /**
      * Short Type Name as used in SQL and in YAML specification files
      * @return
      */
    def typeName : String = {
        this.getClass.getSimpleName
        .stripSuffix("$").stripSuffix("Type")
        .toLowerCase(Locale.ROOT)
    }

    def parse(value:String) : Any
    def interpolate(value: FieldValue, granularity:String) : Iterable[Any]
}

@JsonDeserialize
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "array", value = classOf[ArrayType]),
    new JsonSubTypes.Type(name = "map", value = classOf[MapType]),
    new JsonSubTypes.Type(name = "struct", value = classOf[StructType])
))
abstract class ContainerType extends FieldType {

}


private object FieldTypeDeserializer {
    private val nonDecimalNameToType = {
        Seq(NullType, DateType, TimestampType, BinaryType, IntegerType, BooleanType, LongType,
            DoubleType, FloatType, ShortType, ByteType, StringType, CalendarIntervalType)
            .map(t => t.typeName -> t).toMap ++
        Map("int" -> IntegerType)
    }

    private val FIXED_DECIMAL = """decimal\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)""".r
    private val VARCHAR = """varchar\(\s*(\d+)\s*\)""".r

    def deserialize(jp: JsonParser, ctxt: DeserializationContext): FieldType = {
        jp.getCurrentToken match {
            case JsonToken.VALUE_STRING => {
                jp.getText.toLowerCase match {
                    case "decimal" => DecimalType.USER_DEFAULT
                    case FIXED_DECIMAL (precision, scale) => DecimalType(precision.toInt, scale.toInt)
                    case VARCHAR(length) => VarcharType(length.toInt)
                    case other => nonDecimalNameToType.getOrElse (
                        other,
                        throw new JsonMappingException(jp, s"Failed to convert the JSON string '${jp.getText}' to a field type.")
                    )
                }
            }
            case JsonToken.START_OBJECT => {
                jp.readValueAs(classOf[ContainerType])
            }
            case _ => throw new JsonMappingException(jp, "Wrong type for FieldType")
        }
    }
}


private class FieldTypeDeserializer(vc:Class[_]) extends StdDeserializer[FieldType](vc) {
    import java.io.IOException

    def this() = this(null)

    @throws[IOException]
    @throws[JsonProcessingException]
    def deserialize(jp: JsonParser, ctxt: DeserializationContext): FieldType = {
        FieldTypeDeserializer.deserialize(jp, ctxt)
    }
}
