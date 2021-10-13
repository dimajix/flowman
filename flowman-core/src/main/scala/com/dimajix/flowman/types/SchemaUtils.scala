/*
 * Copyright 2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.types

import java.util.Locale

import scala.language.existentials


object SchemaUtils {

    /**
     * This will normalize a given schema in the sense that all field names are converted to lowercase and all
     * metadata is stripped except the comments
     * @param schema
     * @return
     */
    def normalize(schema:StructType) : StructType = {
        com.dimajix.flowman.types.StructType(schema.fields.map(normalize))
    }
    private def normalize(field:Field) : Field = {
        Field(field.name.toLowerCase(Locale.ROOT), normalize(field.ftype), field.nullable, description=field.description)
    }
    private def normalize(dtype:FieldType) : FieldType = {
        dtype match {
            case struct:StructType => normalize(struct)
            case array:ArrayType => ArrayType(normalize(array.elementType),array.containsNull)
            case map:MapType => MapType(normalize(map.keyType), normalize(map.valueType), map.containsNull)
            case dt:FieldType => dt
        }
    }

    /**
     * Replaces all occurances of VarChar and Char types by String types.
     * @param schema
     * @return
     */
    def replaceCharVarchar(schema:StructType) : StructType = {
        StructType(schema.fields.map(replaceCharVarchar))
    }
    def replaceCharVarchar(field:Field) : Field = {
        field.copy(ftype = replaceCharVarchar(field.ftype))
    }
    private def replaceCharVarchar(dtype:FieldType) : FieldType = {
        dtype match {
            case struct:StructType => replaceCharVarchar(struct)
            case array:ArrayType => ArrayType(replaceCharVarchar(array.elementType),array.containsNull)
            case map:MapType => MapType(replaceCharVarchar(map.keyType), replaceCharVarchar(map.valueType), map.containsNull)
            case _:CharType => StringType
            case _:VarcharType => StringType
            case dt:FieldType => dt
        }
    }

    /**
     * Verify is a given source field is compatible with a given target field, i.e. if a safe conversion is
     * possible from a source field to a target field
     * @param sourceField
     * @param targetField
     * @return
     */
    def isCompatible(sourceField:Field, targetField:Field) : Boolean = {
        if (sourceField.name.toLowerCase(Locale.ROOT) != targetField.name.toLowerCase(Locale.ROOT)) {
            false
        }
        else {
            val sourceNullable = sourceField.nullable || sourceField.ftype == NullType
            val targetNullable = targetField.nullable || targetField.ftype == NullType
            if (sourceNullable && !targetNullable) {
                false
            }
            else {
                val coercedType = coerce(sourceField.ftype, targetField.ftype)
                coercedType == targetField.ftype
            }
        }
    }

    private def coerceNumericTypes(left:NumericType[_], right:NumericType[_]) : NumericType[_] = {
        class TypeInfo
        case class FractionalType(dt:NumericType[_], precision:Int, scale:Int) extends TypeInfo
        case class IntegralType(dt:NumericType[_], size:Int) extends TypeInfo { def precision:Int = math.round(math.ceil(size*8*math.log(2)/math.log(10))).toInt }
        def info(t:NumericType[_]) : TypeInfo = {
            t match {
                case ByteType => IntegralType(t, 1)
                case ShortType => IntegralType(t, 2)
                case IntegerType => IntegralType(t, 4)
                case LongType => IntegralType(t, 8)
                case FloatType => FractionalType(t, 4,10)
                case DoubleType => FractionalType(t, 8,20)
                case dt:DecimalType => FractionalType(t, dt.precision, dt.scale)
            }
        }

        val leftInfo = info(left)
        val rightInfo = info(right)

        (leftInfo, rightInfo) match {
            // DECIMALs are promoted to bigger type
            case (FractionalType(DecimalType(lp,ls), _, _), FractionalType(DecimalType(rp,rs), _, _)) =>
                val lIntegerPrecision = lp - ls
                val lFractionalPrecision = ls
                val rIntegerPrecision = rp - rs
                val rFractionalPrecision = rs
                val totalIntegerPrecision = scala.math.max(lIntegerPrecision, rIntegerPrecision)
                val totalFractionalPrecision = scala.math.max(lFractionalPrecision, rFractionalPrecision)
                val totalPrecision = scala.math.min(totalIntegerPrecision + totalFractionalPrecision, DecimalType.MAX_PRECISION)
                DecimalType(totalPrecision, totalFractionalPrecision)

            // Integrals are promoted to bigger type
            case (IntegralType(_, lp), IntegralType(_, rp)) =>
                if (lp > rp)
                    left
                else
                    right

            // Integral and DECIMAL are promoted to decimal
            case (lt:IntegralType, FractionalType(rt:DecimalType, _, _)) =>
                coerceNumericTypes(DecimalType(lt.precision, 0), rt)
            case (FractionalType(lt:DecimalType, _, _),  rt:IntegralType) =>
                coerceNumericTypes(lt, DecimalType(rt.precision, 0))

            // Integral and DECIMAL are promoted to decimal
            case (IntegralType(_, lp), FractionalType(_, _, _)) =>
                DoubleType
            case (FractionalType(_, _, _),  IntegralType( _, rp)) =>
                DoubleType

            // DECIMAL and fractional are promoted to DOUBLE
            case (FractionalType(_, _, _), FractionalType(_:DecimalType, _, _)) =>
                DoubleType
            case (FractionalType(_:DecimalType, _, _),  FractionalType( _, _, _)) =>
                DoubleType

            // Non-decimal fractionals are promoted to bigger type
            case (FractionalType(_, lp, _), FractionalType(_, rp, _)) =>
                if (lp > rp)
                    left
                else
                    right
        }
    }

    /**
      * Performs type coercion, i.e. find the tightest common data type that can be used to contain values of two
      * other incoming types
      * @param left
      * @param right
      * @return
      */
    @throws[TypeCoerceException]
    @throws[FieldMergeException]
    def coerce(left: FieldType, right:FieldType) : FieldType = {
        (left,right) match {
            case (l,r) if l == r => l
            case (NullType, r) => r
            case (l, NullType) => l

            case (StringType, _) => StringType
            case (_ ,StringType) => StringType

            case (VarcharType(lp), VarcharType(rp)) => VarcharType(scala.math.max(lp,rp))
            case (CharType(lp) ,VarcharType(rp)) => VarcharType(scala.math.max(lp,rp))
            case (VarcharType(lp) ,CharType(rp)) => VarcharType(scala.math.max(lp,rp))
            case (CharType(lp) ,CharType(rp)) => VarcharType(scala.math.max(lp,rp))

            case (CharType(_), _) => StringType
            case (_ ,CharType(_)) => StringType

            case (VarcharType(_), _) => StringType
            case (_ ,VarcharType(_)) => StringType

            case (l:NumericType[_], r:NumericType[_]) => coerceNumericTypes(l,r)

            case (DateType, TimestampType) => TimestampType
            case (TimestampType, DateType) => TimestampType

            case (ArrayType(ltype,lnull), ArrayType(rtype,rnull)) =>
                ArrayType(coerce(ltype,rtype), lnull || rnull)
            case (_:ArrayType, _) => throw new TypeCoerceException(left, right)
            case (_, _:ArrayType) => throw new TypeCoerceException(left, right)

            case (MapType(lkey, lval, lnull), MapType(rkey, rval, rnull)) =>
                MapType(coerce(lkey, rkey), coerce(lval, rval), lnull || rnull)
            case (_:MapType, _) => throw new TypeCoerceException(left, right)
            case (_, _:MapType) => throw new TypeCoerceException(left, right)

            case (lt:StructType, rt:StructType) =>
                union(Seq(lt, rt))
            case (_:StructType, _) => throw new TypeCoerceException(left, right)
            case (_, _:StructType) => throw new TypeCoerceException(left, right)

            case _ => StringType
        }
    }

    /**
      * Merges two fields into a common field
      * @param newField
      * @param existingField
      * @return
      */
    @throws[FieldMergeException]
    def merge(newField:Field, existingField:Field) : Field = {
        val nullable = existingField.nullable || existingField.ftype == NullType ||
            newField.ftype == NullType || newField.nullable
        val dataType = try {
            coerce(existingField.ftype, newField.ftype)
        }
        catch {
            case _:TypeCoerceException => throw new FieldMergeException(existingField, newField)
        }
        val description = existingField.description.orElse(newField.description)
        val default = existingField.default.orElse(newField.default)
        val size = existingField.size.orElse(newField.size)
        val format = existingField.format.orElse(newField.format)

        Field(existingField.name, dataType, nullable, description, default, size, format)
    }

    /**
      * Create a UNION of several schemas
      * @param schemas
      * @return
      */
    def union(schemas:Seq[StructType]) : StructType = {
        val schemaFields = schemas.map(_.fields)
        val allColumns = schemaFields
            .foldLeft(UnionSchema())((union, fields) => {
                val fieldNames = fields.map(_.name)
                fields.zipWithIndex
                    .foldLeft(union) { case (union,(field,idx)) =>
                        val prev = fieldNames.take(idx)
                        val next = fieldNames.drop(idx + 1)
                        union.withField(field.name, prev, next, field)
                    }
            })

        // Fix "nullable" property of columns not present in all input schemas
        val allColumnNames = allColumns.fieldsByName.keySet
        val nullableColumns = schemaFields.foldLeft(allColumns)((union, schema) => {
            val fields = schema.map(_.name.toLowerCase(Locale.ROOT)).toSet
            allColumnNames.foldLeft(union)((union,field) =>
                if (fields.contains(field))
                    union
                else
                    union.withNullable(field)
            )
        })

        StructType(nullableColumns.fields)
    }

    /**
      * Create a UNION schema from preciely two incoming schemas
      * @param left
      * @param right
      * @return
      */
    def union(left:StructType, right:StructType) : StructType= {
        union(Seq(left, right))
    }

    private case class UnionSchema(
        fieldsByName:Map[String,Field] = Map[String,Field](),
        fieldNames:Seq[String] = Seq()
    ) {
        def fields:Seq[Field] = fieldNames.map(name => fieldsByName(name))

        def withNullable(name:String) : UnionSchema = {
            val lowerName = name.toLowerCase(Locale.ROOT)
            val field = fieldsByName(lowerName)

            if (!field.nullable) {
                val newField = field.copy(nullable = true)
                val newFieldByName = fieldsByName.updated(lowerName, newField)
                val newFieldNames = fieldNames

                UnionSchema(
                    newFieldByName,
                    newFieldNames
                )
            }
            else {
                this
            }
        }
        def withField(name:String, prev:Seq[String], next:Seq[String], field:Field) : UnionSchema = {
            val lowerName = name.toLowerCase(Locale.ROOT)
            val newField = fieldsByName.get(lowerName).map(SchemaUtils.merge(_, field)).getOrElse(field)
            val newFieldByName = fieldsByName.updated(lowerName, newField)
            val newFieldNames =
                if (fieldsByName.contains(lowerName)) {
                    fieldNames
                }
                else {
                    val fieldIndexByName = fieldNames.zipWithIndex.toMap
                    val insertAfter = prev.flatMap(fieldIndexByName.get).reverse.headOption
                    val insertBefore = next.flatMap(fieldIndexByName.get).headOption
                    (insertAfter, insertBefore) match {
                        case (Some(idx), _) => (fieldNames.take(idx + 1) :+ lowerName) ++ fieldNames.drop(idx + 1)
                        case (_ ,Some(idx)) => (fieldNames.take(idx) :+ lowerName) ++ fieldNames.drop(idx)
                        case (None,None) => fieldNames :+ lowerName
                    }
                }

            UnionSchema(
                newFieldByName,
                newFieldNames
            )
        }
    }
}

