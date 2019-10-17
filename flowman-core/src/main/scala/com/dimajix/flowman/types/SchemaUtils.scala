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


object SchemaUtils {
    /**
      * Performs type coercion, i.e. find the tightest common data type that can be used to contain values of two
      * other incoming types
      * @param left
      * @param right
      * @return
      */
    def coerce(left: FieldType, right:FieldType) : FieldType = {
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

        def coerceNumericTypes(left:NumericType[_], right:NumericType[_]) : NumericType[_] = {
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
                    DecimalType(totalIntegerPrecision + totalFractionalPrecision, totalFractionalPrecision)

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
            case (_:ArrayType, _) => ???
            case (_, _:ArrayType) => ???

            case (MapType(lkey, lval, lnull), MapType(rkey, rval, rnull)) =>
                MapType(coerce(lkey, rkey), coerce(lval, rval), lnull || rnull)
            case (_:MapType, _) => ???
            case (_, _:MapType) => ???

            case (lt:StructType, rt:StructType) =>
                union(Seq(lt, rt))
            case (_:StructType, _) => ???
            case (_, _:StructType) => ???

            case _ => StringType
        }
    }

    /**
      * Merges two fields into a common field
      * @param newField
      * @param existingField
      * @return
      */
    def merge(newField:Field, existingField:Field) : Field = {
        val nullable = existingField.nullable || existingField.ftype == NullType ||
            newField.ftype == NullType || newField.nullable
        val dataType = coerce(existingField.ftype, newField.ftype)
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
                    union.withField(field, union.fieldsByName(field).copy(nullable = true))
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

        def withField(name:String, newField:Field) : UnionSchema = {
            val lowerName = name.toLowerCase(Locale.ROOT)
            val newFieldByName = fieldsByName.updated(lowerName, newField)
            val newFieldNames =
                if (fieldsByName.contains(lowerName)) {
                    fieldNames
                }
                else {
                    fieldNames :+ lowerName
                }

            UnionSchema(
                newFieldByName,
                newFieldNames
            )
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

