/*
 * Copyright (C) 2018 The Flowman Authors
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

import org.apache.spark.sql.types.DataType


case object FloatType extends FractionalType[Float] {
    override protected def fractionalNum = scala.math.Numeric.FloatIsFractional
    override protected def integralNum = new scala.math.Integral[Float] {
        def plus(x: Float, y: Float): Float = x + y
        def minus(x: Float, y: Float): Float = x - y
        def times(x: Float, y: Float): Float = x * y
        def div(x: Float, y: Float): Float = x / y
        def mod(x: Float, y: Float): Float = x % y
        def quot(x: Float, y: Float): Float = (x / y).toLong.toFloat
        def rem(x: Float, y: Float): Float = (x % y).toLong.toFloat
        def negate(x: Float): Float = -x
        def fromInt(x: Int): Float = x.toFloat
        def toLong(x: Float): Long = x.toLong
        def toInt(x: Float): Int = x.toInt
        def toFloat(x: Float): Float = x
        def toDouble(x: Float): Double = x.toDouble
        def compare(x: Float, y: Float): Int = java.lang.Float.compare(x, y)
        override def zero: Float = 0.0f
        override def one: Float = 1.0f
        def parseString(str: String): Option[Float] = try Some(str.toFloat) catch { case _:NumberFormatException => None }
    }

    protected def parseRaw(value:String) : Float = value.toFloat

    override def sparkType : DataType = org.apache.spark.sql.types.FloatType
}
