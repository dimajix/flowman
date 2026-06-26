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


case object DoubleType extends FractionalType[Double] {
    override protected def fractionalNum = scala.math.Numeric.DoubleIsFractional
    override protected def integralNum = new scala.math.Integral[Double] {
        def plus(x: Double, y: Double): Double = x + y
        def minus(x: Double, y: Double): Double = x - y
        def times(x: Double, y: Double): Double = x * y
        def div(x: Double, y: Double): Double = x / y
        def mod(x: Double, y: Double): Double = x % y
        def quot(x: Double, y: Double): Double = (x / y).toLong.toDouble
        def rem(x: Double, y: Double): Double = (x % y).toLong.toDouble
        def negate(x: Double): Double = -x
        def fromInt(x: Int): Double = x.toDouble
        def toLong(x: Double): Long = x.toLong
        def toInt(x: Double): Int = x.toInt
        def toFloat(x: Double): Float = x.toFloat
        def toDouble(x: Double): Double = x
        def compare(x: Double, y: Double): Int = java.lang.Double.compare(x, y)
        override def zero: Double = 0.0
        override def one: Double = 1.0
        def parseString(str: String): Option[Double] = try Some(str.toDouble) catch { case _:NumberFormatException => None }
    }

    override protected def parseRaw(value:String) : Double = value.toDouble

    override def sparkType : DataType = org.apache.spark.sql.types.DoubleType
}
