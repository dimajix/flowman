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

package com.dimajix.util

import java.lang.reflect.Constructor
import java.lang.reflect.Parameter


object Reflection {
    private val OPTION_TYPE = classOf[Option[_]]
    private val SEQ_TYPE = classOf[Seq[_]]
    private val SET_TYPE = classOf[Set[_]]
    private val MAP_TYPE = classOf[Map[_,_]]

    def companion[T](name : String)(implicit man: Manifest[T]) : Option[T] = {
        try {
            Some(Class.forName(name + "$").getField("MODULE$").get(man.runtimeClass).asInstanceOf[T])
        }
        catch {
            case _:ClassNotFoundException => None
        }
    }

    /**
     * This function will try to instantiate a specific class with the given arguments. The method is clever and
     * tries to fill in missing parameters with default values, like an empty Map for a Map parameter or None for
     * an Option. This is not possible for fundamental data types like ints, booleans etc. For missing parameters
     * of other types, the code will reccursively try to construct a default instance of the corresponding type with
     * the same rules
     * @param clazz
     * @param args
     * @tparam T
     * @return
     */
    def construct[T <: AnyRef](clazz:Class[T], args:Map[String,Any]) : T = {
        val argNames = args.keySet
        val allConstructors = clazz.getDeclaredConstructors()

        def isCandidate(con:Constructor[_]) : Boolean = {
            val paramNames = con.getParameters.map(_.getName).toSet
            argNames.forall(arg => paramNames.contains(arg))
        }
        def createParameter(param:Parameter) : AnyRef = {
            if (args.contains(param.getName)) {
                args(param.getName) match {
                    case s:Short => java.lang.Short.valueOf(s)
                    case i:Int => Integer.valueOf(i)
                    case l:Long => java.lang.Long.valueOf(l)
                    case f:Float => java.lang.Float.valueOf(f)
                    case d:Double => java.lang.Double.valueOf(d)
                    case b:Boolean => java.lang.Boolean.valueOf(b)
                    case r:AnyRef => r
                    case _ => throw new IllegalArgumentException(s"Parameter '${param.getName}' has unsupported type")
                }
            }
            else {
                param.getType() match {
                    case OPTION_TYPE => None
                    case SEQ_TYPE => Seq.empty
                    case SET_TYPE => Set.empty
                    case MAP_TYPE => Map.empty
                    case other => construct(other.asInstanceOf[Class[AnyRef]], Map.empty)
                }
            }
        }
        def tryConstruct(con:Constructor[_]) : Option[T] = {
            val params = con.getParameters
            try {
                val pvals = params.map(createParameter)
                Some(con.newInstance(pvals:_*).asInstanceOf[T])
            }
            catch {
                case _:Throwable => None
            }
        }

        val sortedConstructors = allConstructors
            .filter(isCandidate)
            .sortBy(_.getParameters.length)

        for (c <- sortedConstructors) {
            tryConstruct(c) match {
                case Some(i) => return i
                case None =>
            }
        }

        throw new IllegalArgumentException(s"Cannot construct instance of ${clazz.getName} with parameters ${args}")
    }
}
