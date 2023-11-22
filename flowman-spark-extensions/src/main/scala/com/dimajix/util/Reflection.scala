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
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
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
      * Invokes the "copy" method from case classes with the given optional parameter map. Helps to bridge binary
      * incompatible Spark libraries, where some case classes have more fields and therefore the ABI does not match
      * any more.
      * @param src
      * @param args
      * @tparam T
      * @return
      */
    def copy[T <: AnyRef](src:T, args:Map[String,Any]) : T = {
        val clazz = src.getClass
        val argNames = args.keySet
        val allCopies = clazz.getDeclaredMethods
            .filter(_.getName == "copy")

        def isCandidate(con: Method): Boolean = {
            val paramNames = con.getParameters.map(_.getName).toSet
            argNames.forall(arg => paramNames.contains(arg))
        }
        def createParameter(param:Parameter) : AnyRef = {
            val paramName = param.getName
            if (args.contains(paramName)) {
                toAnyRef(args(paramName), param)
            }
            else {
                val getter = clazz.getDeclaredMethod(paramName)
                getter.invoke(src)
            }
        }
        def tryCopy(copy: Method): Option[T] = {
            val params = copy.getParameters
            try {
                val pvals = params.map(createParameter)
                Some(copy.invoke(src, pvals:_*).asInstanceOf[T])
            }
            catch {
                case ex:InvocationTargetException => throw ex.getTargetException
                case _: Throwable => None
            }
        }

        val sortedConstructors = allCopies
            .filter(isCandidate)
            .sortBy(_.getParameters.length)

        for (c <- sortedConstructors) {
            tryCopy(c) match {
                case Some(i) => return i
                case None =>
            }
        }

        throw new NoSuchMethodError(s"Class ${clazz.getName} does not provide a copy method")
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
    def construct[T <: AnyRef](clazz:Class[T], args:Map[String,Any])(implicit man: Manifest[T]) : T = {
        val argNames = args.keySet
        val allConstructors = clazz.getDeclaredConstructors()

        def isCandidate(con:Constructor[_]) : Boolean = {
            val paramNames = con.getParameters.map(_.getName).toSet
            argNames.forall(arg => paramNames.contains(arg))
        }
        def tryConstruct(con:Constructor[_]) : Option[T] = {
            val params = con.getParameters
            try {
                val pvals = params.map(p => createParameterOrDefault(p, args))
                Some(con.newInstance(pvals:_*).asInstanceOf[T])
            }
            catch {
                case ex:InvocationTargetException => throw ex.getTargetException
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

        try {
            apply[T](clazz.getName, args)
        }
        catch {
            case _:NoSuchMethodError =>
                throw new NoSuchMethodError(s"Cannot construct instance of ${clazz.getName} with parameters (${args.map(kv => kv._1 + ":" + kv._2.getClass.getName).mkString(",")})")
        }
    }

    def apply[T <: AnyRef](clazzName:String, args:Map[String,Any])(implicit man: Manifest[T]) : T = {
        val companion =
            try {
                Class.forName(clazzName + "$").getField("MODULE$").get(man.runtimeClass).asInstanceOf[T]
            }
            catch {
                case _:ClassNotFoundException => throw new NoSuchMethodError(s"No companion object found for class $clazzName")
            }
        invoke[T,T](companion, "apply", man.runtimeClass.asInstanceOf[Class[T]], args)
    }

    /**
      * Invokes the given method name
      * @param src
      * @param methodName
      * @param rc
      * @param args
      * @tparam T
      * @tparam R
      * @return
      */
    def invoke[T <: AnyRef,R](src:T, methodName:String, rc:Class[R], args:Map[String,Any]) : R = {
        val argNames = args.keySet
        val clazz = src.getClass

        // Only allow methods with correct name and where all parameters can be fullfilled
        def score(method: Method): Int = {
            val paramNames = method.getParameters.map(_.getName).toSet
            paramNames.intersect(argNames).size - argNames.diff(paramNames).size - paramNames.diff(argNames).size
        }

        val superClazz = if (clazz.getSuperclass != classOf[Object]) Seq(clazz.getSuperclass) else Seq.empty[Class[_]]
        val allClazzes = Seq(clazz) ++ superClazz ++ clazz.getInterfaces.toSeq
        val method = allClazzes.flatMap(_.getDeclaredMethods())
            .filter(_.getName == methodName)
            .sortBy(m => score(m))
            .lastOption
            .getOrElse(throw new NoSuchMethodError(s"No appropriate method '$methodName' found in class ${clazz.getName}"))

        val params = method.getParameters
        val pvals = params.map(p => createParameterOrDefault(p, args))
        try {
            method.invoke(src, pvals: _*).asInstanceOf[R]
        }
        catch {
            case ex:InvocationTargetException =>
                throw ex.getTargetException
        }
    }

    def invokeIfExists[T <: AnyRef](src: T, methodName: String, args: AnyRef*) : Unit = {
        val clazz = src.getClass

        val method = try {
            Some(clazz.getMethod(methodName, args.map(_.getClass):_*))
        }
        catch {
            case _:NoSuchMethodException => None
        }

        method.foreach { method =>
            try {
                method.invoke(src, args: _*)
            }
            catch {
                case ex: InvocationTargetException =>
                    throw ex.getTargetException
            }
        }
    }

    private def findMethod[T <: AnyRef](src: T, methodName: String, args: Map[String, Any]) : Option[Method] = {
        val argNames = args.keySet
        val clazz = src.getClass

        // Only allow methods with correct name and where all parameters can be fullfilled
        def score(method: Method): Int = {
            val paramNames = method.getParameters.map(_.getName).toSet
            paramNames.intersect(argNames).size - argNames.diff(paramNames).size - paramNames.diff(argNames).size
        }

        val superClazz = if (clazz.getSuperclass != classOf[Object]) Seq(clazz.getSuperclass) else Seq.empty[Class[_]]
        val allClazzes = Seq(clazz) ++ superClazz ++ clazz.getInterfaces.toSeq
        allClazzes.flatMap(_.getDeclaredMethods())
            .filter(_.getName == methodName)
            .sortBy(m => score(m))
            .lastOption
    }

    private def createParameterOrDefault(param: Parameter, args: Map[String, Any]): AnyRef = {
        if (args.contains(param.getName)) {
            toAnyRef(args(param.getName), param)
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

    private def toAnyRef(value:Any, param:Parameter) : AnyRef = {
        value match {
            case s: Short => java.lang.Short.valueOf(s)
            case i: Int => Integer.valueOf(i)
            case l: Long => java.lang.Long.valueOf(l)
            case f: Float => java.lang.Float.valueOf(f)
            case d: Double => java.lang.Double.valueOf(d)
            case b: Boolean => java.lang.Boolean.valueOf(b)
            case r: AnyRef => r
            case _ => throw new UnsupportedOperationException(s"Parameter '${param.getName}' has unsupported type")
        }
    }
}
