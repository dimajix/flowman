/*
 * Copyright 2018 Kaya Kupferschmidt
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

package com.dimajix.flowman.execution

import java.io.StringWriter
import java.util.NoSuchElementException

import scala.collection.JavaConverters._

import org.apache.velocity.VelocityContext

import com.dimajix.flowman.templating.RecursiveValue
import com.dimajix.flowman.templating.Velocity


object Environment {
    private lazy val rootContext = Velocity.newContext()
}


final class Environment(rawEnvironment:Map[String,Any]) {
    private val templateEngine = Velocity.newEngine()
    private val templateContext = Velocity.newContext(Environment.rootContext)

    // Configure templating context
    rawEnvironment.foreach { case (key,value) =>
        val finalValue = value match {
            case s:String => RecursiveValue(templateEngine, templateContext, s)
            case v:Any => v
            case null => null
        }
        templateContext.put(key, finalValue)
    }

    private def evaluateNotNull(string:String, additionalValues:Map[String,AnyRef]) : String = {
        val output = new StringWriter()
        val context = if (additionalValues.nonEmpty)
            new VelocityContext(additionalValues.asJava, templateContext)
        else
            templateContext
        templateEngine.evaluate(context, output, "context", string)
        output.getBuffer.toString
    }

    /**
      * Evaluates a string containing expressions to be processed.
      *
      * @param string
      * @return
      */
    def evaluate(string:String) : String = evaluate(string, Map())

    /**
      * Evaluates a string containing expressions to be processed. This variant also accepts a key-value Map
      * with additional values to be used for evaluation
      *
      * @param string
      * @return
      */
    def evaluate(string:String, additionalValues:Map[String,AnyRef]) : String = {
        if (string != null)
            evaluateNotNull(string, additionalValues)
        else
            null
    }

    /**
      * Evaluates a string containing expressions to be processed.
      *
      * @param string
      * @return
      */
    def evaluate(string:Option[String]) : Option[String] = {
        string.map(evaluate).map(_.trim).filter(_.nonEmpty)
    }

    /**
      * Evaluates a string containing expressions to be processed. This variant also accepts a key-value Map
      * with additional values to be used for evaluation
      *
      * @param string
      * @return
      */
    def evaluate(string:Option[String], additionalValues:Map[String,AnyRef]) : Option[String] = {
        string.map(s => evaluate(s, additionalValues)).map(_.trim).filter(_.nonEmpty)
    }

    /**
      * Evaluates a key-value map containing values with expressions to be processed.
      *
      * @param map
      * @return
      */
    def evaluate(map: Map[String,String]): Map[String,String] = evaluate(map, Map())

    /**
      * Evaluates a key-value map containing values with expressions to be processed.  This variant also accepts a
      * key-value Map with additional values to be used for evaluation
      *
      * @param map
      * @return
      */
    def evaluate(map: Map[String,String], additionalValues:Map[String,AnyRef]): Map[String,String] = {
        map.map { case(name,value) => (name, evaluate(value, additionalValues)) }
    }

    /**
      * Returns the current environment used for replacing variables. References to other variables or functions
      * will be resolved and evaluated.
      *
      * @return
      */
    def toMap : Map[String,Any] = rawEnvironment.map {
        case (k, s: String) => k -> evaluate(s)
        case (k, any) => k -> any
    }

    /**
      * Returns the current environment used for replacing variables. References to other variables or functions
      * will be resolved and evaluated.
      *
      * @return
      */
    def toSeq : Seq[(String,Any)] = toMap.toSeq

    def contains(key:String) : Boolean = rawEnvironment.contains(key)

    def apply(key:String) : String = get(key) match {
        case Some(x) => x.toString
        case None => throw new NoSuchElementException(s"Environment variable '$key' not found")
    }

    def get(key:String) : Option[Any] = rawEnvironment.get(key).map  {
        case s: String => evaluate(s)
        case any => any
    }
}
