/*
 * Copyright 2018-2020 Kaya Kupferschmidt
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

import org.apache.hadoop.conf.{Configuration => HadoopConf}
import org.apache.spark.SparkConf

import com.dimajix.flowman.config.Configuration
import com.dimajix.flowman.config.FlowmanConf
import com.dimajix.flowman.fs.FileSystem
import com.dimajix.flowman.model.Connection
import com.dimajix.flowman.model.ConnectionIdentifier
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobIdentifier
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.Namespace
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.model.Template
import com.dimajix.flowman.model.TemplateIdentifier
import com.dimajix.flowman.model.Test
import com.dimajix.flowman.model.TestIdentifier

case class SettingLevel(
    level:Int
)
object SettingLevel {
    val SCOPE_OVERRIDE = new SettingLevel(500)
    val GLOBAL_OVERRIDE = new SettingLevel(400)
    val JOB_OVERRIDE = new SettingLevel(300)
    val PROJECT_PROFILE = new SettingLevel(250)
    val PROJECT_SETTING = new SettingLevel(200)
    val NAMESPACE_PROFILE = new SettingLevel(150)
    val NAMESPACE_SETTING = new SettingLevel(100)
    val NONE = new SettingLevel(0)
}


abstract class Context {
    /**
      * Returns the [[Namespace]] associated with this context. Can be [[None]]
      * @return
      */
    def namespace : Option[Namespace]

    /**
      * Returns the [[Project]] associated with this context. Can be [[None]]
      * @return
      */
    def project : Option[Project]

    /**
      * Returns the root context in a hierarchy of connected contexts
      * @return
      */
    def root : RootContext

    /**
      * Evaluates a string containing expressions to be processed.
      *
      * @param string
      * @return
      */
    def evaluate(string:String) : String

    /**
      * Evaluates a string containing expressions to be processed. This variant also accepts a key-value Map
      * with additional values to be used for evaluation
      *
      * @param string
      * @return
      */
    def evaluate(string:String, additionalValues:Map[String,AnyRef]) : String

    /**
      * Evaluates a string containing expressions to be processed.
      *
      * @param string
      * @return
      */
    def evaluate(string:Option[String]) : Option[String]

    /**
      * Evaluates a string containing expressions to be processed. This variant also accepts a key-value Map
      * with additional values to be used for evaluation
      *
      * @param string
      * @return
      */
    def evaluate(string:Option[String], additionalValues:Map[String,AnyRef]) : Option[String]

    /**
      * Evaluates a key-value map containing values with expressions to be processed.
      *
      * @param map
      * @return
      */
    def evaluate(map: Map[String,String]): Map[String,String]

    /**
      * Evaluates a key-value map containing values with expressions to be processed.  This variant also accepts a
      * key-value Map with additional values to be used for evaluation
      *
      * @param map
      * @return
      */
    def evaluate(map: Map[String,String], additionalValues:Map[String,AnyRef]): Map[String,String]

    /**
      * Try to retrieve the specified database [[Connection]]. Performs lookups in parent context if required
      *
      * @param identifier
      * @return
      */
    @throws[InstantiateConnectionFailedException]
    @throws[NoSuchConnectionException]
    def getConnection(identifier: ConnectionIdentifier): Connection

    /**
      * Returns a specific named [[Mapping]]. The mapping can either be inside this Contexts project or in a different
      * project within the same namespace
      *
      * @param identifier
      * @return
      */
    @throws[InstantiateMappingFailedException]
    @throws[NoSuchMappingException]
    def getMapping(identifier: MappingIdentifier, allowOverrides:Boolean=true) : Mapping

    /**
      * Returns a specific named [[Relation]]. The relation can either be inside this Contexts project or in a different
      * project within the same namespace
      *
      * @param identifier
      * @return
      */
    @throws[InstantiateRelationFailedException]
    @throws[NoSuchRelationException]
    def getRelation(identifier: RelationIdentifier, allowOverrides:Boolean=true): Relation

    /**
      * Returns a specific named [[Target]]. The target can either be inside this Contexts project or in a different
      * project within the same namespace
      *
      * @param identifier
      * @return
      */
    @throws[InstantiateTargetFailedException]
    @throws[NoSuchTargetException]
    def getTarget(identifier: TargetIdentifier): Target

    /**
      * Returns a specific named [[Job]]. The job can either be inside this Contexts project or in a different
      * project within the same namespace
      *
      * @param identifier
      * @return
      */
    @throws[InstantiateJobFailedException]
    @throws[NoSuchJobException]
    def getJob(identifier: JobIdentifier): Job

    /**
     * Returns a specific named [[Test]]. The Test can either be inside this Contexts project or in a different
     * project within the same namespace
     *
     * @param identifier
     * @return
     */
    @throws[InstantiateTestFailedException]
    @throws[NoSuchTestException]
    def getTest(identifier: TestIdentifier): Test

    /**
     * Returns a specific named [[Template]]. The Test can either be inside this Contexts project or in a different
     * project within the same namespace
     *
     * @param identifier
     * @return
     */
    @throws[InstantiateTemplateFailedException]
    @throws[NoSuchTemplateException]
    def getTemplate(identifier: TemplateIdentifier): Template[_]

    /**
     * Returns the list of active profile names
     * @return
     */
    def profiles : Set[String]

    /**
      * Returns all configuration options as a key-value map
      *
      * @return
      */
    def config : Configuration
    def rawConfig : Map[String,(String, Int)]

    /**
      * Returns the current environment used for replacing variables
      *
      * @return
      */
    def environment : Environment
    def rawEnvironment : Map[String,(Any, Int)]

    /**
      * Returns the FileSystem as configured in Hadoop
      * @return
      */
    def fs : FileSystem

    /**
     * Returns the FlowmanConf object, which contains all Flowman settings.
     * @return
     */
    def flowmanConf : FlowmanConf

    /**
      * Returns a SparkConf object, which contains all Spark settings as specified in the conifguration. The object
      * is not necessarily the one used by the Spark Session!
      * @return
      */
    def sparkConf : SparkConf

    /**
      * Returns a Hadoop Configuration object which contains all settings form the configuration. The object is not
      * necessarily the one used by the active Spark session
      * @return
      */
    def hadoopConf : HadoopConf

    /**
     * Returns a possibly shared execution environment
     * @return
     */
    def execution : Execution
}
