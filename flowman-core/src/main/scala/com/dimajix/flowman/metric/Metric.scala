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

package com.dimajix.flowman.metric

/**
  * This is the simple base class for all metrics
  */
abstract class Metric {
    /**
      * Returns the name of this metric
      * @return
      */
    def name : String

    /**
      * Returns all labels associated with this metrics. A label is an arbitrary key-value pair used to
      * distinguish between different metrics. They are also used in the MetricRegistry to find specific metrics
      * @return
      */
    def labels : Map[String,String]

    /**
      * Resets this metric
      */
    def reset() : Unit
}


/**
  * A MetricBundle encapsulates multiple metrics of the same kind belonging together. While the set of MetricBundles
  * is more or less static (i.e. each MetricBundle has to be registered with the MetricRegistry), the metrics inside
  * each bundle may be dynamic (i.e. change over time)
  */
abstract class MetricBundle {
    /**
      * Returns the name of this metric bundle. Multiple individual metrics may be contained within a single bundle,
      * these represent different instances of the same metrics, which can be distinguished by their labels
      * @return
      */
    def name : String

    /**
      * Returns all labels associated with this metrics. A label is an arbitrary key-value pair used to
      * distinguish between different metrics. They are also used in the MetricRegistry to find specific metrics
      * and metric bundles
      * @return
      */
    def labels : Map[String,String]

    /**
      * Returns all metrics in this bundle. This operation may be expensive, since the set of metrics may be
      * dynamic and change over time
      * @return
      */
    def metrics : Seq[Metric]

    /**
      * Resets and/or removes all metrics in this bundle.
      */
    def reset() : Unit
}


/**
  * A MetricBoard is a collection of multiple MetricBundles to be published together to one or multiple MetricSinks
  * @param labels
  * @param bundles
  */
case class MetricBoard(
    labels:Map[String,String],
    bundles:Seq[MetricBundle]
) {
    def reset() : Unit =  bundles.foreach(_.reset())
    def metrics : Seq[Metric] = bundles.flatMap(_.metrics)
}


case class Selector(
   name:Option[String] = None,
   labels:Map[String,String] = Map()
) {
    require(name != null)
    require(labels != null)
}
