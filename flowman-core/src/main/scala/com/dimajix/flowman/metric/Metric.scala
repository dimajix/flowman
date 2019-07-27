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


case class MetricFamily(
    name:String,
    metrics:Seq[Metric]
)
