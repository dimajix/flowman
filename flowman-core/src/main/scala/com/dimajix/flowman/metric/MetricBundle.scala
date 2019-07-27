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
  * A MetricBundle encapsulates multiple metrics of the same kind belonging together. While the set of MetricBundles
  * is more or less static (i.e. each MetricBundle has to be registered with the MetricRegistry), the metrics inside
  * each bundle may be dynamic (i.e. change over time)
  */
abstract class MetricBundle {
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
