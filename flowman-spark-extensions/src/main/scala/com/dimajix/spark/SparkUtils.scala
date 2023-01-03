/*
 * Copyright 2021 Kaya Kupferschmidt
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

package com.dimajix.spark

import org.apache.spark.SparkContext


object SparkUtils {
    val SPARK_JOB_DESCRIPTION = "spark.job.description"
    val SPARK_JOB_GROUP_ID = "spark.jobGroup.id"
    val SPARK_JOB_INTERRUPT_ON_CANCEL = "spark.job.interruptOnCancel"

    def withJobGroup[T](sc:SparkContext, jobGroupId:String, jobDescription:String)(fn: => T) : T = {
        val (prevJobGroup, prevJobDescription) = getJobGroup(sc)
        sc.setJobGroup(jobGroupId, jobDescription)
        try {
            fn
        }
        finally {
            sc.setJobGroup(prevJobGroup, prevJobDescription)
        }
    }

    def getJobGroup(sc:SparkContext) : (String,String) = {
        val jobGroupId = sc.getLocalProperty(SPARK_JOB_GROUP_ID)
        val jobDescription = sc.getLocalProperty(SPARK_JOB_DESCRIPTION)
        (jobGroupId, jobDescription)
    }
}
