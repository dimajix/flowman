/*
 * Copyright 2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.spi

import java.util.ServiceLoader

import scala.collection.JavaConverters._

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame

import com.dimajix.flowman.documentation.ColumnTest
import com.dimajix.flowman.documentation.TestResult
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution


object ColumnTestExecutor {
    def executors : Seq[ColumnTestExecutor] = {
        val loader = ServiceLoader.load(classOf[ColumnTestExecutor])
        loader.iterator().asScala.toSeq
    }
}

trait ColumnTestExecutor {
    /**
     * Executes a column test
     * @param execution - execution to use
     * @param context - context that can be used for resource lookups like relations or mappings
     * @param df - DataFrame containing the output to test
     * @param column - Path of the column to test
     * @param test - Test to execute
     * @return
     */
    def execute(execution: Execution, context:Context, df: DataFrame, column:String, test: ColumnTest): Option[TestResult]
}
