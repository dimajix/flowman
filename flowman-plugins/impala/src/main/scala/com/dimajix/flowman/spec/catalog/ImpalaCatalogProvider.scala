/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.catalog

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.flowman.annotation.CatalogType
import com.dimajix.flowman.catalog.ExternalCatalog
import com.dimajix.flowman.catalog.ImpalaExternalCatalog
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.ConnectionIdentifier
import com.dimajix.flowman.spec.connection.ImpalaConnection
import com.dimajix.flowman.spec.connection.JdbcConnection


@CatalogType(kind = "impala")
class ImpalaCatalogProvider extends CatalogProvider {
    @JsonProperty(value="connection", required=true) private var _connection:String = ""

    def connection(implicit context: Context) : ConnectionIdentifier = ConnectionIdentifier.parse(context.evaluate(_connection))

    override def createCatalog(session: Session): ExternalCatalog = {
        implicit val context = session.context
        val con = context.getConnection(this.connection)
        val connection = con match {
            case jdbc:JdbcConnection => new ImpalaExternalCatalog.Connection(
                jdbc.url,
                "",
                -1,
                if (jdbc.driver != null && jdbc.driver.nonEmpty) jdbc.driver else ImpalaExternalCatalog.IMPALA_DEFAULT_DRIVER,
                jdbc.username,
                jdbc.password,
                jdbc.properties
            )
            case impala:ImpalaConnection => new ImpalaExternalCatalog.Connection(
                "",
                impala.host,
                impala.port,
                if (impala.driver != null && impala.driver.nonEmpty) impala.driver else ImpalaExternalCatalog.IMPALA_DEFAULT_DRIVER,
                impala.username,
                impala.password,
                impala.properties
            )
        }

        new ImpalaExternalCatalog(connection)
    }
}
