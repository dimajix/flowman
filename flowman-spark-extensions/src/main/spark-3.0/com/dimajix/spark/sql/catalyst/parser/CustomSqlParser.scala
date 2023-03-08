/*
 * Copyright (C) 2022 The Flowman Authors
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

package com.dimajix.spark.sql.catalyst.parser

import org.apache.spark.sql.catalyst.parser.AbstractSqlParser
import org.apache.spark.sql.catalyst.parser.AstBuilder
import org.apache.spark.sql.catalyst.parser.ParserUtils.withOrigin
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.SingleDataTypeContext
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType


class CustomAstBuilder(conf: SQLConf) extends AstBuilder(conf) {
    override def visitSingleDataType(ctx: SingleDataTypeContext): DataType = withOrigin(ctx) {
        typedVisit[DataType](ctx.dataType)
    }
}

object CustomSqlParser extends AbstractSqlParser(SQLConf.get) {
    val astBuilder = new CustomAstBuilder(SQLConf.get)
}
