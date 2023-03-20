/*
 * Copyright (C) 2023 The Flowman Authors
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

package com.dimajix.flowman.kernel.model;

import java.util.List;
import java.util.stream.Collectors;

import lombok.Value;
import lombok.val;

import com.dimajix.flowman.kernel.ConsoleUtils;


@Value
public class DataFrame {
    StructType schema;
    List<Row> rows;

    public void show() {
        val header = schema.getFields().stream().map(StructField::getName).toArray(String[]::new);
        val rows = this.rows.stream().map(Row::toStringArray).collect(Collectors.toList());
        ConsoleUtils.showTable(header, rows);
    }
}
