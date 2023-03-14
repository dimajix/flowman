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

import com.google.common.base.Strings;
import lombok.Value;
import lombok.val;


@Value
public class DataFrame {
    StructType schema;
    List<Row> rows;

    public void show() {
        val header = schema.getFields().stream().map(StructField::getName).toArray(String[]::new);
        val rows = this.rows.stream().map(Row::toStringArray).collect(Collectors.toList());
        val numCols = header.length;
        val columnWidth = new int[numCols];

        for (int i = 0; i < numCols; i++) {
            columnWidth[i] = header[i].length();
        }

        for (val row : rows) {
            for (int i = 0; i < numCols; i++) {
                val rl = row[i].length();
                if (columnWidth[i] < rl)
                    columnWidth[i] = rl;
            }
        }

        // Separator
        val rowSep = rowSeparator(columnWidth);
        System.out.println(rowSep);
        System.out.println(padRow(header, columnWidth));
        System.out.println(rowSep);
        for (val row : rows) {
            System.out.println(padRow(row, columnWidth));
        }
        System.out.println(rowSep);
    }

    private static String rowSeparator(int[] columnWidth) {
        val sb = new StringBuilder();
        sb.append("+");
        for (val l : columnWidth) {
            sb.append(Strings.repeat("-", l));
            sb.append("+");
        }
        return sb.toString();
    }

    private static String padRow(String[] row, int[] columnWidth) {
        val sb = new StringBuilder();
        sb.append("|");
        for (int i = 0; i < row.length; i++) {
            val col = row[i];
            val width = columnWidth[i];
            val cl = col.length();
            String r;
            if (cl > width)
                r = col.substring(0, width - 3) + "...";
            else if (cl < width)
                r = Strings.padEnd(col, width, ' ');
            else
                r = col;
            sb.append(r);
            sb.append("|");
        }
        return sb.toString();
    }
}
