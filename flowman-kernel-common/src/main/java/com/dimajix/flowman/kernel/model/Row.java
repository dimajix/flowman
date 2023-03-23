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

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.val;


final public class Row {
    public static Row ofProto(com.dimajix.flowman.kernel.proto.Row row) {
        val length = row.getFieldCount();
        val fields = new Object[length];
        val inputFields = row.getFieldList();
        int i = 0;
        for (val inputField : inputFields) {
            fields[i] = ofProto(inputField);
            i++;
        }

        return new Row(fields);
    }
    public static Object ofProto(com.dimajix.flowman.kernel.proto.Field field) {
        if (field.hasDouble()) {
            return Double.valueOf(field.getDouble());
        }
        else if (field.hasLong()) {
            return Long.valueOf(field.getLong());
        }
        else if (field.hasBool()) {
            return Boolean.valueOf(field.getBool());
        }
        else if (field.hasString()) {
            return field.getString();
        }
        else if (field.hasBytes()) {
            return field.getBytes();
        }
        else if (field.hasTimestamp()) {
            val ts = field.getTimestamp();
            return new java.sql.Timestamp(ts.getSeconds() * 1000L * 1000L + ts.getNanos());
        }
        else if (field.hasDate()) {
            val dt = field.getDate();
            return new java.sql.Date(dt.getDays());
        }
        else if (field.hasMap()) {
            // TODO
            throw new UnsupportedOperationException();
        }
        else if (field.hasArray()) {
            val in = field.getArray();
            return in.getValuesList().stream().map(Row::ofProto).collect(Collectors.toList());
        }
        else if (field.hasRow()) {
            return ofProto(field.getRow());
        }
        else if (field.hasNull()) {
            return null;
        }
        else {
            throw new IllegalArgumentException();
        }
    }

    private final Object[] values;

    public Row(Object[] values) {
        this.values = Arrays.copyOf(values, values.length);
    }
    public int length() {
        return values.length;
    }

    public Object get(int i) {
        return values[i];
    }

    public boolean isNullAt(int i) {
        return values[i] == null;
    }

    public double getDouble(int i) {
        return ((Double)values[i]).doubleValue();
    }
    public long getLong(int i) {
        return ((Long)values[i]).longValue();
    }
    public boolean getBoolean(int i) {
        return ((Boolean)values[i]).booleanValue();
    }
    public String getString(int i) {
        return (String)values[i];
    }
    public byte[] getBytes(int i) {
        return (byte[])values[i];
    }
    public Timestamp getTimestamp(int i) {
        return (Timestamp)values[i];
    }
    public Date getDate(int i) {
        return (Date)values[i];
    }
    public Row getRow(int i) {
        return (Row)values[i];
    }

    public <K,V> Map<K,V> getMap(int i) {
        return (Map<K, V>) values[i];
    }
    public <T> List<T> getList(int i) {
        return (List<T>)values[i];
    }

    @Override
    public String toString() {
        return "Row{" +
            "values=" + Arrays.toString(values) +
            '}';
    }
    public String[] toStringArray() {
        return Arrays.stream(values).map(e ->
            e == null ? "null"
                : e instanceof byte[] ? toString((byte[])e)
                : e.toString()
            ).toArray(String[]::new);
    }
    private static String toString(byte[] byteArray) {
        val sb = new StringBuilder();
        for (val b : byteArray)
            sb.append(String.format("%02X", b));
        return sb.toString();
    }
}
