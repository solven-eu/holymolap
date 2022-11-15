/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package eu.solven.holymolap.calcite;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Source;
import org.checkerframework.checker.nullness.qual.Nullable;

import eu.solven.holymolap.cube.IHolyCube;

/**
 * Base class for table that reads CSV files.
 */
public class HolyCalciteTable extends AbstractTable {
	// protected final Source source;
	protected final @Nullable RelProtoDataType protoRowType;

	private @Nullable IHolyCube cube;

	public HolyCalciteTable(IHolyCube cube) {
		this.cube = cube;

		this.protoRowType = null;
	}

	@Override
	public RelDataType getRowType(RelDataTypeFactory typeFactory) {
		if (protoRowType != null) {
			return protoRowType.apply(typeFactory);
		}
		// if (rowType == null) {
		// rowType = deduceRowType((JavaTypeFactory) typeFactory, source, isStream());
		// }
		// return rowType;
		return null;
	}

	public static @Nullable RelDataType deduceRowType(JavaTypeFactory typeFactory, Source source, boolean stream) {
		final List<RelDataType> types = new ArrayList<>();
		final List<String> names = new ArrayList<>();
		if (stream) {
			// names.add(FileSchemaFactory.ROWTIME_COLUMN_NAME);
			// types.add(typeFactory.createSqlType(SqlTypeName.TIMESTAMP));
			throw new UnsupportedOperationException();
		}
		// source.

		// try (CSVReader reader = openCsv(source)) {
		// String[] strings = reader.readNext();
		// if (strings == null) {
		// strings = new String[] { "EmptyFileHasNoColumns:boolean" };
		// }
		// for (String string : strings) {
		// final String name;
		// final RelDataType fieldType;
		// final int colon = string.indexOf(':');
		// if (colon >= 0) {
		// name = string.substring(0, colon);
		// String typeString = string.substring(colon + 1);
		// Matcher decimalMatcher = DECIMAL_TYPE_PATTERN.matcher(typeString);
		// if (decimalMatcher.matches()) {
		// int precision = Integer.parseInt(decimalMatcher.group(1));
		// int scale = Integer.parseInt(decimalMatcher.group(2));
		// fieldType = parseDecimalSqlType(typeFactory, precision, scale);
		// } else {
		// switch (typeString) {
		// case "string":
		// fieldType = toNullableRelDataType(typeFactory, SqlTypeName.VARCHAR);
		// break;
		// case "boolean":
		// fieldType = toNullableRelDataType(typeFactory, SqlTypeName.BOOLEAN);
		// break;
		// case "byte":
		// fieldType = toNullableRelDataType(typeFactory, SqlTypeName.TINYINT);
		// break;
		// case "char":
		// fieldType = toNullableRelDataType(typeFactory, SqlTypeName.CHAR);
		// break;
		// case "short":
		// fieldType = toNullableRelDataType(typeFactory, SqlTypeName.SMALLINT);
		// break;
		// case "int":
		// fieldType = toNullableRelDataType(typeFactory, SqlTypeName.INTEGER);
		// break;
		// case "long":
		// fieldType = toNullableRelDataType(typeFactory, SqlTypeName.BIGINT);
		// break;
		// case "float":
		// fieldType = toNullableRelDataType(typeFactory, SqlTypeName.REAL);
		// break;
		// case "double":
		// fieldType = toNullableRelDataType(typeFactory, SqlTypeName.DOUBLE);
		// break;
		// case "date":
		// fieldType = toNullableRelDataType(typeFactory, SqlTypeName.DATE);
		// break;
		// case "timestamp":
		// fieldType = toNullableRelDataType(typeFactory, SqlTypeName.TIMESTAMP);
		// break;
		// case "time":
		// fieldType = toNullableRelDataType(typeFactory, SqlTypeName.TIME);
		// break;
		// default:
		// LOGGER.warn("Found unknown type: {} in file: {} for column: {}. Will assume the type of "
		// + "column is string.", typeString, source.path(), name);
		// fieldType = toNullableRelDataType(typeFactory, SqlTypeName.VARCHAR);
		// break;
		// }
		// }
		// } else {
		// name = string;
		// fieldType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
		// }
		// names.add(name);
		// types.add(fieldType);
		// if (fieldTypes != null) {
		// fieldTypes.add(fieldType);
		// }
		// }
		// } catch (IOException e) {
		// // ignore
		// }
		if (names.isEmpty()) {
			names.add("line");
			types.add(typeFactory.createSqlType(SqlTypeName.VARCHAR));
		}
		return typeFactory.createStructType(Pair.zip(names, types));
	}

	/** Returns the field types of this CSV table. */
	// public List<RelDataType> getFieldTypes(RelDataTypeFactory typeFactory) {
	// if (fieldTypes == null) {
	// fieldTypes = new ArrayList<>();
	// CsvEnumerator.deduceRowType((JavaTypeFactory) typeFactory, source, fieldTypes, isStream());
	// }
	// return fieldTypes;
	// }

	/** Returns whether the table represents a stream. */
	protected boolean isStream() {
		return false;
	}

}