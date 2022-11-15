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

import java.util.Map;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.google.common.collect.ImmutableMap;

import eu.solven.holymolap.cube.IHolyCube;

/**
 * Schema mapped onto a directory of CSV files. Each table in the schema is a CSV file in that directory.
 */
public class HolyCalciteSchema extends AbstractSchema {
	private final IHolyCube cube;

	/**
	 * Creates a CSV schema.
	 *
	 * @param cube
	 *            Directory that holds {@code .csv} files
	 */
	public HolyCalciteSchema(IHolyCube cube) {
		this.cube = cube;
	}

	@Override
	protected Map<String, Table> getTableMap() {
		final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

		builder.put("holy", new HolyCalciteTable(cube));

		return builder.build();
	}

}