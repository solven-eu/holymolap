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

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import eu.solven.holymolap.cube.IHolyCube;

/**
 * Factory that creates a {@link HolyCalciteSchema}.
 *
 * <p>
 * Allows a custom schema to be included in a <code><i>model</i>.json</code> file.
 */
public class HolyCalciteSchemaFactory implements SchemaFactory {
	public static final HolyCalciteSchemaFactory INSTANCE = new HolyCalciteSchemaFactory();

	protected HolyCalciteSchemaFactory() {
		// hidden
	}

	@Override
	public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
		final IHolyCube cube = (IHolyCube) operand.get("cube");
		return new HolyCalciteSchema(cube);
	}
}