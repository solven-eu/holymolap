package eu.solven.holymolap.serialization;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.NavigableMap;

import org.assertj.core.api.Assertions;
import org.assertj.core.util.Files;
import org.junit.Ignore;
import org.junit.Test;

import com.esotericsoftware.kryo.Kryo;

import eu.solven.holymolap.ATestAggregation;
import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.measures.IHolyMeasuresDefinition;
import eu.solven.holymolap.measures.definition.HolyMeasuresTableDefinition;
import eu.solven.holymolap.measures.operator.OperatorFactory;
import eu.solven.holymolap.query.AggregateQueryBuilder;
import eu.solven.holymolap.query.AggregationToMapHelper;
import eu.solven.holymolap.query.EmptyAggregationQuery;
import eu.solven.holymolap.sink.HolyCubeSink;
import eu.solven.holymolap.sink.IHolyCubeSink;
import eu.solven.holymolap.sink.record.FastEntry;
import eu.solven.holymolap.stable.v1.pojo.MeasuredAxis;

@Ignore("TODO Repair Kryo serialziation")
public class TestKryoSerialization extends ATestAggregation {

	@Test
	public void testSerializationWithKryo() {

		MeasuredAxis measuredAxis = new MeasuredAxis(DOUBLE_FIRST_KEY, OperatorFactory.SUM);
		IHolyMeasuresDefinition definitions = HolyMeasuresTableDefinition.of(measuredAxis);

		IHolyCubeSink sink = new HolyCubeSink(definitions);
		IHolyCube initialCube = sink.sink(new FastEntry(Arrays.asList(FIRST_KEY, DOUBLE_FIRST_KEY),
				new Object[] { FIRST_VALUE, DOUBLE_FIRST_VALUE })).closeToHolyCube();

		Kryo kryo = HolyKryoHelper.kryoForHolyCube();

		Path tmpFolder = Files.newTemporaryFolder().toPath();
		HolyKryoHelper.storePut(kryo, tmpFolder, "someCube.hcube", initialCube);
		IHolyCube cube = HolyKryoHelper.storeGet(kryo, tmpFolder, "someCube.hcube");

		{
			AggregationToMapHelper
					.singleMeasureToNavigableMap(cube, new EmptyAggregationQuery().addAggregations(measuredAxis))
					.values()
					.forEach(aggregate -> {
						Assertions.assertThat(aggregate).isEqualTo(DOUBLE_FIRST_VALUE);
					});

			// There is a single fact for DoubleKey
			{
				NavigableMap<? extends NavigableMap<?, ?>, ?> result =
						AggregationToMapHelper.singleMeasureToNavigableMap(cube,
								AggregateQueryBuilder.filter(FIRST_KEY, FIRST_VALUE)
										.build()
										.addAggregations(measuredAxis));

				result.values().forEach(aggregate -> {
					Assertions.assertThat(aggregate).isEqualTo(DOUBLE_FIRST_VALUE);
				});
			}
		}
	}

}
