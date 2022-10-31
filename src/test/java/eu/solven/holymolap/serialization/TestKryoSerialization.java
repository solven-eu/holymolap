package eu.solven.holymolap.serialization;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.NavigableMap;

import org.assertj.core.api.Assertions;
import org.assertj.core.util.Files;
import org.junit.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import de.javakaffee.kryoserializers.guava.ImmutableListSerializer;
import eu.solven.holymolap.ATestAggregation;
import eu.solven.holymolap.cache.CompressedIntArray;
import eu.solven.holymolap.cube.HolyCube;
import eu.solven.holymolap.cube.IHolyCube;
import eu.solven.holymolap.cube.cellset.HolyBitmapCellMultiSet;
import eu.solven.holymolap.immutable.axes.AxisWithCoordinates;
import eu.solven.holymolap.immutable.axis.ImmutableAxisSmallColumn;
import eu.solven.holymolap.immutable.column.ImmutableDoubleAggregatesColumn;
import eu.solven.holymolap.immutable.column.ImmutableLongAggregatesColumn;
import eu.solven.holymolap.immutable.dictionary.AxisCoordinatesDictionary;
import eu.solven.holymolap.immutable.table.HolyDictionarizedTable;
import eu.solven.holymolap.measures.HolyMeasureColumnMeta;
import eu.solven.holymolap.measures.HolyMeasuresTable;
import eu.solven.holymolap.measures.IHolyMeasuresDefinition;
import eu.solven.holymolap.measures.aggregation.DoubleAggregationLogic;
import eu.solven.holymolap.measures.aggregation.LongAggregationLogic;
import eu.solven.holymolap.measures.definition.HolyMeasuresTableDefinition;
import eu.solven.holymolap.measures.operator.OperatorFactory;
import eu.solven.holymolap.measures.operator.SumDoubleBinaryOperator;
import eu.solven.holymolap.query.AggregateHelper;
import eu.solven.holymolap.query.AggregateQueryBuilder;
import eu.solven.holymolap.query.EmptyAggregationQuery;
import eu.solven.holymolap.sink.HolyCubeSink;
import eu.solven.holymolap.sink.IHolyCubeSink;
import eu.solven.holymolap.sink.record.FastEntry;
import eu.solven.holymolap.stable.v1.pojo.MeasuredAxis;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;

public class TestKryoSerialization extends ATestAggregation {

	private static void storePut(Kryo kryo, Path cacheFolder, String pathAsString, IHolyCube value) {
		File fileToPersist = new File(cacheFolder.toFile(), Paths.get(pathAsString).getFileName().toString());
		try (Output output = new Output(new FileOutputStream(fileToPersist))) {
			kryo.writeObject(output, value);
		} catch (FileNotFoundException e) {
			throw new UncheckedIOException(e);
		}
	}

	private static IHolyCube storeGet(Kryo kryo, Path cacheFolder, String pathAsString) {
		File fileToPersist = new File(cacheFolder.toFile(), Paths.get(pathAsString).getFileName().toString());
		try (Input input = new Input(new FileInputStream(fileToPersist))) {
			return kryo.readObject(input, HolyCube.class);
		} catch (KryoException e) {
			throw new RuntimeException("Issue processing: " + pathAsString, e);
		} catch (FileNotFoundException e) {
			throw new UncheckedIOException(e);
		}
	}

	@Test
	public void testSerializationWithKryo() {

		MeasuredAxis measuredAxis = new MeasuredAxis(DOUBLE_FIRSY_KEY, OperatorFactory.SUM);
		IHolyMeasuresDefinition definitions = HolyMeasuresTableDefinition.of(measuredAxis);

		IHolyCubeSink sink = new HolyCubeSink(definitions);
		IHolyCube initialCube = sink.sink(new FastEntry(Arrays.asList(FIRST_KEY, DOUBLE_FIRSY_KEY),
				new Object[] { FIRST_VALUE, DOUBLE_FIRST_VALUE })).closeToHolyCube();

		Kryo kryo = new Kryo();

		// https://github.com/magro/kryo-serializers/issues/76
		// https://github.com/EsotericSoftware/kryo/issues/860
		kryo.addDefaultSerializer(Object2LongMap.class, new JavaSerializer());

		kryo.register(HolyCube.class);
		kryo.register(HolyDictionarizedTable.class);
		kryo.register(ImmutableAxisSmallColumn.class);
		kryo.register(CompressedIntArray.class);
		kryo.register(int[].class);
		// kryo.register(IntCompressor.class);
		// kryo.register(AtomicLong.class);
		kryo.register(HolyMeasuresTable.class);
		kryo.register(HolyMeasuresTableDefinition.class);
		kryo.register(HolyMeasureColumnMeta.class);
		kryo.register(DoubleAggregationLogic.class);
		kryo.register(LongAggregationLogic.class);
		kryo.register(ArrayList.class);
		kryo.register(ImmutableDoubleAggregatesColumn.class);
		kryo.register(ImmutableLongAggregatesColumn.class);
		kryo.register(DoubleArrayList.class);
		kryo.register(LongArrayList.class);
		kryo.register(MeasuredAxis.class);
		kryo.register(SumDoubleBinaryOperator.class);
		kryo.register(HolyBitmapCellMultiSet.class);
		kryo.register(AxisWithCoordinates.class);
		kryo.register(AxisCoordinatesDictionary.class);
		// https://github.com/magro/kryo-serializers
		ImmutableListSerializer.registerSerializers(kryo);

		kryo.register(Object2LongOpenHashMap.class);

		Path tmpFolder = Files.newTemporaryFolder().toPath();
		storePut(kryo, tmpFolder, "someCube.hcube", initialCube);
		IHolyCube cube = storeGet(kryo, tmpFolder, "someCube.hcube");

		{
			AggregateHelper.singleMeasureToNavigableMap(cube, new EmptyAggregationQuery().addAggregations(measuredAxis))
					.values()
					.forEach(aggregate -> {
						Assertions.assertThat(aggregate).isEqualTo(DOUBLE_FIRST_VALUE);
					});

			// There is a single fact for DoubleKey
			{
				NavigableMap<? extends NavigableMap<?, ?>, ?> result = AggregateHelper.singleMeasureToNavigableMap(cube,
						AggregateQueryBuilder.filter(FIRST_KEY, FIRST_VALUE).build().addAggregations(measuredAxis));

				result.values().forEach(aggregate -> {
					Assertions.assertThat(aggregate).isEqualTo(DOUBLE_FIRST_VALUE);
				});
			}
		}
	}
}
