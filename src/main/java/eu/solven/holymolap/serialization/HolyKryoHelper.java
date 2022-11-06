package eu.solven.holymolap.serialization;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import de.javakaffee.kryoserializers.guava.ImmutableListSerializer;
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
import eu.solven.holymolap.measures.aggregation.DoubleAggregationLogic;
import eu.solven.holymolap.measures.aggregation.LongAggregationLogic;
import eu.solven.holymolap.measures.definition.HolyMeasuresTableDefinition;
import eu.solven.holymolap.measures.operator.CountBinaryOperator;
import eu.solven.holymolap.measures.operator.SumDoubleBinaryOperator;
import eu.solven.holymolap.stable.v1.pojo.MeasuredAxis;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;

public class HolyKryoHelper {
	private static final Logger LOGGER = LoggerFactory.getLogger(HolyKryoHelper.class);

	protected HolyKryoHelper() {
		// hidden
	}

	public static Kryo kryoForHolyCube() {
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
		kryo.register(HolyBitmapCellMultiSet.class);
		kryo.register(AxisWithCoordinates.class);
		kryo.register(AxisCoordinatesDictionary.class);

		kryo.register(CountBinaryOperator.class);
		kryo.register(SumDoubleBinaryOperator.class);

		// https://github.com/magro/kryo-serializers
		ImmutableListSerializer.registerSerializers(kryo);

		kryo.register(Object2LongOpenHashMap.class);
		return kryo;
	}

	public static IHolyCube storeGet(Kryo kryo, Path cacheFolder, String pathAsString) {
		File fileToPersist =
				new File(cacheFolder.toFile(), Paths.get(pathAsString).getFileName().toString() + ".holymolap");

		if (fileToPersist.length() == 0) {
			LOGGER.warn("We delete {} as it is length=0", fileToPersist);
			boolean deleted = fileToPersist.delete();
			LOGGER.warn("Result for {} deletion: {}", fileToPersist, deleted);
		}

		try (Input input = new Input(new FileInputStream(fileToPersist))) {
			return kryo.readObject(input, HolyCube.class);
		} catch (KryoException e) {
			throw new RuntimeException("Issue processing: " + pathAsString, e);
		} catch (FileNotFoundException e) {
			throw new UncheckedIOException(e);
		}
	}

	public static void storePut(Kryo kryo, Path cacheFolder, String pathAsString, IHolyCube value) {
		File fileToPersist =
				new File(cacheFolder.toFile(), Paths.get(pathAsString).getFileName().toString() + ".holymolap");
		fileToPersist.getParentFile().mkdirs();

		if (fileToPersist.length() == 0) {
			LOGGER.warn("We delete {} as it is length=0", fileToPersist);
			boolean deleted = fileToPersist.delete();
			LOGGER.warn("Result for {} deletion: {}", fileToPersist, deleted);
		}

		try (Output output = new Output(new FileOutputStream(fileToPersist))) {
			kryo.writeObject(output, value);
		} catch (FileNotFoundException e) {
			throw new UncheckedIOException(e);
		}
	}
}
