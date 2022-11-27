package eu.solven.holymolap.mutable.axis;

import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestSkippedHeaderRows {
	@Test
	public void testGetElements_offset0() {
		MutableAxisColumn decorated = new MutableAxisColumn();
		SkippedHeaderRows skipped = new SkippedHeaderRows(2, decorated);

		decorated.appendCoordinateRef(1);
		decorated.appendCoordinateRef(2);

		{
			int[] buffer = new int[4];
			skipped.getRowToIndex(0, buffer, 0, 4);
			Assertions.assertThat(buffer).containsExactly(-1, -1, 1, 2);
		}
		{
			int[] buffer = new int[2];
			skipped.getRowToIndex(0, buffer, 0, 2);
			Assertions.assertThat(buffer).containsExactly(-1, -1);
		}

		{
			int[] buffer = new int[2];
			skipped.getRowToIndex(1, buffer, 0, 2);
			Assertions.assertThat(buffer).containsExactly(-1, 1);
		}

		{
			int[] buffer = new int[2];
			skipped.getRowToIndex(2, buffer, 0, 2);
			Assertions.assertThat(buffer).containsExactly(1, 2);
		}
	}
}
