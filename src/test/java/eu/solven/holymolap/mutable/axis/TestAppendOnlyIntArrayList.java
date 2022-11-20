package eu.solven.holymolap.mutable.axis;

import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestAppendOnlyIntArrayList {
	@Test
	public void testAppend() {
		AppendOnlyIntArrayList appendToMe = new AppendOnlyIntArrayList();

		appendToMe.append(2);
		appendToMe.append(123);
		appendToMe.append(5);
		appendToMe.append(456);
		appendToMe.append(13);

		Assertions.assertThat(appendToMe.size()).isEqualTo(5);

		{
			int[] buffer = new int[5];
			appendToMe.getElements(0, buffer, 0, 5);
			Assertions.assertThat(buffer).containsExactly(2, 123, 5, 456, 13);
		}

		{
			int[] buffer = new int[5];
			appendToMe.getElements(1, buffer, 1, 3);
			Assertions.assertThat(buffer).containsExactly(0, 123, 5, 456, 0);
		}
	}
}
