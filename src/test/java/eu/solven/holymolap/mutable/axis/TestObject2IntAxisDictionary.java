package eu.solven.holymolap.mutable.axis;

import java.time.LocalDate;

import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;

public class TestObject2IntAxisDictionary {
	final Object someObject = new Object();

	@Test
	public void testRejectInvalidClasses_withAssertions() {
		boolean assertAreEnabled = false;
		assert assertAreEnabled = true;
		Assume.assumeTrue(assertAreEnabled);

		Object2IntAxisDictionary dic = new Object2IntAxisDictionary();

		// Reject invalid class
		Assertions.assertThatThrownBy(() -> dic.getIndexMayAppend(someObject)).isInstanceOf(AssertionError.class);

		// We accept any Object with a custom .hashCode
		Assertions.assertThat(dic.getIndexMayAppend(123L)).isEqualTo(0);
		Assertions.assertThat(dic.getIndexMayAppend(LocalDate.now())).isEqualTo(1);

		// Still reject invalid class after valid classes
		Assertions.assertThatThrownBy(() -> dic.getIndexMayAppend(someObject)).isInstanceOf(AssertionError.class);

		// Also reject classes extending Object (with no custom .hashCode
		Assertions.assertThatThrownBy(() -> dic.getIndexMayAppend(new TestObject2IntAxisDictionary()))
				.isInstanceOf(AssertionError.class);
	}

	@Test
	public void testRejectInvalidClasses_withoutAssertions() {
		boolean assertAreEnabled = false;
		assert assertAreEnabled = true;
		Assume.assumeFalse(assertAreEnabled);

		Object2IntAxisDictionary dic = new Object2IntAxisDictionary();

		// Would invalid class if assertions were enabled
		Assertions.assertThat(dic.getIndexMayAppend(someObject)).isEqualTo(0);
		Assertions.assertThat(dic.getIndexMayAppend(new Object())).isEqualTo(1);

		// We accept any Object with a custom .hashCode
		Assertions.assertThat(dic.getIndexMayAppend(123L)).isEqualTo(2);
		Assertions.assertThat(dic.getIndexMayAppend(LocalDate.now())).isEqualTo(3);

		// Still reject invalid class after valid classes
		Assertions.assertThat(dic.getIndexMayAppend(someObject)).isEqualTo(0);

		// Would invalid class if assertions were enabled
		Assertions.assertThat(dic.getIndexMayAppend(new TestObject2IntAxisDictionary())).isEqualTo(4);
	}
}
