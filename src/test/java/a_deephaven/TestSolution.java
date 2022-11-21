package a_deephaven;

import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestSolution {
	@Test
	public void test() {
		Assertions.assertThat(new Solution().solution("CBACD")).isEqualTo("C");
		Assertions.assertThat(new Solution().solution("CABABD")).isEqualTo("");
		Assertions.assertThat(new Solution().solution("ACBDACBD")).isEqualTo("ACBDACBD");

		Assertions.assertThat(new Solution().solution("ACDB")).isEqualTo("");
	}
}
