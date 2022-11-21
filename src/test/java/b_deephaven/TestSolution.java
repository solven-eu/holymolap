package b_deephaven;

import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestSolution {
	@Test
	public void test() {
		Assertions.assertThat(new Solution().solution("  15G 2009-09-23 system.zip~")).isEqualTo("NO FILES");
		Assertions.assertThat(new Solution().solution("   15 2009-09-23 system.zip~")).isEqualTo("6");
		Assertions.assertThat(new Solution().solution("  15M 2009-09-23 system.zip~")).isEqualTo("NO FILES");
		Assertions.assertThat(new Solution().solution("9999K 2009-09-23 system.zip~")).isEqualTo("6");
		Assertions.assertThat(new Solution().solution(" 715K 1990-01-30 system.zip~")).isEqualTo("NO FILES");
		
		Assertions.assertThat(new Solution().solution(" 715K 2009-09-23 .~")).isEqualTo("0");
		Assertions.assertThat(new Solution().solution(" 715K 2009-09-23 .zip~")).isEqualTo("0");
		Assertions.assertThat(new Solution().solution(" 715K 2009-09-23 .zip")).isEqualTo("NO FILES");
		Assertions.assertThat(new Solution().solution(" 715K 2009-09-23 ")).isEqualTo("NO FILES");
		Assertions.assertThat(new Solution().solution(" 715K 2009-09-23 system2 .zip.zip~")).isEqualTo("12");
		Assertions.assertThat(new Solution().solution("")).isEqualTo("NO FILES");

		Assertions.assertThat(new Solution().solution(" 715K 2009-09-23 system.zip~")).isEqualTo("6");
		Assertions.assertThat(new Solution().solution(" 715K 2009-09-23 system2.zip~")).isEqualTo("7");
		Assertions.assertThat(new Solution().solution(" 715K 2009-09-23 system2.zip.zip~")).isEqualTo("11");

		Assertions.assertThat(
				new Solution().solution(" 715K 2009-09-23 system.zip~\n" + " 179K 2013-08-14 to-do-list.xml~\n"
						+ " 645K 2013-06-19 blockbuster.mpeg~\n"
						+ "  536 2010-12-12 notes.html\n"
						+ " 688M 1990-02-11 delete-this.zip~\n"
						+ "  23K 1987-05-24 setup.png~\n"
						+ " 616M 1965-06-06 important.html\n"
						+ "  14M 1992-05-31 crucial-module.java~\n"
						+ " 192K 1990-01-31 very-long-filename.dll~"))
				.isEqualTo("6");
	}

	@Test
	public void test2() {
		Assertions.assertThatThrownBy(() -> new Solution().solution(" 715K 2009-09-23"))
				.isInstanceOf(AssertionError.class);
	}
}
