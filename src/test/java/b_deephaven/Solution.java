package b_deephaven;

import java.util.Arrays;
import java.util.OptionalInt;
import java.util.stream.Stream;

public class Solution {

	public String solution(String S) {
		char lineSeparator = 10;
		String[] lines = S.split(Character.toString(lineSeparator));

		assert lines.length <= 100 : "Too many rows";

		OptionalInt optResult = Stream.of(lines)
				// Reject empty rows (like trailing ones)
				.filter(l -> !l.isEmpty())
				.map(l -> {
					assert l.length() >= 5 + 1 + 10 + 1 : "Line '" + l + " is too short";

					int columnStart = 0;
					// First column has fixed size
					String rawSize = l.substring(columnStart, columnStart + 5);

					// Second column has fixed size
					columnStart += 1 + rawSize.length();
					String rawLastModified = l.substring(columnStart, columnStart + 10);

					columnStart += 1 + rawLastModified.length();
					String fileName = l.substring(columnStart);

					return Arrays.asList(rawSize, rawLastModified, fileName);
				})
				.filter(row -> {
					assert row.size() == 3 : "IllegalStateException";

					// Starts with easier rejection pattern (.endsWith)
					String fileName = row.get(2);
					boolean isBackupFile = fileName.endsWith("~");
					if (!isBackupFile) {
						return false;
					}

					// Next with easy rejection pattern (compare strings as dates)
					String rawLastModified = row.get(1);
					// TODO Was it a strict rejection?
					boolean isRecentEnough = rawLastModified.compareTo("1990-01-31") >= 0;
					if (!isRecentEnough) {
						return false;
					}

					// End with most errorProne/slow filter
					String rawSize = row.get(0);

					// Column size if 5
					// The biggest with K is 9999K which is around 9*2^20 which is strictly lower than 14*2^20
					// Hence we can consider only columns with 14M+ or XG+
					if (rawSize.endsWith("G")) {
						return false;
					} else if (rawSize.endsWith("M")) {
						int firstDigitIndex = rawSize.lastIndexOf(' ');
						int lastDigitIndex = rawSize.length() - 1;
						int nbM = Integer.parseInt(rawSize.substring(firstDigitIndex + 1, lastDigitIndex));

						// TODO Was it a strict rejection?
						if (nbM >= 14) {
							return false;
						}
					}

					// All constrains are valid: we accept the entry
					return true;
				})
				.map(row -> row.get(2))
				.mapToInt(fileName -> fileName.lastIndexOf('.'))
				.min();

		if (optResult.isPresent()) {
			return Integer.toString(optResult.getAsInt());
		} else {
			return "NO FILES";
		}
	}
}
