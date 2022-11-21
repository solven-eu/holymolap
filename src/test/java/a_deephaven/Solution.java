package a_deephaven;

public class Solution {

	public String solution(String S) {
		int inputLength = S.length();

		if (inputLength <= 1) {
			return S;
		}

		// Worst case: we do not purge a single character
		StringBuilder sb = new StringBuilder(inputLength);

		for (int inputIndex = 0; inputIndex < inputLength; inputIndex++) {
			char currentChar = S.charAt(inputIndex);

			int bufferLength = sb.length();

			if (sb.length() == 0) {
				sb.append(currentChar);
			} else {
				char headChar = sb.charAt(bufferLength - 1);
				if (currentChar == 'A' && headChar == 'B' || currentChar == 'B' && headChar == 'A'
						|| currentChar == 'C' && headChar == 'D'
						|| currentChar == 'D' && headChar == 'C') {
					// We discard the head and nextChar
					sb.deleteCharAt(bufferLength - 1);
				} else {
					// We accept next char
					sb.append(currentChar);
				}
			}
		}

		if (sb.length() == inputLength) {
			// Return the original String instead of duplicating it
			return S;
		}

		return sb.toString();
	}
}
