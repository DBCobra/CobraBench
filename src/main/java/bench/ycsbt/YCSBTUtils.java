package bench.ycsbt;

import bench.ZipfianGenerator;
import io.grpc.netty.shaded.io.netty.util.internal.ThreadLocalRandom;
import main.Config;

public class YCSBTUtils {
	private static final char[] UpperCaseLetters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();
	private static final char[] LowerCaseLetters = "abcdefghijklmnopqrstuvwxyz".toCharArray();
	private static final char[] Numerals = "0123456789".toCharArray();
	private static final int MaxLowerCaseLetters = LowerCaseLetters.length;
	private static final ZipfianGenerator zipf = new ZipfianGenerator(Config.get().NUM_KEYS);

	private long UInt64Rand() {
		return ThreadLocalRandom.current().nextLong();
	}

	public static int RndIntRange(int min, int max) {
		if (min == max) {
			return min;
		}
		if (max <= min) {
			return max;
		}
		int ret = ThreadLocalRandom.current().nextInt(max - min + 1) + min;
		return ret;
	}

	public static long RndInt64Range(long min, long max) {
		if (min == max) {
			return min;
		}
		if (max <= min) {
			return max;
		}
		long ret = ThreadLocalRandom.current().nextLong(max - min + 1) + min;
		return ret;
	}

	public static long NURnd(long P, long Q, int A, int s) {
		return (((RndInt64Range(P, Q) | (RndInt64Range(0, A) << s)) % (Q - P + 1)) + P);
	}

	public static String RndAlphaNumFormatted(char[] szFormat) {
		String res = "";
		for (int i = 0; i < szFormat.length; i++) {
			switch (szFormat[i]) {
			case 'a':
				res += UpperCaseLetters[RndIntRange(0, 25)];
				break;
			case 'n':
				res += Numerals[RndIntRange(0, 9)];
				break;
			default:
				res += szFormat[i];
				break;
			}
		}
		return res;
	}
	
	public static long zipfian() {
		return zipf.nextValue();
	}
}
