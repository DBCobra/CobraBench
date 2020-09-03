package bench;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;

public class Report {
	private class Result {
		public final long st, end;

		public Result(long st, long end) {
			this.st = st;
			this.end = end;
		}

		public int duration() {
			return (int) (end - st);
		}
	}

	public static final double[] pctls = { 10, 25, 50, 75, 90, 95, 99, 99.9 };

	private LinkedList<Result> data = new LinkedList<>();

	public Report() {

	}

	public void addLat(long st, long end) {
		data.add(new Result(st, end));
	}

	public void dumpLats(String filename) throws IOException {
		File directory = new File(filename).getParentFile();
		if (!directory.exists()){
			directory.mkdirs();
		}
		BufferedWriter bw = new BufferedWriter(new FileWriter(filename));
		for (Result res : data) {
			bw.write(res.st + " " + res.end + "\n");
		}
		bw.close();
	}

	public double[] percentiles(int[] nums) {
		double[] ret = new double[pctls.length];
		int j = 0, n = nums.length;
		for (int i = 0; i < n && j < pctls.length; i++) {
			double current = i * 100.0 / n;
			if (current > pctls[j]) {
				ret[j] = nums[i];
				j++;
			}
		}
		return ret;
	}

	public String SprintLatencies(int[] lats) {
		StringBuilder sb = new StringBuilder();
		DecimalFormat df = new DecimalFormat();
		df.setMaximumFractionDigits(4);
		df.setMinimumFractionDigits(4);

		double[] perc = percentiles(lats);
		sb.append("Latency distribution:\n");
		for (int i = 0; i < pctls.length; i++) {
			if (perc[i] > 0) {
				sb.append("  " + pctls[i] + "% in " + df.format(perc[i]/1000000) + "ms.\n");
			}
		}

		return sb.toString();
	}

	public String histogram(int[] lats) {
		StringBuilder sb = new StringBuilder();
		DecimalFormat df = new DecimalFormat();
		df.setMaximumFractionDigits(2);
		df.setMinimumFractionDigits(2);
		df.setMinimumIntegerDigits(2);

		int bc = 10;
		double[] buckets = new double[bc + 1];
		int[] counts = new int[bc + 1];
		int refinedLength = lats.length * 999 / 1000 - 1;
		int slowest = lats[refinedLength];
		int fastest = lats[0];
		double bs = (slowest - fastest) / (double) bc;
		for (int i = 0; i < bc; i++) {
			buckets[i] = fastest + bs * i;
		}
		buckets[bc] = slowest;
		int bi = 0, max = 0;
		for (int i = 0; i < refinedLength;) {
			if (lats[i] <= buckets[bi]) {
				i++;
				counts[bi]++;
				if (max < counts[bi]) {
					max = counts[bi];
				}
			} else if (bi < buckets.length - 1) {
				bi++;
			}
		}

		sb.append("Response time histogram:\n");
		for (int i = 0; i < buckets.length; i++) {
			// Normalize bar lengths.
			int barLen = 0;
			if (max > 0) {
				barLen = counts[i] * 40 / max;
			}
			String bar = String.join("", Collections.nCopies(barLen, "∎"));
			sb.append("  " + df.format(buckets[i] / 1000000) + "[" + counts[i] + "]\t|" + bar + "\n");
		}

		return sb.toString();
	}

	public String getReport() {
		int[] lats = new int[data.size()];
		long avgTotal = 0;

		for (int i = 0; i < data.size(); i++) {
			Result res = data.get(i);
			int dur = res.duration();
			lats[i] = dur;
			avgTotal += dur;
		}

		double average = avgTotal / ((double) lats.length);

		Arrays.sort(lats);
		int fastest = lats[0];
		int slowest = lats[lats.length - 1];

		StringBuilder sb = new StringBuilder();
		DecimalFormat df = new DecimalFormat();
		df.setMaximumFractionDigits(4);
		sb.append("Summary:\n");
		sb.append("  Slowest: " + df.format(slowest / 1000000.0) + "ms \n");
		sb.append("  Fastest: " + df.format(fastest / 1000000.0) + "ms \n");
		sb.append("  Average: " + df.format(average / 1000000.0) + "ms \n");
		sb.append(SprintLatencies(lats));
		sb.append(histogram(lats));

		return sb.toString();
	}
}
