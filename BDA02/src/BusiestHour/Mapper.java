package BusiestHour;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, IntWritable, IntWritable> {
	public static final Log log = LogFactory.getLog(Mapper.class);

	public static int hour = -1;
	public static int max = -1;

	private static class Op {
		private Op() {

		}

		private static Op instance = new Op();

		private static Op getInstance() {
			return instance;
		}

		public synchronized void exec(int h, int n) {
			if (n > max) {
				hour = h;
				max = n;
			}
		}
	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		StringTokenizer itr = new StringTokenizer(value.toString(), "\n");

		while (itr.hasMoreTokens()) {
			// Line
			String line = itr.nextToken();

			String[] splitted = line.split("\t");

			int hour = Integer.parseInt(splitted[0]);
			int number = Integer.parseInt(splitted[1]);

			Op.getInstance().exec(hour, number);

		}
	}

	@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
		context.write(new IntWritable(hour), new IntWritable(max));
	}
}
