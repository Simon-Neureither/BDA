package HTTPStatusCodeDistribution;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		context.write(key, new IntWritable(sum));
	}
}
