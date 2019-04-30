package Sorter;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {


	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		
		for (IntWritable value : values)
		{
			context.write(key, value);
		}
	}
}
