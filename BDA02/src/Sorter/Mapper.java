package Sorter;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, IntWritable, IntWritable> {

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
		
		while (itr.hasMoreTokens())
		{
			String line = itr.nextToken();
			String[] splitted = line.split("\t");

			context.write(new IntWritable(Integer.parseInt(splitted[1])), new IntWritable(Integer.parseInt(splitted[0])));
		}
	}
}