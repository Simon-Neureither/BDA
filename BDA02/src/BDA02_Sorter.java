import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class BDA02_Sorter {
	
	public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, Text> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
			
			while (itr.hasMoreTokens())
			{
				String line = itr.nextToken();
				String[] splitted = line.split("\t");
				
				try
				{
					context.write(new IntWritable(Integer.parseInt(splitted[1])), new Text(splitted[0]));
				} catch (Exception e)
				{
					context.write(new IntWritable(10000000), new Text("ERROR: " + line));
				}
			}
		}
	}

	public static class IntSumReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		private IntWritable result = new IntWritable();
		
	    MultipleOutputs<IntWritable, Text> mos;
	    
	    @Override
	    public void setup(Context context) {
	        mos = new MultipleOutputs(context);
	    }

		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			for (Text value : values)
			{
				mos.write("sorted", key, value);
				context.write(key, value);
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			mos.close();
		}
	}

	
	public static void main(String inputPath, String outputPath) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "sort");
		job.setJarByClass(BDA02.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath, "out2"));

	    MultipleOutputs.addNamedOutput(job, "sorted", TextOutputFormat.class, IntWritable.class, Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
