import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class BDA02 {
	public static class LineInformation
	{
		public String hostname = "";
		public String date = "";
		public String query = "";
		public int responseCode = 0;
		public String responseLength = "";
		
		public static LineInformation parseLine(String line)
		{
			String hostname = null;
			String date = "";
			String query = "";
			int responseCode = 0;
			String responseLength = "";
						
			String[] splitted = line.split(" ");
			
			int section = 0;
			
			for (int i = 0; i < splitted.length; i++)
			{
				switch (section)
				{
				case 0:
					hostname = splitted[i];
					section++;
					break;
				case 1:
					if (splitted[i].contains("["))
					{
						date += splitted[i];
					}
					else if (splitted[i].contains("]"))
					{
						date += " " + splitted[i];
						section++;
					}
					else
					{
						// Ignore.
					}
					break;
				case 2:
					if (splitted[i].startsWith("\""))
					{
						query = splitted[i].substring(1);
					}
					else if (splitted[i].startsWith("HTTP") && splitted[i].endsWith("\""))
					{
						//query += splitted[i].substring(0, splitted[i].length() - 1);
						section++;
					}
					else
					{
						query += " " + splitted[i];
					}
					break;
				case 3:
					responseCode = Integer.parseInt(splitted[i]);
					section++;
					break;
				case 4:
					responseLength = splitted[i];
					section++;
					break;
				default:
					System.err.println("Error while parsing, unknown section!");
					break;
				}
			}
			
			LineInformation info = new LineInformation();
			
			info.hostname = hostname;
			info.date = date;
			info.query = query;
			info.responseCode = responseCode;
			info.responseLength = responseLength;
			
			return info;
		}
		
		public String toString()
		{
			String string = "";
			
			string += hostname + "\r";
			string += date + "\r";
			string += query + "\r";
			string += responseCode + "\r";
			string += responseLength + "\r";
			
			return string;
		}
		
	}


	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
			
			while (itr.hasMoreTokens())
			{
				// Line
				String line = itr.nextToken();
				
				LineInformation info = null;
				try {
					info = LineInformation.parseLine(line);
				} catch (Exception e)
				{
					System.err.println(e);
				}
				if (info == null)
					word.set("ERROR IN LINE: " + line);
				else
				{
					word.set(info.hostname);
				}
				context.write(word, one);
				
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		
	    MultipleOutputs<Text, Text> mos;
	    
	    @Override
	    public void setup(Context context) {
	        mos = new MultipleOutputs(context);
	    }

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
			
			//mos.write("hostnameUnsorted", result, key);
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			mos.close();
		}
	}
	
	
	public static class DistributionHourMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private final static SimpleDateFormat format = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");
		private Text word = new Text();
		
	    public static final Log log = LogFactory.getLog(DistributionHourMapper.class);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
			
			while (itr.hasMoreTokens())
			{
				// Line
				String line = itr.nextToken();
				
				LineInformation info = null;
				try {
					info = LineInformation.parseLine(line);
				} catch (Exception e)
				{
					System.err.println(e);
				}
				if (info == null)
				{
					System.out.println("ERROR IN LINE: " + line);
					log.error("ERROR IN LINE: " + line);
					context.write(new IntWritable(-1), new IntWritable(1));
				}
				else
				{
					int hourOfDay = - 1;
					try {
						hourOfDay = format.parse(info.date.substring(1, info.date.length() - 1)).getHours();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						System.err.println(info.date);
						log.error("ERROR WITH DATE: " + info.date);

						e.printStackTrace();
					}
					
					context.write(new IntWritable(hourOfDay), one);
				}
				
			}
		}
	}


	public static class DistributionHourReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		private IntWritable result = new IntWritable();
		
	    MultipleOutputs<IntWritable, IntWritable> mos;
	    
	    
	    @Override
	    public void setup(Context context) {
	        mos = new MultipleOutputs(context);
	    }

		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
			
			mos.write("distributionHourOfDay", key, new IntWritable(sum));
			
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			mos.close();
		}
	}
	
	
	
	
	static boolean JobHourDistribution(String input, String output) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "HourDistribution");
		job.setJarByClass(BDA02.class);
		job.setMapperClass(HourDistribution.Mapper.class);
		job.setCombinerClass(HourDistribution.Reducer.class);
		job.setReducerClass(HourDistribution.Reducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job.waitForCompletion(true);
	}
	
	static boolean busiestHour(String input, String output) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "BusiestHour");
		job.setJarByClass(BDA02.class);
		job.setMapperClass(BusiestHour.Mapper.class);
		
		// No reduce job.
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job.waitForCompletion(true);
	}
	
	static boolean ResponseLengthDistribution(String input, String output) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ResponseLengthDistribution");
		job.setJarByClass(BDA02.class);
		job.setMapperClass(ResponseLengthDistribution.Mapper.class);
		job.setReducerClass(ResponseLengthDistribution.Reducer.class);
		job.setCombinerClass(ResponseLengthDistribution.Reducer.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job.waitForCompletion(true);
	}
	
	static boolean Sort(String input, String output) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Sorter");
		job.setJarByClass(BDA02.class);
		job.setMapperClass(Sorter.Mapper.class);
		job.setReducerClass(Sorter.Reducer.class);
		job.setCombinerClass(Sorter.Reducer.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job.waitForCompletion(true);
	}
	
	static boolean HTTPStatusCodeDistribution(String input, String output) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "HTTPStatusCodeDistribution");
		job.setJarByClass(BDA02.class);
		job.setMapperClass(HTTPStatusCodeDistribution.Mapper.class);
		job.setReducerClass(HTTPStatusCodeDistribution.Reducer.class);
		job.setCombinerClass(HTTPStatusCodeDistribution.Reducer.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job.waitForCompletion(true);
	}
	
	static boolean IPCountry(String input, String output) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "IPCountry");
		job.setJarByClass(BDA02.class);
		job.setMapperClass(IPCountry.Mapper.class);
		job.setReducerClass(IPCountry.Reducer.class);
		job.setCombinerClass(IPCountry.Reducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job.waitForCompletion(true);
	}
	
	static boolean One(String input, String output) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "One");
		job.setJarByClass(BDA02.class);
		job.setMapperClass(OneMapper.class);
		job.setReducerClass(OneReducer.class);
		job.setCombinerClass(OneReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		MultipleOutputs.addNamedOutput(job, "oneout", TextOutputFormat.class, Text.class, Text.class);

		
		return job.waitForCompletion(true);
	}

	public static void main(String[] args) throws Exception {
		Configuration conf;/* = new Configuration();
		Job job = Job.getInstance(conf, "hostname");
		job.setJarByClass(BDA02.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1], "out1"));
		
		
		 //  MultipleOutputs.addNamedOutput(job, "hostnameUnsorted", TextOutputFormat.class, NullWritable.class, Text.class);
		//MultipleOutputs.addNamedOutput(job, "distributionHourOfDay", TextOutputFormat.class, IntWritable.class, IntWritable.class);
		
	   boolean result = job.waitForCompletion(true);
	   
	   if (result)
	   {
		   result = BDA02_Sorter.main(args[1] + "/" + "out1/part-r-00000", args[1] + "/hostnameAccessCount");
	   }
	   */
		boolean result = true;
		Job job;
		if (result && false) {
		
			conf = new Configuration();
			job = Job.getInstance(conf, "distributionHourOfDay");
			job.setJarByClass(BDA02.class);
			job.setMapperClass(DistributionHourMapper.class);
			job.setCombinerClass(DistributionHourReducer.class);
			job.setReducerClass(DistributionHourReducer.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1], "hourOfDay"));
		
			MultipleOutputs.addNamedOutput(job, "distributionHourOfDay", TextOutputFormat.class, IntWritable.class,
					Text.class);
		
			result = job.waitForCompletion(true);
		}
	   
		if (result)
		{
			//result = One(args[0], args[1] + "/one") && false;
			result = result && JobHourDistribution(args[0], args[1] + "/hourOfDay");
			result = result && busiestHour(args[1] + "/hourOfDay" + "/part-r-00000", args[1] + "/busiestHour");
			result = result && ResponseLengthDistribution(args[0], args[1] + "/responseLengthDistribution");
			result = result && Sort(args[1] + "/responseLengthDistribution" + "/part-r-00000", args[1] + "/responseLengthDistributionSorted");
			result = result && HTTPStatusCodeDistribution(args[0], args[1] + "/HTTPStatusCodeDistribution");
			result = result && IPCountry(args[0], args[1] + "/IPCountry");
		}
		
	   
		System.exit(result ? 0 : 1);
		
		
		
		/*
		job = Job.getInstance(conf, "sort");
		job.setJarByClass(BDA02.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		*/
	}
}
