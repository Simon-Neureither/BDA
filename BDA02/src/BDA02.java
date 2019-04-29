import java.io.IOException;
import java.util.StringTokenizer;
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
				
<<<<<<< HEAD
				StringTokenizer tok =  new StringTokenizer(line, " ");	
				
			}
			/*
			
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
=======
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
>>>>>>> branch 'master' of https://github.com/Simon-Neureither/BDA
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
	

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(BDA02.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1], "out1"));
		
		
	 //  MultipleOutputs.addNamedOutput(job, "hostnameUnsorted", TextOutputFormat.class, NullWritable.class, Text.class);
		
	   boolean result = job.waitForCompletion(true);
	   
	   if (result)
	   {
		   BDA02_Sorter.main(args[1] + "/" + "out1/part-r-00000", args[1]);
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
