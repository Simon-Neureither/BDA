import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class OneReducer extends Reducer<Text, Text, Text, Text> {
		private IntWritable result = new IntWritable();
		
	    MultipleOutputs<IntWritable, IntWritable> mos;
	    
	    private static Map<Integer, Integer> map = new TreeMap<Integer, Integer>();

	    
	    <K, V> String generateFileName(K k, V v) {
	    	   return k.toString() + "_" + v.toString();
	    	 }
	    
	    @Override
	    public void setup(Context context) {
	        mos = new MultipleOutputs(context);
	    }

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			if ("HourOfDay".equals(key.toString()))
			{
				for (Text value : values)
				{
					int val = Integer.parseInt(value.toString());
					
					if (map.containsKey(val))
					{
						map.put(val, map.get(val) + 1);
						
						//mos.write("oneout", new Text(""+val), new Text(""+val));
					}
					else
					{
						map.put(val, 1);
					}
					
				}

				
				
			}
			else if ("ResponseLength".equals(key.toString()))
			{
				
			}
			else if ("HTTPCode".equals(key.toString()))
			{
				
			}
			
		//	mos.write("distributionHourOfDay", key, new IntWritable(0));
			
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			
			int sum = 0;
			for (Entry<Integer, Integer> e : map.entrySet())
			{
				sum = sum + e.getValue();
			}
			
			for (Entry<Integer, Integer> e : map.entrySet())
			{
				//mos.write("oneout", new Text(""+e.getKey()), new Text(""+e.getValue()));
				
				//mos.write("oneout", new Text(""+e.getKey()),  new Text(""+e.getValue()));
				
				//FileSystem fs = FileSystem.get
				
				mos.write("oneout", new Text(""+e.getKey()), new Text(""+e.getValue()));
				
				;//mos.write("distributionHourOfDay", new IntWritable(e.getKey()), new Text(e.getValue() + " " + (e.getValue() / sum * 100) + "%"));
			}
			mos.close();
		}
	}
	
	