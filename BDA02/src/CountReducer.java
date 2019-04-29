import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountReducer extends Reducer <Text, IntWritable, Text, IntWritable> {

	
		private IntWritable total= new IntWritable(0);
		
		protected void reduce (Text key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException{
			
			//Auslesen
			
			int sum=0;
			for (IntWritable value : values) {
				sum = value.get() + 1;
			}
				total.set(sum);
				context.write(key,total);
		}
		
}
