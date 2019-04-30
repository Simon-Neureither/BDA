package HTTPStatusCodeDistribution;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import util.LineInformation;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, IntWritable, IntWritable> {
    public static final Log log = LogFactory.getLog(Mapper.class);

    public static final IntWritable one = new IntWritable(1);
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		StringTokenizer itr = new StringTokenizer(value.toString(), "\n");

		while (itr.hasMoreTokens()) {
			// Line
			String line = itr.nextToken();
			
			
			LineInformation info = null;
			try {
				info = LineInformation.parseLine(line);
			} catch (Exception e) {
				System.err.println(e);
			}
			if (info == null) {
				log.error("ERROR IN LINE: " + line);
			} else {
				

				context.write(new IntWritable(info.responseCode), one);
			}

		}
	}
}
