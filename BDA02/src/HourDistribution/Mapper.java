package HourDistribution;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import util.LineInformation;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, IntWritable, IntWritable> {
	private final static SimpleDateFormat format = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");
    public static final Log log = LogFactory.getLog(Mapper.class);

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
				int hourOfDay = -1;
				try {
					hourOfDay = format.parse(info.date.substring(1, info.date.length() - 1)).getHours();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					System.err.println(info.date);
					log.error("ERROR WITH DATE: " + info.date);
					e.printStackTrace();
				}

				context.write(new IntWritable(hourOfDay), new IntWritable(1));
			}

		}
	}
}
