import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import util.LineInformation;

public class OneMapper extends Mapper<Object, Text, Text, Text> {

	private final static IntWritable one = new IntWritable(1);
	private final static SimpleDateFormat format = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");
	private Text word = new Text();

	public static final Log log = LogFactory.getLog(OneMapper.class);

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
				System.out.println("ERROR IN LINE: " + line);
				log.error("ERROR IN LINE: " + line);
				context.write(new Text("ERROR"), new Text(line));
			} else {
				int hourOfDay = -1;
				try {
					hourOfDay = format.parse(info.date.substring(1, info.date.length() - 1)).getHours();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					System.err.println(info.date);
					log.error("ERROR WITH DATE: " + info.date);
					
					context.write(new Text("ERROR_DATE"), new Text(line));

					e.printStackTrace();
				}

				context.write(new Text("HourOfDay"), new Text(""+hourOfDay));
				context.write(new Text("ResponseLength"), new Text(info.responseLength));
				context.write(new Text("HTTPCode"), new Text("" +info.responseCode));
			}

		}
	}
}
