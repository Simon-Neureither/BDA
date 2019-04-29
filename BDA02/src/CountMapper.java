import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountMapper extends Mapper <Object, Text, Text, IntWritable> {
	
		private final static IntWritable one= new IntWritable(1);
		
		
		
		//Zeile eines Weblogfiles
		
	protected void map (Object key, Text value, Context context)
			throws IOException, InterruptedException{
		Configuration config= context.getConfiguration();
		
		Pattern logEntryPattern=Pattern.compile(config.get("logEntryRegEx"));
		String [] fieldsToCount=config.get("fieldsToCount").split("");
		String [] entries=value.toString().split("\r?\n");
 	

	
	// Schlüsselpaar für jeden Eintrag generieren
	
	for (int i=0;i<entries.length;i++) {
		Matcher logEntryMatcher= logEntryPattern.matcher(entries[i]);
		if (logEntryMatcher.find()) {
			for (String index : fieldsToCount) {
				if (!index.equals("")) {
					Text t= new Text(index + " "+logEntryMatcher.group(Integer.parseInt(index)));
					context.write(t, one);
					}
				}
			}
		}	
	}
}
