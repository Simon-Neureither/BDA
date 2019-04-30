import java.io.*;
import java.util.*;
import java.io.BufferedReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegExParser {
	public static void FileParser (String record) {
		//Regulären Ausdruck erzeugen
		
		final String regex="(^\\S+\\.[\\S+\\.]+\\S+)\\s"+"\\[(\\d{2}/\\w{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2} -\\d{4})]"
		+"\\\"(\\S+)\\s(\\S+)\\s*(\\S*)\\\""+"\\s(\\d{3})\\s"+"\\s(\\d+)$";
		final Pattern pattern=Pattern.compile(regex, Pattern.MULTILINE);
		final Matcher matcher=pattern.matcher(record);
		
		//Erzeugen einer HashMap
		
	
	}
	
	public static void main (String[]args)
	{
		int count;
		try {
			BufferedReader br=new BufferedReader(new FileReader("C:\\Users\\Baron89\\Desktop\\BigData Analyse - Praktikum\\Praktikum 2\\access_log_Aug95"));
		    while((count=br.read())!=-1){
		    	System.out.println((char) count);	
		    }
		    	br.close();}
		catch (IOException e) {
			System.out.println("Fehler beim Lesen der Datei");
		}
	}
}
	//hosts
	/*host_pattern = r'(^\S+\.[\S+\.]+\S+)\s'
			hosts = [re.search(host_pattern, item).group(1)
			           if re.search(host_pattern, item)
			           else 'no match'
			           for item in sample_logs]
			hosts*/
		
	//Zeit
			/*ts_pattern = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
			timestamps = [re.search(ts_pattern, item).group(1) for item in sample_logs]
			timestamps*/
			
	//Anfrage
			
	/*method_uri_protocol_pattern = r'\"(\S+)\s(\S+)\s*(\S*)\"'
	method_uri_protocol = [re.search(method_uri_protocol_pattern, item).groups()
			if re.search(method_uri_protocol_pattern, item)
			               else 'no match'
			              for item in sample_logs]
			    method_uri_protocol*/
			    
	//Status
	
			    /*status_pattern = r'\s(\d{3})\s'
			    status = [re.search(status_pattern, item).group(1) for item in sample_logs]
			    print(status)*/
			    
	//Speichergröße
			    
			    /*content_size_pattern = r'\s(\d+)$'
			    content_size = [re.search(content_size_pattern, item).group(1) for item in sample_logs]
			    print(content_size)*/
			    
			    
   // Zusammen
			    
			    /*logs_df = base_df.select(regexp_extract('value', host_pattern, 1).alias('host'),
                        regexp_extract('value', ts_pattern, 1).alias('timestamp'),
                        regexp_extract('value', method_uri_protocol_pattern, 1).alias('method'),
                        regexp_extract('value', method_uri_protocol_pattern, 2).alias('endpoint'),
                        regexp_extract('value', method_uri_protocol_pattern, 3).alias('protocol'),
                        regexp_extract('value', status_pattern, 1).cast('integer').alias('status'),
                        regexp_extract('value', content_size_pattern, 1).cast('integer').alias('content_size'))
			    		logs_df.show(10, truncate=True)
			    		print((logs_df.count(), len(logs_df.columns)))*/
			    
			   
