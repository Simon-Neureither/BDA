package IPCountry;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CountryResponse;

import util.LineInformation;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, IntWritable> {
	public static final Log log = LogFactory.getLog(Mapper.class);

	public static final Text unknownHost = new Text("UnknownHost");
	public static final Text unknownCountry = new Text("UnknownCountry");

	public static final IntWritable one = new IntWritable(1);
	
	private DatabaseReader databaseReader = null;
	
	private Set<String> hostnames = new HashSet<String>();
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		databaseReader = new DatabaseReader.Builder(new FileInputStream("/home/hadoop/GeoLite2-Country.mmdb")).build();
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {

		synchronized (hostnames) {
			insertCountryHostnames(hostnames, context);
		}
		
		synchronized (threads) {
			for (Thread t : threads)
			{
				t.join();
			}
		}

		databaseReader.close();
	}
	
	
	private void insertCountry(String hostname, Context context) throws IOException, InterruptedException {
		// do lookup...
		CountryResponse response = null;
		try {
			response = databaseReader.country(InetAddress.getByName(hostname));
		} catch (GeoIp2Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			log.error("GEO IP ERROR:");
			log.error(e1);
		} catch (UnknownHostException e) {
			synchronized (context) {
				context.write(unknownHost, one);
			}
			return;
		}

		String country = null;

		if (response != null) {
			country = response.getCountry().getIsoCode();
		}

		synchronized (context) {
			if (country == null)
				context.write(unknownCountry, one);
			else
				context.write(new Text(country), one);
		}
	}
	
	private Set<Thread> threads = new HashSet<Thread>();
	
	private void insertCountryHostnames(Set<String> hostnames_, Context context)
	{
		Set<String> hostnames = new HashSet<String>();
		hostnames.addAll(hostnames_);
		hostnames_.clear();
		
		
		Thread t = new Thread(() -> {
			long s = System.currentTimeMillis();
			log.info("thread: start: " + hostnames.size());
			hostnames.parallelStream().forEach(
					(String hostname) -> {
						try {
							insertCountry(hostname, context);
						} catch (IOException | InterruptedException e) {
							log.error("ERROR DURING 'insertCountry' hostname: " + hostname);
							e.printStackTrace();
						}
					}
					);
			log.info("thread: end: " + (System.currentTimeMillis() - s));
		});
		
		synchronized (threads) {
			t.start();
			threads.add(t);
		}
		
		/*
		log.info("insertCountryHostnames: start: " + hostnames.size());
		hostnames.parallelStream().forEach(
				(String hostname) -> {
					try {
						insertCountry(hostname, context);
					} catch (IOException | InterruptedException e) {
						log.error("ERROR DURING 'insertCountry' hostname: " + hostname);
						e.printStackTrace();
					}
				}
				);
		log.info("insertCountryHostnames: end: time: " + (System.currentTimeMillis() - s));
		*/
	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
		
		
		while (itr.hasMoreTokens())
		{
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
			}
			else
			{
				if (info.hostname.matches("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}"))
				{
					insertCountry(info.hostname, context);
					/*
					synchronized (hostnames)
					{
						hostnames.add(info.hostname);
						
						if (hostnames.size() > 5000)
							insertCountryHostnames(hostnames, context);
					}
					*/
				}
				else
				{
					synchronized (context) {
						context.write(unknownHost, one);
					}
				}
			}
		}
				
	}
}
