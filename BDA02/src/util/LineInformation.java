package util;

public class LineInformation {
	public String hostname = "";
	public String date = "";
	public String query = "";
	public int responseCode = 0;
	public String responseLength = "";

	public static LineInformation parseLine(String line) {
		String hostname = null;
		String date = "";
		String query = "";
		int responseCode = 0;
		String responseLength = "";
		
		int idx;
		
		idx = line.indexOf(" ");
		
		hostname = 	line.substring(0, idx);
		line = line.substring(idx);
		
		line = line.substring(" - - ".length());
		
		idx = line.indexOf("]");
		date = line.substring(0, idx + 1);
		line = line.substring(idx + 2);
		
		idx = line.lastIndexOf("\"");
		
		query = line.substring(0, idx + 1);
		
		line = line.substring(idx + 2);
		
		idx = line.indexOf(" ");
		responseCode = Integer.parseInt(line.substring(0, idx));
		line = line.substring(idx + 1);
		responseLength = line;
		
		LineInformation info = new LineInformation();

		
		info.hostname = hostname;
		info.date = date;
		info.query = query;
		info.responseCode = responseCode;
		info.responseLength = responseLength;

		return info;
	}

	public String toString() {
		String string = "";

		string += hostname + "\r";
		string += date + "\r";
		string += query + "\r";
		string += responseCode + "\r";
		string += responseLength + "\r";

		return string;
	}

}
