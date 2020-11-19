package mapReduce;
import java.util.ArrayList;
import java.util.Arrays;
import java.io.PrintStream;
import java.util.HashMap;

public class UDF1 implements UDFInterface{
	
	int window = 1;
	int start = 0;
	int end = 5;
	
	public UDF1() {
		return;
	}
	
	//TimeSensors Time Series example
	
	public HashMap<String,String> map_func(Integer key, String val){
		/*
		 * Creates a hashmap where the key is the sensor and the value is the corresponding time stamp
		 */
		HashMap<String,String> a = new HashMap<>();
		String[] values = val.split("\t");
		String time_stamp = values[0];
		String[] sensors = Arrays.copyOfRange(values, 1, values.length);
		
		for(String each_sensor: sensors) {
			a.put(each_sensor, time_stamp);
		}
		return a;
		}

	public HashMap<String,String> reduce_func(String key,ArrayList<String> values){
		/*
		 * Creates a hashmap of key as sensors and values as all the time stamps where the sensor was not triggered
		 * The window size, start and end is decided by the user
		 * It looks for gaps in each sensor and stores it as an ArrayList of time stamps
		 */
		HashMap<String,String> b = new HashMap<>();
		ArrayList<Integer> time_stamp = new ArrayList<Integer>();
		ArrayList<Integer> gaps = new ArrayList<Integer>();
		for(String val: values) {
			int time_s = Integer.parseInt(val);
			time_stamp.add(time_s);
		}	
		for(int i = start;i<=end;i+=window) {
			
			gaps.add(i);
			
		}
		gaps.removeAll(time_stamp);
		String listString = "";
		for (Integer time : gaps)
		{
			String s = time.toString();
		    listString += s + ",";
		}
		b.put(key, listString);
		return b;
	}
	
}