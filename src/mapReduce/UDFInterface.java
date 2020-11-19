package mapReduce;

import java.util.*;

public interface UDFInterface{
	//declaring the map and reduce User defined functions
	public HashMap<String,String> map_func(Integer key, String val);
	public HashMap<String,String> reduce_func(String key,ArrayList<String> values);
	
}