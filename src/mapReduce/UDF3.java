package mapReduce;
import java.util.ArrayList;
import java.util.Arrays;
import java.io.PrintStream;
import java.util.HashMap;

public class UDF3 implements UDFInterface{
	
	public UDF3() {
		return;
	}
	//Inverted Index example
	
	public HashMap<String,String> map_func(Integer key, String val){
		/*
		 * Creating a hashmap of word and the corresponding document it occurs in.
		 */
		String[] content = val.split("\t");
		String source = content[0];
		String words = (String) content[1];
		HashMap<String,String> a = new HashMap<>();
		for(String target: words.split(",")) {
		target = target.toLowerCase().trim();
		if(!a.containsKey(target))
		{
			a.put(target, source);
		}
		}
		return a;
		}

	public HashMap<String,String> reduce_func(String key,ArrayList<String> values){
		/*
		 * Creating a hashmap of words and all the corresponding documents it occurs in.
		 */
		HashMap<String,String> b = new HashMap<>();
		b.put(key,values.toString());
		return b;
		}
}