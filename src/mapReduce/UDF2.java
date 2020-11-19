package mapReduce;
import java.util.ArrayList;
import java.util.Arrays;
import java.io.PrintStream;
import java.util.HashMap;

public class UDF2 implements UDFInterface{
	
	public UDF2() {
		return;
	}
	
	//Word Count Example
	
	public HashMap<String,String> map_func(Integer key, String val){
		/*
		 * Creating a hashmap that stores as key words and value as the 1, it ignores special characters and punctuation.
		 * Only considers alphabets and numerics.
		 */
		String[] sentence = val.replaceAll("[^a-zA-Z0-9\\s]", "").split(" ");
		HashMap<String,String> a = new HashMap<>();
		for(String word: sentence) {
		Integer prev_val = 1;
		word = word.toLowerCase().trim();
		if(a.containsKey(word))
		{
		prev_val = Integer.parseInt(a.get(word))+1;
		}
		a.put(word, prev_val.toString());
		}
		return a;
		}

	public HashMap<String,String> reduce_func(String key, ArrayList<String> values){
		/*
		 * Creates a hashmap that stores as key the words in a document and the corresponding frequency in that document as values.
		 */
		HashMap<String,String> b = new HashMap<>();
		Integer sum=0;
		for(String val: values) {
			int num = Integer.parseInt(val);
			sum+=num;
		}
		b.put(key,sum.toString());
		return b;
		}
}