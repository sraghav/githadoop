package raghav.hadoop.learning.nse;

import java.io.IOException;
//import java.util.Date;
import java.util.Iterator;

//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class NSEAnalysisReducerI extends MapReduceBase
implements Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterator<Text> values,
		      OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub
	//	int count = 0;
		Text value;
		
		// key = ICICI
		// value = ICICI 10-aug-2015 | 56.09
		String[] pairs;
		
		Float 	highprice = new Float(0.0f);
		String	highDateStr="";
		
		while(values.hasNext())
		{
			value = values.next();
			System.out.println("Reducer:For "+key+"  ||data before split =>"+value+"<");
			//System.out.println("Reducer:For "+key+"  ||data before split =>"+value.toString()+"<");
			
			pairs = value.toString().split("\\|");
			System.out.println("Reducer:For "+key+"  ||data => |"+pairs[0]+"|"+pairs[1]+"|");
			
			float price = Float.parseFloat(pairs[1]);
			System.out.println("Reducer:For "+key+" price => |"+price+"|");
			
			if (price >= highprice) {
				highprice = price;
				highDateStr = pairs[0];
			}
			
			System.out.println("Reducer:For "+key+"| After Comparison=>"+highprice+"|"+"Date=>"+highDateStr);
			
		}
		output.collect(key , new Text(highprice.toString() + "|"+ highDateStr));
	}
}
