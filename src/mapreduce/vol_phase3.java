package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class vol_phase3 {

	public static class Map3 extends Mapper<Object, Text, DoubleWritable, Text> {
		public void map(Object key, Text value, Context context) {

			String tempkey = value.toString().split("\t")[0].split(",")[0];
			key = new Text(tempkey);
			String stock = key.toString();
			// System.out.println(key.toString()+"#"+value.toString().split("\t")[1]);
			// // key: AAPL Value: 0.07600408284562592
			double vol_index = Double
					.parseDouble(value.toString().split("\t")[1]);

			try {
				// System.out.println(stock + " " + Double.toString(vol_index));
				context.write(new DoubleWritable(vol_index), new Text(stock));
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public static class Reduce3 extends
			Reducer<DoubleWritable, Text, Text, Text> {

		private LinkedList<Double> indexList = new LinkedList<Double>();
		private ArrayList<String> stockList = new ArrayList<String>();

		private Text key1 = new Text();
		private Text value1 = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			HashMap<String, Double> pair = new HashMap<String, Double>();
			String top = null;
			int count = 0;
			for (Text value : values) {

				indexList.add(Double.parseDouble(key.toString()));
				stockList.add(value.toString());
				pair.put(value.toString(), Double.parseDouble(key.toString()));
			}

			Collections.sort(indexList);

//			Iterator it = pair.entrySet().iterator();
//			for (int i = indexList.size() - 1; i >= indexList.size() - 11; i--) {
//				//System.out.println("Top 10 stocks with minimum volatility :");
//				pair.get(indexList.get(i));
//				context.write(new Text(indexList.get(i).toString()), new Text(
//						pair.get(indexList.get(i)).toString()));
//
//			}
			for (int i = 0; i<10; i++) {
				System.out.println("Top 10 stocks with max volatility :");
				pair.get(indexList.get(i));
				if(!pair.get(indexList.get(i)).isNaN())
				{
					context.write(new Text(indexList.get(i).toString()), new Text(pair.get(indexList.get(i)).toString()));
				}
			}
//			
//			List<Double> list1 = new ArrayList<Double>(pair.values());
//			List<String> list2 = new ArrayList<String>(pair.keySet());
//			
//			for(int i=0;i<10;i++){
//				System.out.println(pair.get(i));
//				context.write(new Text("null"),new Text(pair.get(i).toString()));
//			}
			
			/*
			 * while (it.hasNext()) { int index =0; Map.Entry p =
			 * (Map.Entry)it.next();
			 * 
			 * if(!indexList.getFirst().isNaN()&& indexList.getFirst()!=0) {
			 * context.write(new Text(p.getKey().toString()), new
			 * Text(p.getValue().toString())); indexList.removeFirst(); }
			 * it.remove(); }
			 */
		}
	}

}
