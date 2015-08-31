package mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class vol_phase2 {

	public static class Map2 extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) {
			String line = value.toString();

			try {
				String tempkey = value.toString().split("\t")[0].split(",")[0];
				key = new Text(tempkey);
				// System.out.println("<"+key.toString()+"#"+value.toString().split("\t")[1]+">");
				context.write(new Text(key.toString()), new Text(line)); // <AAPL, 0.11008843169103054>

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int index = 0;
			double sum = 0.00;
			double sumsq = 0.00;
			ArrayList<String> x_i = new ArrayList<String>();
			for (Text value : values) {
				// System.out.println(value.toString().split("\t")[1]);
				x_i.add(value.toString().split("\t")[1]);
				sum = sum + Double.parseDouble(value.toString().split("\t")[1]);

				index++;
			}

			double x_bar = sum / index;
			for (int i = 0; i < x_i.size(); i++) {
				sumsq = sumsq + (Double.parseDouble(x_i.get(i)) - x_bar)* (Double.parseDouble(x_i.get(i)) - x_bar);
			}
			
			if(index>1)
			{
				double val1 = sumsq / (index - 1);
				double vol = Math.sqrt(val1);
				if(vol>0.0)
				{
					context.write(key, new Text(Double.toString(vol)));
				}
			}
			//System.out.println(key.toString()+" "+vol);

		}
	}
}