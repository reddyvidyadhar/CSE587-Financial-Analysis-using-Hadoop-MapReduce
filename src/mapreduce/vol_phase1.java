package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class vol_phase1 {

	public static class Map1 extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			double val1 = 0.00;

			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String fileName = fileSplit.getPath().getName();
			fileName = fileName.substring(0, fileName.indexOf("."));
			Text key1 = null;
			String value1 = null;
			
			String line = value.toString(); // receive one line
			String element[] = null;
			element = line.split(","); // CSV files

			if (!element[0].contains("Date")) {
				String year = element[0].split("-")[0];
				String month = element[0].split("-")[1];
				String day = element[0].split("-")[2];

				key1 = new Text(fileName+","+year+"-"+month); // Key : AAPL, 2014-12
				value1 = day+","+element[6]; 				 // Value: 01,109.95
				
				context.write(key1, new Text(value1));
			}

		}
	}

	// /////////////////////// Reducer1	// /////////////////////////////////////////////

	public static class Reduce1 extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) {

			double first = 0;
			double last =0;
			double x_i =0;
			int min=32,max=0;
			int day=0;
			
			for(Text value:values)
			{
				day = Integer.parseInt(value.toString().split(",")[0]);
				if(day<min)
				{
					last = Double.parseDouble(value.toString().split(",")[1]);
					min = day;
				}
				if(day>max)
				{
					first = Double.parseDouble(value.toString().split(",")[1]);
					max = day;
				}				
			}
			//System.out.print("FIRST"+first);
			//System.out.println("LAST"+last);
			x_i = (first-last)/last;
			//System.out.println("<"+key.toString()+"#"+x_i+">");
			try {
				context.write(key,new Text(Double.toString(x_i)));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
		}
	}
}