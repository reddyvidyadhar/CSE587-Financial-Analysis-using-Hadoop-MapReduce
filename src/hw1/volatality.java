package hw1;
import java.util.Date;

import mapreduce.vol_phase1;
import mapreduce.vol_phase2;
import mapreduce.vol_phase3;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class volatality {

	public static void main(String[] args) throws Exception {		
		long start = new Date().getTime();		
		//Configuration conf = new Configuration();
		//String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
	     Job job1 = Job.getInstance();
	     job1.setJarByClass(vol_phase1.class);
	     Job job2 = Job.getInstance();
	     job2.setJarByClass(vol_phase2.class);
	     Job job3 = Job.getInstance();
	     job3.setJarByClass(vol_phase3.class);
		 

		System.out.println("\n**********Volatality Index-> Start**********\n");

		job1.setJarByClass(vol_phase1.class);
		job1.setMapperClass(vol_phase1.Map1.class);
		job1.setReducerClass(vol_phase1.Reduce1.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		int NOfReducer1=Integer.valueOf(args[1]);
		job1.setNumReduceTasks(NOfReducer1);
	
		job2.setJarByClass(vol_phase2.class);
		job2.setMapperClass(vol_phase2.Map2.class);
		job2.setReducerClass(vol_phase2.Reduce2.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		int NOfReducer2=Integer.valueOf(args[1]);
		job2.setNumReduceTasks(NOfReducer2);

		job3.setJarByClass(vol_phase3.class);
		job3.setMapperClass(vol_phase3.Map3.class);
		job3.setReducerClass(vol_phase3.Reduce3.class);
		job3.setMapOutputKeyClass(DoubleWritable.class);
		job3.setMapOutputValueClass(Text.class);
		
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path("Inter_"+args[1]));
		FileInputFormat.addInputPath(job2, new Path("Inter_"+args[1]));
		FileOutputFormat.setOutputPath(job2, new Path("Inter2_"+args[1]));
		
		FileInputFormat.addInputPath(job3, new Path("Inter2_"+args[1]));
		FileOutputFormat.setOutputPath(job3, new Path("output_"+args[1]));
		
		job1.waitForCompletion(true);
		job2.waitForCompletion(true);
		job3.waitForCompletion(true);
		boolean status1 = job1.waitForCompletion(true);
		boolean status2 = job2.waitForCompletion(true);
		boolean status3 = job3.waitForCompletion(true);
		if (status3 == true) {
			long end = new Date().getTime();
			System.out.println("\nJob completed in " + (end-start) + "milliseconds\n");
		}
		System.out.println("\n**********Volatality Index-> End**********\n");		
//		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}

