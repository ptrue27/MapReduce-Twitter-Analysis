import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CityCount {

	public static class TweetsMapper
			extends Mapper<Object, Text, Text, Text> {
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] tokens = value.toString().split("\\t", 2);
			String user = tokens[0];
			context.write(new Text(user), new Text("1"));
		}
	}
	
	public static class UsersMapper
			extends Mapper<Object, Text, Text, Text> {
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] tokens = value.toString().split("\\t", 2);
			String user = tokens[0];
			String city = tokens[1].split(",", 2)[0];
			context.write(new Text(user), new Text(city));
		}
	}
	
	public static class JoinUserReducer
			extends Reducer<Text,Text,Text,IntWritable> {
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			int count = 0;
			String city = "";
			
			for (Text val : values) {
				String valstr = val.toString();
				if (valstr.equals("1")) {
					count++;
				} 
				else {
					city = valstr;
				}
		    }
			
			if (city != "") {
				context.write(new Text(city), new IntWritable(count));
			}
		}
	}
	
	public static class CityCountMapper
			extends Mapper<Object, Text, Text, IntWritable> {
	
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String tokens[] = value.toString().split("\\t", 2);
			String city = tokens[0];
			int count = Integer.parseInt(tokens[1]);
			context.write(new Text(city), new IntWritable(count));
		}
	}
	
	public static class CityCountReducer
			extends Reducer<Text,IntWritable,Text,IntWritable> {
	
		private TreeMap<Integer, String> tree = new TreeMap<Integer, String>();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			
			int count = 0;
			for (IntWritable val : values) {
				count += val.get();
		    }
			
			tree.put(count, key.toString());
		    if (tree.size() > 15) {
		    	tree.remove(tree.firstKey());
		    }
		}
		
		public void cleanup(Context context)
				throws IOException, InterruptedException {
		  
			for (Map.Entry<Integer, String> entry : tree.entrySet()) {
				int count = entry.getKey();
				String city = entry.getValue();
				context.write(new Text(city), new IntWritable(count));
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		// Job 1
	    Configuration conf1 = new Configuration();
	    Job job1 = Job.getInstance(conf1, "UserJoin");
	    job1.setJarByClass(CityCount.class);
	    
	    MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, TweetsMapper.class);
	    MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, UsersMapper.class);
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(Text.class);
	    
	    job1.setReducerClass(JoinUserReducer.class);
	    FileOutputFormat.setOutputPath(job1, new Path(args[2]));
	    job1.waitForCompletion(true);
	    
	    // Job 2
	    Configuration conf2 = new Configuration();
	    Job job2 = Job.getInstance(conf2, "CityCount");
	    job2.setJarByClass(CityCount.class);
	    
	    job2.setMapperClass(CityCountMapper.class);
	    FileInputFormat.addInputPath(job2, new Path(args[2]));
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(IntWritable.class);
	    
	    job2.setReducerClass(CityCountReducer.class);
	    FileOutputFormat.setOutputPath(job2, new Path(args[3]));
	    System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}
