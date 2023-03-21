import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HashtagCount {

	public static class HashtagCountMapper
    		extends Mapper<Object, Text, Text, IntWritable> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String patternStr = "(?<=\\s|^)#(\\w*[A-Za-z_]+\\w*)";
			Pattern pattern = Pattern.compile(patternStr);
			Matcher matcher = pattern.matcher(value.toString());
      
	    	while (matcher.find()) {
	    		String hashtag = matcher.group();
	    		context.write(new Text(hashtag), new IntWritable(1));
	    	}
		}
	}

	public static class HashtagCountReducer
       		extends Reducer<Text,IntWritable,Text,IntWritable> {
		
		private TreeMap<Integer, String> tree = new TreeMap<Integer, String>();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			
			int count = 0;
			for (IntWritable val : values) {
				count += val.get();
	        }
			
			tree.put(count, key.toString());
		    if (tree.size() > 25) {
		    	tree.remove(tree.firstKey());
		    }
		}
		
		public void cleanup(Context context)
				throws IOException, InterruptedException {
		  
			for (Map.Entry<Integer, String> entry : tree.entrySet()) {
				int count = entry.getKey();
				String hashtag = entry.getValue();
				context.write(new Text(hashtag), new IntWritable(count));
			}
		}
	}
  
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "HashtagCount");
	    job.setJarByClass(HashtagCount.class);
	    
	    job.setMapperClass(HashtagCountMapper.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    job.setReducerClass(HashtagCountReducer.class);
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}