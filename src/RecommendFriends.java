import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RecommendFriends {

    public static void main(String[] args) throws Exception {
    		/*
         * Validate that two arguments were passed from the command line.
         */
        if (args.length != 2) {
          System.out.printf("Usage: APDriver <input dir> <output dir>\n");
          System.exit(-1);
        }
        
        Configuration conf = new Configuration();

        @SuppressWarnings("deprecation")
		Job job = new Job(conf, "RecommendFriends");
        /*
         * Specify the jar file that contains your driver, mapper, and reducer.
         * Hadoop will transfer this jar file to nodes in your cluster running 
         * mapper and reducer tasks.
         */
        job.setJarByClass(RecommendFriends.class);
        
        job.setMapperClass(StubMapper.class);
        job.setReducerClass(StubReducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        /*Creating Filesystem object with the configuration*/
        FileSystem fs = FileSystem.get(conf);
        /*Check if output path (args[1])exist or not*/
        if(fs.exists(new Path(args[1]))){
    	   /*If exist delete the output path*/
    	   fs.delete(new Path(args[1]),true);
    	}

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}

