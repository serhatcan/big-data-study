package tr.com.serhatcan.example1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LetterCount {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // used to delete output files in every execution
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path(args[1]), true);
        // -------------------------------------------
        Job job = Job.getInstance(conf, "letter count");
        job.setJarByClass(LetterCount.class);
        job.setMapperClass(LetterMapper.class);
        //job.setCombinerClass(LetterReducer.class);
        job.setReducerClass(LetterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(2);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
