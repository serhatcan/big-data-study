package tr.com.serhatcan.temperature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by serhat on 26.03.2017.
 */
public class PlaceMainHelper {

    public static void run(String[] args,
                           String jobName,
                           Class<?> mainClass,
                           Class<? extends Mapper> mapperClass,
                           Class<? extends Reducer> reducerClass) throws Exception {

        run(args, jobName, mainClass, mapperClass, reducerClass, Text.class, DoubleWritable.class);
    }

    public static void run(String[] args,
                           String jobName,
                           Class<?> mainClass,
                           Class<? extends Mapper> mapperClass,
                           Class<? extends Reducer> reducerClass,
                           Class<?> outputKeyClass,
                           Class<?> outputValueClass) throws Exception {
        Configuration conf = new Configuration();
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));


        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        // used to delete output files in every execution
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(mainClass);
        job.setMapperClass(mapperClass);
        //job.setCombinerClass(LetterReducer.class);
        job.setReducerClass(reducerClass);
        job.setOutputKeyClass(outputKeyClass);
        job.setOutputValueClass(outputValueClass);
        job.setNumReduceTasks(2);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
