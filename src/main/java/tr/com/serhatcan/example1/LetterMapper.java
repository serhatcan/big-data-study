package tr.com.serhatcan.example1;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LetterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text letter = new Text();
    IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        for (int i = 0; i < value.getLength(); i++) {
            letter.set("" + (char) value.charAt(i));
            context.write(letter, one);
        }
    }

}
