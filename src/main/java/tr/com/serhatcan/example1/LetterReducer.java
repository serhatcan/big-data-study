package tr.com.serhatcan.example1;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LetterReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    IntWritable result = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable value: values){
            sum += value.get();
        }
        result.set(sum);
        context.write(key, result);
    }

}
