package tr.com.serhatcan.temperature.average;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by serhat on 26.03.2017.
 */
public class PlaceAvgTempReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    DoubleWritable result = new DoubleWritable();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        int valueCount = 0;
        double sum = 0.0;
        for(DoubleWritable value: values){
            sum += value.get();
            valueCount++;
        }
        double avg = sum / valueCount;
        result.set(avg);
        context.write(key, result);
    }
}
