package tr.com.serhatcan.temperature.minimum;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by serhat on 26.03.2017.
 */
public class PlaceMinTempReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    DoubleWritable result = new DoubleWritable();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double minValue = 0.0;
        for(DoubleWritable value : values) {
            if(value.get() < minValue) {
                minValue = value.get();
            }
        }
        result.set(minValue);
        context.write(key, result);
    }
}
