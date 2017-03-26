package tr.com.serhatcan.temperature.maximum;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Collections;

/**
 * Created by serhat on 26.03.2017.
 */
public class PlaceMaxTempReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    DoubleWritable result = new DoubleWritable();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double maxValue = 0.0;
        for(DoubleWritable value : values) {
            if(value.get() > maxValue) {
                maxValue = value.get();
            }
        }
        result.set(maxValue);
        context.write(key, result);
    }
}
