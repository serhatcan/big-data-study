package tr.com.serhatcan.temperature.allinone;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by serhat on 26.03.2017.
 */
public class PlaceAllInOneReducer extends Reducer<Text, DoubleWritable, Text, Text> {
    Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        double minValue = Double.MAX_VALUE;
        double maxValue = 0.0;
        int valueCount = 0;
        double sum = 0.0;
        for(DoubleWritable value : values) {
            if(value.get() < minValue) {
                minValue = value.get();
            }
            if(value.get() > maxValue) {
                maxValue = value.get();
            }
            sum += value.get();
            valueCount++;
        }

        result.set("min:" + minValue + ", max:" + maxValue +", avg:" + sum / valueCount);
        context.write(key, result);
    }

}
