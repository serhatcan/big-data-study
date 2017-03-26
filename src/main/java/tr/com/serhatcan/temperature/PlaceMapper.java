package tr.com.serhatcan.temperature;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

/**
 * Sample line : // B253 19.58
 *
 * Created by serhat on 26.03.2017.
 */
public class PlaceMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        BufferedReader reader = new BufferedReader(new StringReader(value.toString()));
        String s = reader.readLine();
        while(s!=null){
            String[] values = s.split(" ");
            Text outputKey = new Text(values[0]);
            DoubleWritable outputValue = new DoubleWritable(Double.valueOf(values[1]));
            context.write(outputKey, outputValue);
            s = reader.readLine();
        }
    }

}
