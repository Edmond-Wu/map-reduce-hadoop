/**
 * Created by Edmond Wu on 11/29/2016.
 */

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Authors: Edmond Wu, Vincent Xie
 */
public class MapTwo extends Mapper<Text, IntWritable, Text, Text> {
    public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        double ratio = (1.0 * value.get()) / Driver.total;
        context.write(key, new Text(value.get() + " " + ratio));
    }
}
