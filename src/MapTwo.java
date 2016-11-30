import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Authors: Edmond Wu, Vincent Xie
 */
public class MapTwo extends Mapper<Text, IntWritable, Text, IntWritable> {
    public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        String token = key.toString();
        if (token.contains("Total")) {
            Driver.total = value.get();
        }
        else {
            context.write(new Text(token), value);
        }
    }
}
