import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Authors: Edmond Wu, Vincent Xie
 */
public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    private int winsWhite = 0, winsBlack = 0, draws = 0;

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int result = 0;
        for (IntWritable val : values) {
            result += val.get();
        }
        context.write(key, new IntWritable(result));
    }
}