import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Authors: Edmond Wu, Vincent Xie
 */
public class Map extends Mapper<Object, Text, Text, IntWritable> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString(), "\n");
        context.write(new Text("Black"), new IntWritable(0));
        context.write(new Text("White"), new IntWritable(0));
        context.write(new Text("Draw"), new IntWritable(0));
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            if (token.length() > 3) {
                char number = token.charAt(token.length() - 3);
                if (number == '1') {
                    context.write(new Text("Black"), new IntWritable(1));
                }
                else if (number == '2') {
                    context.write(new Text("Draw"), new IntWritable(1));
                }
                else {
                    context.write(new Text("White"), new IntWritable(1));
                }
                context.write(new Text("AA"), new IntWritable(1));
            }
        }
    }
}
