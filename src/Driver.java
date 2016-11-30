import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * Authors: Edmond Wu, Vincent Xie
 */
public class Driver {

    public static int total = 1;

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        Job job = Job.getInstance(config, "Win/Loss/Draw Counter");

        Configuration reduceConfig = new Configuration(false);
        ChainReducer.setReducer(job, Reduce.class, Object.class, Text.class, Text.class, IntWritable.class, reduceConfig);
        //Configuration secondMap = new Configuration(false);
        //ChainReducer.addMapper(job, MapTwo.class, Text.class, IntWritable.class, Text.class, IntWritable.class, secondMap);
        Configuration thirdMap = new Configuration(false);
        ChainReducer.addMapper(job, MapThree.class, Text.class, IntWritable.class, Text.class, Text.class, thirdMap);

        job.setJarByClass(Driver.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(Map.class);
        //job.setCombinerClass(Reduce.class);
        //job.setReducerClass(Reduce.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.submit();
    }
}