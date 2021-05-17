package pack1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

public class Program3 {
    /*
     Mapper Class
     This class takes the input key value pair as Long,Text
     Output Key Value pair is Text,Text
     */
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            Text word = new Text();
            IntWritable val = new IntWritable(1);
            StringTokenizer tokenizer = new StringTokenizer(value.toString(), ".");
            while (tokenizer.hasMoreTokens())
            {
                StringTokenizer tokenizer2 = new StringTokenizer(tokenizer.nextToken());
                while (tokenizer2.hasMoreTokens())
                {
                    word.set(tokenizer2.nextToken().toLowerCase());
                    context.write(word, val);
                }

            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable,Text, IntWritable>
    {

        public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException
        {
            int sum=0;
           for (IntWritable val:value)
           {
               sum=sum+val.get();
           }
            context.write(key,new IntWritable(sum));
        }

    }

    /*
    This is the driver class where different Configuration parameters have been defined.
    */
    public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf= new Configuration();
        Job job = new Job(conf,"Shares");
        job.setJarByClass(Program3.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass( Map.class);
        job.setNumReduceTasks(1);
        job.setReducerClass(Reduce.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
