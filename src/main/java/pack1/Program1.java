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
import java.util.List;

public class Program1 {
   public static class Map extends Mapper<LongWritable, Text,Text,Text>
    {

       public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
           String[] str = value.toString().split(",");
           String price=str[4];
           context.write(new Text(str[1]),new Text(price));

       }

    }

    public static class Reduce extends Reducer<Text, Text,Text, DoubleWritable>
    {

        public void reduce(Text key,Iterable<Text> value,Context context) throws IOException, InterruptedException {
            double count=0;
            double sum=0;

          DoubleWritable lng=new DoubleWritable();
            for(Text price:value)
            {
                count++;
                lng.set(Double.parseDouble(price.toString()));
                sum=sum +lng.get() ;
            }



          context.write(new Text("Count"),new DoubleWritable(count));
            context.write(new Text("Sum"),new DoubleWritable(sum));

            context.write(new Text("Average"),new DoubleWritable(sum/count));




        }

    }
    public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf= new Configuration();
     Job job = new Job(conf,"Shares");
     job.setJarByClass(Program1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
     job.setOutputKeyClass(Text.class);
     job.setOutputValueClass(DoubleWritable.class);
     job.setMapperClass( Map.class);
     job.setNumReduceTasks(1);
     job.setReducerClass(Reduce.class);
     FileInputFormat.addInputPath(job, new Path(args[1]));
     FileOutputFormat.setOutputPath(job, new Path(args[2]));
     System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
