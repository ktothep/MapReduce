package pack1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class Program4 {
    public static class Map extends Mapper<LongWritable, Text,Text,Custom>
    {
        public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
             Text date=new Text();
             String val[]=value.toString().toLowerCase().split(",");
             Custom custom =new Custom();
             custom.setExchange(val[0]);
             custom.setSymbol(val[1]);
             custom.setStockAdjClose(Double.parseDouble(val[8]));
             custom.setStockClose(Double.parseDouble(val[6]));
             custom.setStockVolume(Double.parseDouble(val[7]));
             custom.setStockOpen(Double.parseDouble(val[3]));
             custom.setStockLow(Double.parseDouble(val[5]));
             custom.setStockHigh(Double.parseDouble(val[4]));
             date.set(val[2]);
             context.write(date,custom);

        }
    }

    public static class Reduce extends Reducer<Text, Custom,Text, Custom>
    {

        public void reduce(Text key, Iterable<Custom> value, Context context) throws IOException, InterruptedException
        {
            for (Custom val:value)
            {
              if(val.getStockHigh()>=5)
              {
                  context.write(key,val);
              }
            }

        }

    }

    public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf =new Configuration() ;
        Job job =new Job(conf,"Stock Analysis");
        job.setJarByClass(Program4.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Custom.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Custom.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
