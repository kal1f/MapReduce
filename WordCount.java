import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;

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

import javax.print.attribute.HashAttributeSet;

public class WordCount {
    public static void main(String [] args) throws Exception
    {
        Configuration configuration =new Configuration();
        Path inputFolder = new Path("/Users/ashcherbak/Downloads/test.txt"); //"/Users/ashcherbak/Downloads/.txt"
        Path outputFolder = new Path("/Users/ashcherbak/Downloads/out"); //"/Users/ashcherbak/Downloads/out"

        Job job =new Job(configuration,"wordcount");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, inputFolder);
        FileOutputFormat.setOutputPath(job, outputFolder);
        System.exit(job.waitForCompletion(true)?0:1);
    }

    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{


        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
        {
            String outKey;
            String line = value.toString().toUpperCase();
            line = line.replaceAll("[^A-Z\\s]+", "");
            line = line.trim();
            String[] words=line.split(" ");

            Hashtable<String, Integer> out = new Hashtable<String, Integer>();

            for(String word: words )
            {
                if(!out.contains(word)){
                    out.put(word, 1);
                }
                else {
                    Integer count = out.get(word);
                    out.put(word, count + 1);
                }

            }

            Set<String> keys = out.keySet();

            for (String s : keys) {
                outKey = s;

                Text outputKey = new Text(outKey);
                IntWritable outputValue = new IntWritable(out.get(outKey));
                con.write(outputKey, outputValue);
            }
        }
    }

    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException
        {
            int sum = 0;
            for(IntWritable value : values)
            {
                sum += value.get();
            }
            con.write(word, new IntWritable(sum));
        }
    }
}