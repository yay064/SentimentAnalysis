import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SentimentAnalysis {

    public static class SentimentSplit extends Mapper<Object, Text, Text, IntWritable> {

        public Map<String, String> emotionDic = new HashMap<String, String>();

        @Override
        public void setup(Context context) throws IOException{

            // build emotion dictionary
            // Dynamically read input text file path by configuration

            Configuration configuration = context.getConfiguration();
            String path = configuration.get("dictionary", "");  // set key at main


            // java 读数据的模版
            BufferedReader br = new BufferedReader(new FileReader(path));
            String line = br.readLine();

            while (line != null) {
                String [] word_feeling = line.split("\t");
                emotionDic.put(word_feeling[0].toLowerCase(), word_feeling[1]);
                line = br.readLine();
            }

            br.close();

        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // read lines from file -> value
            // split into words
            // look up sentiment dict
            // write out to disk

            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            String line = value.toString().trim();

            String [] words = value.toString().split("\\s+"); // split by space

            for (String word : words) {
                if (emotionDic.containsKey(word.toLowerCase())) {
                    context.write(new Text(fileName + "\t" + emotionDic.get(word)), new IntWritable(1));
                }
            }

            // write data that reducer would collect(key value pair)
        }
    }

    public static class SentimentCollection extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            // key: sorted key after shuffle
            // value: a list of values of a same key(collect from mapper output)
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }

            context.write(key, new IntWritable(sum));
        }

    }

    public static void main(String[] args) throws Exception {
        // main -> 驱动
        Configuration configuration = new Configuration();
        configuration.set("dictionary", args[2]);  // get input path dynamically (by user input arg)

        Job job = Job.getInstance(configuration);
        job.setJarByClass(SentimentAnalysis.class);
        job.setMapperClass(SentimentSplit.class);
        job.setReducerClass(SentimentCollection.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
