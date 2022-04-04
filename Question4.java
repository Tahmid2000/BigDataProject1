import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

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
import org.apache.hadoop.util.GenericOptionsParser;

public class Question4 {
    public static class Map extends Mapper <LongWritable, Text, Text, IntWritable>{
        private IntWritable one = new IntWritable();
        private Text word = new Text(); //type of output key
        int count = 1;

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] mydata = value.toString().split(",");
            for (String data:mydata) {
                String[] spaces = data.split(" ");
                for (String space : spaces){
                    if (space.trim().length() > 0){
                        word.set(space); //set word as each input keyword
                        one.set(count);
                        context.write(word, one); // create a pair <keyword, 1>
                    }
                }
            }
            count++;
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
        private Text result = new Text();

        public String sorted(ArrayList<Integer> lines){
            Collections.sort(lines);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < lines.size(); i++){
                sb.append(lines.get(i));
                sb.append(",");
            }
            if (sb.length() > 0 && sb.charAt(sb.length() - 1) == ',')
                sb.deleteCharAt(sb.length() - 1);
            return sb.toString();
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            StringBuilder sb = new StringBuilder();
            ArrayList<Integer> lines = new ArrayList<>();
            for (IntWritable value : values)
                lines.add(value.get());
            result.set("[" + sorted(lines) + "]");
            context.write(key, result);
        }
    }

    //Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        //get all args
        if (otherArgs.length != 2) {
            System.out.println(Arrays.toString(otherArgs));
            System.err.println("Usage: hadoop jar question4.jar <userdata file> <output file>");
            System.exit(2);
        }

        //create a job with name "question4"
        Job job = new Job(conf, "question4");
        job.setJarByClass(Question4.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        //set output key type
        job.setOutputKeyClass(Text.class);
        //set output value type
        job.setOutputValueClass(IntWritable.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        //set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}