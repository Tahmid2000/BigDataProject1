import java.io.IOException;
import java.util.Arrays;
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

public class Question5 {
    public static class Map extends Mapper <LongWritable, Text, Text, IntWritable>{
        private IntWritable one = new IntWritable(1);
        private Text word = new Text(); //type of output key

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] mydata = value.toString().split("\\t");
            String myWord = mydata[0];
            if(mydata.length != 2)
                return;
            mydata[1].replace("]", "");
            mydata[1].replace("[", "");
            String[] lines = mydata[1].split(",");
            word.set(myWord);
            one.set(lines.length);
            context.write(word, one);
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        //private IntWritable result = new IntWritable(0);
        private IntWritable maxCount  = new IntWritable(Integer.MIN_VALUE);
        private Text maxWord = new Text();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            for (IntWritable value: values){
                if (value.get() > maxCount.get()){
                    maxWord.set(key);
                    maxCount.set(value.get());
                }
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            context.write(maxWord, maxCount);
        }
    }
    
    //Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        //get all args
        if (otherArgs.length != 2) {
            System.out.println(Arrays.toString(otherArgs));
            System.err.println("Usage: hadoop jar question5.jar <question4 output file> <output file>");
            System.exit(2);
        }

        //create a job with name "question5"
        Job job = new Job(conf, "question5");
        job.setJarByClass(Question5.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
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