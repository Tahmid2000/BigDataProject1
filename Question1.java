import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question1 {
    public static class Map extends Mapper <LongWritable, Text, Text, Text>{
        private Text pair = new Text(); //type of output key
        private Text list = new Text();

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] mydata = value.toString().split("\\t"); //split user and their friends
            String user = mydata[0];
            if (mydata.length != 2)
                return;
            String[] friends = mydata[1].split(","); //split all friends
            for (String friend : friends) {
                String friendPair = Integer.parseInt(user) < Integer.parseInt(friend) ? "("+ user + ", " + friend + ")" : "("+ friend + ", " + user + ")";
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < friends.length; i++){
                    if (friends[i] != friend)
                        sb.append(friends[i] + ",");
                }
                if (sb.length() > 0 && sb.charAt(sb.length() - 1) == ',')
                    sb.deleteCharAt(sb.length() - 1);
                pair.set(friendPair);
                list.set(sb.toString());
                context.write(pair, list);
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public String mutual(String user1, String user2){
            String[] list1 = user1.split(",");
            String[] list2 = user2.split(",");
            HashSet<String> set = new HashSet<String>();
            StringBuilder sb = new StringBuilder();
            for (String s : list1)
                set.add(s);
            for (String s : list2){
                if (set.contains(s))
                    sb.append(s + ",");
            }
            if (sb.length() > 0 && sb.charAt(sb.length() - 1) == ',')
                sb.deleteCharAt(sb.length() - 1);
            return sb.toString();
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            HashSet<String> outputKeys = new HashSet<String>(Arrays.asList("(0, 1)", "(20, 28193)", "(1, 29826)", "(6222, 19272)", "(28041, 28056)"));
            if (outputKeys.contains(key.toString())){
                String[] friendLists = new String[2];
                int i = 0;
                for (Text value: values){
                    friendLists[i] = value.toString();
                    i++;
                }
                result.set("[" + mutual(friendLists[0], friendLists[1]) + "]");
                context.write(key, result);
            }
        }
    }

    //Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        //get all args
        if (otherArgs.length !=2) {
            System.out.println(Arrays.toString(otherArgs));
            System.err.println("Usage: hadoop jar question1.jar <input file> <output file>");
            System.exit(2);
        }

        //create a job with name "question1"
        Job job = new Job(conf, "question1");
        job.setJarByClass(Question1.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        //set output key type
        job.setOutputKeyClass(Text.class);
        //set output value type
        job.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        //set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}