import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.util.*;

public class Question2 {

    public static class Map extends Mapper<LongWritable, Text, Text, Text>{
        private Text pair = new Text(); //type of output key
        private Text list = new Text();
        private HashMap<String,String> bdays = new HashMap<String,String>();

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String[] mydata = value.toString().split("\\t"); //split user and their friends
            String user = mydata[0];
            if(mydata.length != 2)
                return;
            String[] friends = mydata[1].split(","); //split all friends
            for (String friend : friends) {
                String friendPair = Integer.parseInt(user) < Integer.parseInt(friend) ? "("+ user + ", " + friend + ")" : "("+ friend + ", " + user + ")";
                StringBuilder sb = new StringBuilder();
                for(int i = 0; i < friends.length; i++){
                    if (friends[i] != friend)
                        sb.append(bdays.get(friends[i]) + ",");
                }
                if (sb.length() > 0 && sb.charAt(sb.length() - 1) == ',')
                    sb.deleteCharAt(sb.length() - 1);
                pair.set(friendPair);
                list.set(sb.toString());
                context.write(pair, list);
            }
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            Path arg = new Path(context.getConfiguration().get("join"));
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] ft = fs.listStatus(arg);
            for(FileStatus status : ft) {
                Path path = status.getPath();
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
                String line = br.readLine();
                while(line != null) {
                    String[] s = line.split(",");
                    bdays.put(s[0], s[9]);
                    line = br.readLine();
                }
            }
        }
    }


    public static class Reduce extends Reducer<Text, Text, Text, Text>{
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

        public int countAbove1995(String mutualBdays){
            String[] bdays = mutualBdays.split(",");
            int above1995 = 0;
            for(String bday : bdays) {
                String s = bday.substring(bday.indexOf(':') + 1);
                if (s.length() >= 4){
                    int year = Integer.parseInt(s.substring(s.length() - 4));
                    if (year > 1995)
                        above1995++;
                }
            }
            return above1995;
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            String[] friendLists = new String[2];
            int i = 0;
            for(Text value : values) {
                friendLists[i] = value.toString();
                i++;
            }
            String mutualBdays = mutual(friendLists[0] , friendLists[1]);
            result.set("["+mutualBdays+"], " + countAbove1995(mutualBdays));
            context.write(key, result);
        }

    }

    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 3) {
            System.out.println(Arrays.toString(otherArgs));
            System.err.println("Usage: hadoop jar question2.jar <input file> <userdata file> <output file>");
            System.exit(2);
        }

        conf.set("join", otherArgs[1]);
        // create a job with name "question2"
        Job job = new Job(conf, "question2");
        job.setJarByClass(Question2.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        // set output key type
        job.setOutputKeyClass(Text.class);
        // set output value type
        job.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
