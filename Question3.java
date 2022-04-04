import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.time.Period;
import java.util.Arrays;
import java.util.HashMap;
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


public class Question3 {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private Text user = new Text(); //type of output key
        private Text list = new Text();

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String[] mydata = value.toString().split("\\t"); //split user and their friends
            String userId = mydata[0];
            if(mydata.length != 2)
                return;
            String[] friends = mydata[1].split(","); //split all friends
            for(String friend : friends) {
                user.set(userId);
                list.set(friend);
                context.write(user, list);
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        private HashMap<String,String> bdays = new HashMap<String,String>();
        private LocalDate now = LocalDate.now();

        public int calculateAge(LocalDate birthDate, LocalDate currentDate) {
            if ((birthDate != null) && (currentDate != null))
                return Period.between(birthDate, currentDate).getYears();
            return 0;
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int minAge = Integer.MAX_VALUE;
            for (Text value : values) {
                if (bdays.containsKey(value.toString())) {
                    String[] ind = bdays.get(value.toString()).split("/");
                    LocalDate bday = LocalDate.of(Integer.parseInt(ind[2]), Integer.parseInt(ind[0]), Integer.parseInt(ind[1]));
                    minAge = Math.min(minAge, calculateAge(bday, now));
                }
            }
            result.set("" + minAge);
            context.write(key, result);
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

    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 3) {
            System.out.println(Arrays.toString(otherArgs));
            System.err.println("Usage: hadoop jar question3.jar <input file> <userdata file> <output file>");
            System.exit(2);
        }

        conf.set("join", otherArgs[1]);
        // create a job with name "question3"
        Job job = new Job(conf, "question3");
        job.setJarByClass(Question3.class);
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