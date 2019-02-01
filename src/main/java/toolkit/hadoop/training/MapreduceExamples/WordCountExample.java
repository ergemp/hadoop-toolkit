package toolkit.hadoop.training.MapreduceExamples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import toolkit.hadoop.processors.mapreducers.WordCountRemote;

public class WordCountExample {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://localhost:8020");
            conf.set("mapreduce.framework.name", "yarn");
            conf.set("yarn.resourcemanager.address", "localhost:8032");
            //conf.set("mapreduce.job.running.map.limit", "3");
            //conf.set("mapreduce.job.maps", "3");

            // Create job
            Job job = Job.getInstance(conf, "wordCount_remoteJob");
            job.setJarByClass(WordCountRemote.class);

            job.setMapperClass(WordCountRemote.Map.class);
            job.setReducerClass(WordCountRemote.Reduce.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            // Input
            FileInputFormat.addInputPath(job, new Path(args[0]));
            job.setInputFormatClass(TextInputFormat.class);

            // Output
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.setOutputFormatClass(TextOutputFormat.class);

            // Execute job and return status
            //return job.waitForCompletion(true) ? 0 : 1;
            job.waitForCompletion(true);
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
    }
}
