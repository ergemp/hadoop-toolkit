package org.ergemp.toolkit.hadoop.training.MapreduceExamples.Runners;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.ergemp.toolkit.hadoop.training.MapreduceExamples.Mappers.WordCountMapper;
import org.ergemp.toolkit.hadoop.training.MapreduceExamples.Reducers.WordCountReducer;

// run the jar with hadoop jar
// hadoop jar hadoop_toolkit-1.0-SNAPSHOT-jar-with-dependencies.jar org.ergemp.hadoop.training.MapreduceExamples.Runners.WordCountRunner

public class WordCountRunner {
    public static void main(String[] args) {
        try {
            Configuration cconf = new Configuration();
            cconf.set("fs.defaultFS", "hdfs://localhost:8020");
            cconf.set("mapreduce.framework.name", "yarn");
            cconf.set("yarn.resourcemanager.address", "localhost:8032");
            //conf.set("mapreduce.job.running.map.limit", "3");
            //conf.set("mapreduce.job.maps", "3");

            // Create job
            Job job = Job.getInstance(cconf, "WordCountRunner");
            job.setJarByClass(WordCountRunner.class);

            job.setMapperClass(WordCountMapper.Map.class);
            job.setReducerClass(WordCountReducer.Reduce.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            // Input
            FileInputFormat.addInputPath(job, new Path("/mockdata/airports/airports.dat"));
            job.setInputFormatClass(TextInputFormat.class);

            // Output
            FileOutputFormat.setOutputPath(job, new Path("/output/wordcount_example"));
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
