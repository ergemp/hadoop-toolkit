package toolkit.hadoop.processors.mapreducers;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import toolkit.hadoop.utils.JsonParser;

public class JsonLabelCounter extends Configured implements Tool  {
    public static void main(String[] args) throws Exception
    {
        int res = ToolRunner.run(new Configuration(), new JsonLabelCounter(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception
    {
        /*
        java -cp dist/hadoop_toolkit.jar hadoop_toolkit.json_labelCounter -fs "hdfs://localhost:8020/" -jt "localhost:8032" "/mockdata/clickStream.json" "/outdata/labelCounter" "event" "null"
        */

        if (!(args.length == 4 || args.length == 3))
        {
            System.err.printf("Usage: %s [generic options] <input> <output> <jsonKey> <jsonFilter(optional)> \n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Configuration conf = this.getConf();
        conf.set("jsonKey", args[2]);
        if (args.length == 4)
        {
            conf.set("jsonFilter", args[3]);
            try
            {
                Pattern.compile(args[3]);
                conf.set("jsonFilter", args[3]);
            }
            catch(Exception ex)
            {
                System.out.println("----------------------------------");
                System.out.println("Regex Pattern has syntax error!..." + args[3]);
                System.out.println("Switching to everthig mode .*");
                System.out.println("----------------------------------");
                conf.set("jsonFilter", ".*");
            }
        }
        else
        {
            conf.set("jsonFilter", ".*");
        }

        System.out.println("--------------------");
        System.out.println(conf.get("fs.defaultFS"));
        System.out.println(conf.get("yarn.resourcemanager.address"));
        System.out.println(conf.get("mapreduce.framework.name"));
        System.out.println("jsonKey to count: " + conf.get("jsonKey"));
        System.out.println("filter to apply on jsonValue: " + conf.get("jsonFilter") );

        System.out.println("input: " + args[0]);
        System.out.println("output(will be deleted if exists): " + args[1]);

        System.out.println("--------------------");
        Thread.sleep(2000);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(args[1])))
        {
            fs.delete(new Path(args[1]), true);
        }

        // Create job
        Job job = Job.getInstance(getConf(), "json_labelCounter"); //wordCount_toolInterface.class.getCanonicalName()

        job.setJarByClass(JsonLabelCounter.class);


        job.setMapperClass(JsonLabelCounter.Map.class);
        job.setReducerClass(JsonLabelCounter.Reduce.class);

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
        return job.waitForCompletion(true) ? 0 : 1;

    }

    //Mapper Class
    public static class Map extends Mapper<Object, Text, Text, IntWritable>
    {
        JsonParser parser = new JsonParser();

        private final static IntWritable one = new IntWritable(1);
        private final Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException
        {
            //reset parser variables and parse the line
            parser.line = "";
            parser.value = "";
            parser.key = "";
            parser.search = false;
            parser.parse(value, context.getConfiguration().get("jsonKey"), context.getConfiguration().get("jsonFilter") );

            if (!parser.value.trim().equals(""))
            {
                if (parser.filter())
                {
                    if (parser.value.split(",").length > 0)
                    {
                        for (Integer i=0; i < parser.value.split(",").length; i++)
                        {
                            context.write(new Text(parser.value.split(",")[i]), one);
                        }
                    }
                }

                //context.write(new Text(parser.value), one);
            }
            else
            {
                //context.write(new Text("null"), one);
            }
        }
    }//Mapper Class

    //Reducer Class
    public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>
    {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException
        {
            Integer sum = 0;

            Iterator valuesIt = values.iterator();
            while(valuesIt.hasNext())
            {
                sum += Integer.parseInt(valuesIt.next().toString());
            }

            result.set(sum);
            context.write(key, result);
        }
    }//Reducer Class


}
