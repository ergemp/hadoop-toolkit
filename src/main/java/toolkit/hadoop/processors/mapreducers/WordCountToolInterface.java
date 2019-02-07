package toolkit.hadoop.processors.mapreducers;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

public class WordCountToolInterface extends Configured implements Tool {
//hadoop jar hadoop_toolkit.jar hadoop_toolkit.wordCount_toolInterface -D fs.defaultFS=hdfs://localhost  /input/path /output/path
    //-D fs.defaultFS=hdfs://localhost:8020 /customers.csv /out.txt
    //-D fs.defaultFS=hdfs://localhost:8020 -D yarn.resourcemanager.address=localhost:8032 /customers.csv /out.txt
    //-fs hdfs://127.0.0.1:8020 -jt 127.0.0.1:8032 /customers.csv /out6.txt
    //
    //-fs hdfs://localhost
    //-jt localhost:8032
    //-libjars  -> specify comma separated jar files to include in the classpath.
    //-conf     -> specify an application configuration file
    //-files    -> specify comma separated files to be copied to the map reduce cluster
    //-archives -> specify comma separated archives to be unarchived on the compute machines.

    //java -cp hadoop_toolkit.jar hadoop_toolkit.wordCount_toolInterface -fs hdfs://localhost:8020 -jt localhost:8032 -D mapreduce.framework.name=yarn /customers.csv /out6.txt

    /*
    conf.set("my.dummy.configuration","foobar");
    becomes
    -D my.dummy.configuration=foobar
    */
    public static void main(String[] args) throws Exception
    {
        int res = ToolRunner.run(new Configuration(), new WordCountToolInterface(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception
    {
        Configuration conf = this.getConf();

        System.out.println(conf.get("fs.defaultFS"));
        System.out.println(conf.get("yarn.resourcemanager.address"));
        System.out.println(conf.get("mapreduce.framework.name"));
        Thread.sleep(2000);

        // Create job
        Job job = Job.getInstance(getConf(), "wordCount_toolInterface"); //wordCount_toolInterface.class.getCanonicalName()

        job.setJarByClass(WordCountToolInterface.class);

        job.setMapperClass(WordCountToolInterface.Map.class);
        job.setReducerClass(WordCountToolInterface.Reduce.class);

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

        private final static IntWritable one = new IntWritable(1);
        private final Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException
        {
            /*
            String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();
            for (String pattern : patternsToSkip)
            {
              line = line.replaceAll(pattern, "");
            }
            */

            String line = value.toString();
            String[] lineArr = line.split("\\s+");

            for (int i=0; i<lineArr.length; i++)
            {
                context.write(new Text(lineArr[i]), one);
            }

            //
            //do whatever you want to do here
            //

            /*
            StringTokenizer itr = new StringTokenizer(line,"\\s");

            while (itr.hasMoreTokens())
            {
              word.set(itr.nextToken());
              context.write(word, one);

              //Counter counter = context.getCounter(CountersEnum.class.getName(), CountersEnum.INPUT_WORDS.toString());
              //counter.increment(1);
            }
            */
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

            //Ex1:
            /*
            for (IntWritable val : values) {
              sum += val.get();
            }
            */

            Iterator valuesIt = values.iterator();
            while(valuesIt.hasNext())
            {
                sum += Integer.parseInt(valuesIt.next().toString());
                //sum = sum + 1;
            }

            result.set(sum);
            context.write(key, result);
        }
    }//Reducer Class
}
