package toolkit.hadoop.processors.mapreducers;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MultipleInputsNaiveJoin {
    //Mapper Class
    public static class Map1 extends Mapper<LongWritable,Text,Text,Text>
    {
        //private final static IntWritable one = new IntWritable(1);
        //private final Text word = new Text();

        Text keyEmit = new Text();
        Text valEmit = new Text();

        //@Override
        public void map(LongWritable k, Text value, Mapper.Context context)
                throws IOException, InterruptedException
        {
            Configuration conf = context.getConfiguration();

            String line=value.toString();
            String[] words=line.split("\\$\\$");

            keyEmit.set(words[Integer.parseInt(conf.get("searchKey").split(",")[0])]);
            valEmit.set("map1$$" + line);
            context.write(keyEmit, valEmit);
        }

    } //Mapper Class

    //Mapper Class
    public static class Map2 extends Mapper<LongWritable,Text,Text,Text>
    {
        //private final static IntWritable one = new IntWritable(1);
        //private final Text word = new Text();

        Text keyEmit = new Text();
        Text valEmit = new Text();

        //@Override
        public void map(LongWritable k, Text value, Mapper.Context context)
                throws IOException, InterruptedException
        {

            Configuration conf = context.getConfiguration();

            String line=value.toString();
            String[] words=line.split("\\$\\$");

            keyEmit.set(words[Integer.parseInt(conf.get("searchKey").split(",")[1])]);
            valEmit.set("map2$$" + line);
            context.write(keyEmit, valEmit);

        }

    } //Mapper Class

    //Reducer Class
    public static class Reduce extends Reducer<Text,Text,Text,Text>
    {
        Text valEmit = new Text();
        ArrayList<String> aSecond = new ArrayList();

        String merge = "";
        String sFirst = "";
        String sSecond = "";

        public void reduce(Text key, Iterable<Text> values, Reducer.Context context)
                throws IOException , InterruptedException
        {
            int i = 0;
            aSecond.clear();

            sFirst = "";
            for(Text value:values)
            {
                if  (
                        value.toString().split("\\$\\$")[0].trim().equalsIgnoreCase("map1") &&
                                sFirst.trim().equalsIgnoreCase("")
                )
                {
                    sFirst = value.toString().trim().substring(value.toString().indexOf("$$")+2);
                }
                else if (
                        value.toString().split("\\$\\$")[0].trim().equalsIgnoreCase("map2") &&
                                sFirst.trim().equalsIgnoreCase("")
                )
                {
                    aSecond.add(value.toString().trim().substring(value.toString().indexOf("$$")+2));
                }
                else if (
                        value.toString().split("\\$\\$")[0].trim().equalsIgnoreCase("map2") &&
                                !sFirst.trim().equalsIgnoreCase("")
                )
                {
                    context.write(null, new Text(sFirst + "$$" + value.toString().trim().substring(value.toString().indexOf("$$")+2)));
                }
            }

            if  (
                    aSecond.size() > 0 &&
                            !sFirst.trim().equalsIgnoreCase("")
            )
            {
                for(String sSecond : aSecond)
                {
                    context.write(null, new Text(sFirst + "$$" + sSecond.trim()));
                }
                aSecond.clear();
            }

            /*
            for(Text value:values)
            {
                sSecond = "";
                if (value.toString().split("\\$\\$")[0].trim().equalsIgnoreCase("map2"))
                {
                    sSecond = value.toString();

                }
                context.write(null, new Text(sFirst + "$$" + sSecond));

                //if(i == 0){
                //    merge = value.toString()+",";
                //}
                //else{
                //    merge += value.toString();
                //}

                //if (i != 0)
                //{
                //  context.write(key, new Text(sFirst + "$$" + sSecond));
                //}

                i++;
            }
            */
            //valEmit.set(merge);
            //context.write(key, valEmit);
        }
    } //Reducer Class

    public static void main (String[] args) {
        try {
            Configuration conf = new Configuration();

            conf.set("fs.defaultFS", "localhost:9000");
            conf.set("mapreduce.framework.name", "yarn");
            conf.set("yarn.resourcemanager.address", "localhost:8032");

            Job job = Job.getInstance(conf, "multipleInputs");
            job.setJarByClass(MultipleInputsNaiveJoin.class);

            MultipleInputs.addInputPath(job, new Path(""), TextInputFormat.class, MultipleInputsNaiveJoin.Map1.class);
            MultipleInputs.addInputPath(job, new Path(""), TextInputFormat.class, MultipleInputsNaiveJoin.Map2.class);

            job.setReducerClass(MultipleInputsNaiveJoin.Reduce.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileOutputFormat.setOutputPath(job, new Path("/output/multipleInputs"));

            Integer retVal = job.waitForCompletion(true) ? 0 : -1;
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
