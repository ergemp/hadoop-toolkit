package org.ergemp.toolkit.hadoop.processors.mapreducers;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class TestJob {
    //Mapper Class
    public static class Map extends Mapper<Object, Text, Text, IntWritable>
    {

        private final static IntWritable one = new IntWritable(1);
        private final Text word = new Text();

        @Override
        public void map(Object key, Text value, Mapper.Context context)
                throws IOException, InterruptedException
        {

            String line = value.toString();
            String word2 = "";
            if (!line.trim().equalsIgnoreCase(""))
            {
                if (line.split(",").length > 4)
                {
                    word2=line.split(",")[4];
                    if (word2.trim().equalsIgnoreCase("Female"))
                    {
                        line = line.replace("Female","hatun");
                    }
                    else
                    {
                        line = line.replace("Male","eleman");
                    }
                }
            }

            word.set(line);
            context.write(word , null);
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable ,Text, IntWritable>
    {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            Integer sum = 0;

            Iterator valuesIt = values.iterator();
            while(valuesIt.hasNext()) {
                sum = sum + Integer.parseInt(valuesIt.next().toString());
            }

            result.set(sum);

            context.write(key, result);
        }
    }
}
