package toolkit.hadoop.processors.mapreducers;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.LinkedHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.*;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.JSONArray;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.json.simple.parser.ContainerFactory;

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
