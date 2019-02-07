package toolkit.hadoop.training.MapreduceExamples.Mappers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountMapper {
    //Mapper Class
    public static class Map extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final Text word = new Text();

        @Override
        public void map(Object key, Text value, Mapper.Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer itr = new StringTokenizer(line, " ");

            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }//Mapper Class
}
