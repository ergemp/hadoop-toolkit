package org.ergemp.toolkit.hadoop.training.MapreduceExamples.Reducers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class WordCountReducer {
    //Reducer Class
    public static class Reduce extends Reducer<Text, IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            Integer sum = 0;

            Iterator valuesIt = values.iterator();
            while(valuesIt.hasNext()) {
                sum += Integer.parseInt(valuesIt.next().toString());
            }

            result.set(sum);
            context.write(key, result);
        }
    }//Reducer Class
}
