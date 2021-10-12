package com.anmol;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class MapReduceReducer {

    public static class Reduce extends MapReduceBase implements
            Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key,
                           Iterator<IntWritable> value,
                           OutputCollector<Text, IntWritable> outputCollector,
                           Reporter reporter) throws IOException {

            int wordCount = 0;
            while(value.hasNext()) {
                wordCount += value.next().get();
            }
            outputCollector.collect(key, new IntWritable(wordCount));
        }
    }
}
