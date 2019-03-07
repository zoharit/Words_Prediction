import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortOutPut {

        public static class SortMapper
                extends Mapper<LongWritable, Text, Text, Text> {

            public void map(LongWritable key, Text value, Context context
            ) throws IOException, InterruptedException {
                String[] rows = value.toString().split("\t");
                String[] words = rows[0].split(" ");
                String probminusone = String.valueOf(1 - Double.valueOf(rows[1]));
                if (words.length>=2) {
                    context.write(new Text(words[0] + " " + words[1] + " " + probminusone),
                            new Text(rows[0] + "/t" + rows[1]));
                }
            }
        }
        public static class Sortreducer
                extends Reducer<Text,Text,Text,Text> {
            public void reduce(Text key, Iterable<Text> values,
                               Context context
            ) throws IOException, InterruptedException {
                for(Text value:values){
                    String [] keyValue= value.toString().split("/t");
                    context.write(new Text(keyValue[0]), new Text(keyValue[1]));
                }
            }
        }
    }
