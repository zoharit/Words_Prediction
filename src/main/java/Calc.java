import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class Calc {
    public static class MainMapper
            extends Mapper<Object, Text, Text, Pair> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //System.out.println("counter at calc = "+ context.getCounter(WordCount.UpdateCount.CNT).getValue());
            String[] rows = value.toString().split("\t");
            String[] counts = rows[1].split(":");
            String N0 = counts[0];
            String N1 = counts[1];
            if (Integer.parseInt(N0)!= 0) {
                String resN0 =  "0"+ ":" +N0;
                context.write(new Text(resN0), new Pair(Integer.parseInt(N1), 1));
            }if (Integer.parseInt(N1) != 0) {
                String resN1 =  "1"+ ":" +N1 ;
                context.write(new Text(resN1), new Pair(Integer.parseInt(N0), 1));
            }
        }
    }


    public static class CalcCombiner
            extends Reducer<Text,Pair,Text,Pair> {
        public void reduce(Text key, Iterable<Pair> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            int counter=0;
            for (Pair value : values)
            {
                sum +=value.first;
                counter+=value.second;
            }
            //System.out.println("sum="+sum +"\n value="+counter);
            context.write(key, new Pair(sum, counter));
        }
    }

    public static class Calcreducer
            extends Reducer<Text,Pair,Text,Text> {
        public void reduce(Text key, Iterable<Pair> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            int counter=0;
            for (Pair value : values)
            {
                sum +=value.first;
                counter+=value.second;
                System.out.println("value="+value.toString());
            }
         //   System.out.println("sum="+sum +"\n value="+counter);
            context.write(key, new Text(String.valueOf(sum)+":"+String.valueOf(counter)));
        }
    }
}
