import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import java.io.IOException;
import java.util.List;

public class calcProb {

    public static class MainMapper
            extends Mapper<Object, Text, LongWritable, Text> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] rows = value.toString().split("\t");
            String[] counts = rows[0].split(":");
            String N1_N0 = counts[1];
            context.write(new LongWritable(Long.valueOf(N1_N0)),new Text(rows[1]) );
        }
    }

    public static class Calcreducer
            extends Reducer<LongWritable,Text,Text,Text> {

        private double N=0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
           System.out.println("got setup here");
            Configuration conf = context.getConfiguration();
            System.out.println("got setup here"+ conf.get("N1"));

            N= Double.valueOf(conf.get("N1"));

           // N=context.getCounter(WordCount.UpdateCount.CNT).getValue();
        }



        public void reduce(LongWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            List<Text> myList = Lists.newArrayList(values);
            System.out.println(myList.size());
            double T=0,N=0;
            for(Text val:myList)
            {
            String []T_N=val.toString().split(":");
            T+=Integer.valueOf(T_N[0]);
            N+=Integer.valueOf(T_N[1]);
            }
            double p=0;
            if(N==0)
            {
                p=0;
            }
            else
            {
                p=(T/((double) N));
                p=p/this.N;
            System.out.println("N="+this.N);
            }
            String res=String.valueOf(p);
            context.write(new Text(String.valueOf(key.get())),new Text(res));
        }
    }
}