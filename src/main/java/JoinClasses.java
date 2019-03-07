import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;
import org.mockito.asm.tree.analysis.Value;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class JoinClasses {


    public static class MainMapper1
            extends Mapper<Object, Text, TaggedKey, Text> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] rows = value.toString().split("\t");
            String ngram = rows[0];
            String []n1_n0=rows[1].split(":");
            long r=Long.valueOf(n1_n0[0])+Long.valueOf(n1_n0[1]);
            System.out.println("r = ="+r);
            context.write(new TaggedKey("2n",r),new Text(ngram)) ;



        }
    }

    public static class MainMapper2
            extends Mapper<Object, Text,TaggedKey, Text> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] rows = value.toString().split("\t");
            String R = rows[0];
            String prob=rows[1];
            long long_r = Long.parseLong(String.valueOf(R));
            context.write(new TaggedKey("1p",long_r),new Text(prob) );
        }
    }
    public static class joinReducer
            extends Reducer<TaggedKey,Text,Text,Text> {

        Object currentTag = null;
        Long currentKey = null;
        List<Writable> table1ValuesList = new LinkedList<Writable>();
        boolean writeMode = false;
        double prob=0;

        public void reduce(TaggedKey taggedKey, Iterable<Text> values,
                           Context context)
                throws IOException, InterruptedException
        {
            if (currentKey == null || !currentKey.equals(taggedKey.getKey()))
            {
                prob=0;
            }
            if (taggedKey.getTag().equals("1p"))
            {
                List<Text> myList = Lists.newArrayList(values);
                System.out.println("mylist length is "+myList.size());
                prob=Double.valueOf(myList.get(0).toString());
                System.out.println("PROB IS "+prob);
            }
            if(!taggedKey.getTag().equals("1p"))
            {
                for(Text val :values)
                {
                    context.write(val,new Text(String.valueOf(prob)));
                }
            }
            currentKey = taggedKey.getKey();
           /* if (currentKey == null || !currentKey.equals(taggedKey.getKey())) {
           table1ValuesList.clear();
            writeMode = false;
        } else
            writeMode = (currentTag != null && !currentTag.equals(taggedKey.getTag()));
            if (writeMode) {
            System.out.println("taggek key is = "+taggedKey.getKey());

                crossProduct(taggedKey.getKey(), values, context);
            }
            else {
                for (Writable value : values) {
                    System.out.println("value = "+value.toString());
                    table1ValuesList.add(value);
                }
            }
            currentTag = taggedKey.getTag();

            currentKey = taggedKey.getKey();
*/


        }


        protected void crossProduct(Long key,Iterable<Text> table2Values ,Context context) throws IOException, InterruptedException {
            // This specific implementation of the cross product, combine the data of the customers and the orders (
            // of a given costumer id).
            for (Writable table2Value : table2Values){
                context.write(new Text(String.valueOf(key)), new Text(table1ValuesList.toString() + ": " + table2Value.toString()));
        }

        }
    }
    public static class JoinPartition extends Partitioner<TaggedKey,Text> {
        // ensure that keys with same key are directed to the same reducer
        @Override

        public int getPartition(TaggedKey key,Text value, int numPartitions) {
            return  Math.abs(key.getKey().hashCode()) % numPartitions;
        }
    }


}
