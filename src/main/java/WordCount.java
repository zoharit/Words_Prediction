import java.io.IOException;
import java.util.LinkedList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
    enum UpdateCount{
        CNT
    }
    public static class MainMapper
        extends Mapper<Object, Text, Text, Text>{
        private final static IntWritable one = new IntWritable(1);
        private int oddline=0;

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] row = value.toString().split("\t");
            String ngram = row[0];
            String count = row[2];
            String res;
            if (oddline % 2 == 0) {
                res = count + ":" + "0";
            }
            else{
                res="0"+":"+count;
            }
            context.write(new Text(ngram),new Text(res));
                oddline++;
            }
    }



    public static class MainCombiner
            extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int count1 = 0;
            int count2=0;

            for (Text value : values) {
                String [] counts= value.toString().split(":");
                count1 += Integer.parseInt(counts[0]);
                count2+= Integer.parseInt(counts[1]);
            }
            String res= String.valueOf(count1)+":"+String.valueOf(count2);
            context.write(key, new Text(res));
        }
    }
    public static class MainReducer
            extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int count1 = 0;
            int count2=0;

            for (Text value : values) {
                String [] counts= value.toString().split(":");
                count1 += Integer.parseInt(counts[0]);
                count2+= Integer.parseInt(counts[1]);
            }
            context.getCounter(UpdateCount.CNT).increment(count1+count2);
            String res= String.valueOf(count1)+":"+String.valueOf(count2);
            context.write(key, new Text(res));
        }
    }
    public static void main(String[] args) throws Exception {
        try {
            System.out.println("main start");
            Configuration conf = new Configuration();
            for(int i=0;i<args.length;i++)
            {
                System.out.println("args["+i+"] ="+args[i]);
            }
            Job job1 = Job.getInstance(conf);
            job1.setJarByClass(WordCount.class);
            job1.setMapperClass(WordCount.MainMapper.class);
            job1.setReducerClass(WordCount.MainReducer.class);
            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(Text.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);
            job1.setInputFormatClass(SequenceFileInputFormat.class);

            //job1.setCombinerClass(WordCount.MainCombiner.class);
            FileInputFormat.addInputPath(job1, new Path(args[1]));
            FileOutputFormat.setOutputPath(job1, new Path("intermediate"));
            job1.waitForCompletion(true);
            conf.set("N", String.valueOf(job1.getCounters().findCounter(UpdateCount.CNT).getValue()));
            long c=Long.valueOf(conf.get("N"));

            System.out.println("counter in main is = "+c);
            Job job2 = Job.getInstance(conf); //new Job(conf);
            job2.setJarByClass(Calc.class);
            job2.setMapperClass(Calc.MainMapper.class);
            job2.setReducerClass(Calc.Calcreducer.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Pair.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            //job2.setCombinerClass(Calc.CalcCombiner.class);

            FileInputFormat.addInputPath(job2, new Path("intermediate"));
            FileOutputFormat.setOutputPath(job2, new Path("output1"));
            job2.waitForCompletion(true);
            System.out.println("job2 finshed");

            Configuration conf1 = new Configuration();
            conf1.set("N1", conf.get("N"));

            Job job3 = Job.getInstance(conf1); //new Job(conf);
            System.out.println("counter in main is second= "+conf.get("N"));


            job3.setJarByClass(calcProb.class);
            job3.setMapperClass(calcProb.MainMapper.class);
            job3.setReducerClass(calcProb.Calcreducer.class);
            job3.setMapOutputKeyClass(LongWritable.class);
            job3.setMapOutputValueClass(Text.class);
            job3.setOutputKeyClass(Text.class);
            job3.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job3, new Path("output1"));
            FileOutputFormat.setOutputPath(job3, new Path("output2"));
            System.out.println("starting job 3");
            job3.waitForCompletion(true);
            System.out.println("job3 finshed");

            Configuration join2With3Conf = new Configuration();
            Job job4 = Job.getInstance(join2With3Conf);
            job4.setJarByClass(JoinClasses.class);
            job4.setReducerClass(JoinClasses.joinReducer.class);
            job4.setPartitionerClass(JoinClasses.JoinPartition.class);
            job4.setMapOutputKeyClass(TaggedKey.class);
            job4.setMapOutputValueClass(Text.class);
            job4.setOutputKeyClass(Text.class);
            job4.setOutputValueClass(Text.class);
            MultipleInputs.addInputPath(job4, new Path("intermediate"),TextInputFormat.class, JoinClasses.MainMapper1.class);
            MultipleInputs.addInputPath(job4, new Path("output2"),TextInputFormat.class, JoinClasses.MainMapper2.class);
            FileOutputFormat.setOutputPath(job4, new Path("output3"));
            job4.waitForCompletion(true);
            System.out.println("job 4 finshed");

            Configuration sort=new Configuration();
            Job LastJob= Job.getInstance(sort);
            LastJob.setJarByClass(SortOutPut.class);
            LastJob.setMapperClass(SortOutPut.SortMapper.class);
            LastJob.setMapperClass(SortOutPut.SortMapper.class);
            LastJob.setReducerClass(SortOutPut.Sortreducer.class);
            LastJob.setMapOutputKeyClass(Text.class);
            LastJob.setMapOutputValueClass(Text.class);
            LastJob.setOutputKeyClass(Text.class);
            LastJob.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(LastJob, new Path("output3"));
            FileOutputFormat.setOutputPath(LastJob, new Path(args[2]));
            System.out.println("last job starting");
            LastJob.waitForCompletion(true);
            System.out.println("done all jobs");
         /*   ControlledJob controlledJob2 = new ControlledJob(job2,new LinkedList<ControlledJob>());
            ControlledJob controlledJob3 = new ControlledJob(job3,new LinkedList<ControlledJob>());
            ControlledJob controlledJob4 = new ControlledJob(job4,new LinkedList<ControlledJob>());
            ControlledJob controlledjobLastjob = new ControlledJob(LastJob,new LinkedList<ControlledJob>());

            controlledJob3.addDependingJob(controlledJob2);

            controlledJob4.addDependingJob(controlledJob3);
            controlledjobLastjob.addDependingJob(controlledJob4);
            JobControl jc = new JobControl("JC");
            //jc.addJob(controlledJob2);
            jc.addJob(controlledJob3);
            jc.addJob(controlledJob4);
            jc.addJob(controlledjobLastjob);
            Thread t=new Thread(jc);
            t.start();
            while (!jc.allFinished()) {
                if(jc.getFailedJobList().size() > 0)
                    System.exit(-1);
                Thread.sleep(3000);
                System.out.println("in loop");
            }*/
        System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}