package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileAsBinaryOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by Jun on 2017/6/7.
 */
public class SquenceTest extends Configured implements Tool{

    public static class  conventMapper extends Mapper<Text, Text, LongWritable, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            LongWritable key_long = new LongWritable(Long.parseLong(key.toString()));
            context.write(key_long, value);
        }
    }

    public int run(String[] args) throws Exception {
        //job
        String[] intPath = new String[]{"hdfs://172.18.118.69:8020" + args[1]};
        String outPath = "hdfs://172.18.118.69:8020" + args[2];
        Configuration conf = getConf();
        String jobName = "converSeq";

        JobInitModel jobs = new JobInitModel(intPath, outPath, conf, null, jobName, SquenceTest.class,
                KeyValueTextInputFormat.class,
                SequenceFileOutputFormat.class,

                SquenceTest.conventMapper.class,
                IntWritable.class, Text.class,

                null, null,
                null,
                null, null);

        return BaseDriver.initJob(new JobInitModel[]{jobs}) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(new SquenceTest(), args);
        System.exit(status);
    }
}
