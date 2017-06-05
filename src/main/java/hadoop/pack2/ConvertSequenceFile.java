package hadoop.pack2;

import hadoop.common.JobBuilder;
import hadoop.common.NcdcRecordParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by jun on 17-5-17.
 */
public class ConvertSequenceFile extends Configured implements Tool{


    public int run(String[] strings) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), strings);

        if (job == null) {
            return -1;
        }

        job.setInputFormatClass(SequenceFileInputFormat.class);
        //job.setMapperClass(CleanerMapper.class);
        job.setOutputKeyClass(IntWritable.class);
        //job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(30);
        //job.setOutputFormatClass(MapFileOutputFormat.class);

        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ConvertSequenceFile(), args);
        System.exit(exitCode);
    }

    static class CleanerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value);
            if (parser.isValidTemperature()){
                context.write(new IntWritable(parser.getAirTemperature()), value);
            }
        }
    }
}
