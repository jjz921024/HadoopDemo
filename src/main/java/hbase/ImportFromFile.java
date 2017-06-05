package hbase;

import org.apache.commons.cli.*;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.CellCounter;
import org.apache.hadoop.hbase.mapreduce.RowCounter;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by Jun on 2017/6/4.
 */
public class ImportFromFile extends Configured implements Tool {

    static class ImportMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Writable> {
        private byte[] family = null;
        private byte[] qualifier = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String column = context.getConfiguration().get("conf.column");
            byte[][] colkey = KeyValue.parseColumn(Bytes.toBytes(column));
            family = colkey[0];
            if (colkey.length > 1) {
                qualifier = colkey[1];
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String lineString = value.toString();

            byte[] rowkey = DigestUtils.md5(lineString);
            Put put = new Put(rowkey);
            put.addColumn(family, qualifier, Bytes.toBytes(lineString));

            context.write(new ImmutableBytesWritable(rowkey), (Writable) put);
        }
    }

    private static CommandLine parseArgs(String[] args) {
        Options options = new Options();

        Option o = new Option("t", "table", true, "table to import");
        o.setArgName("table-name");
        o.setRequired(true);
        options.addOption(o);

        o = new Option("c", "column", true, "column for hbase");
        o.setArgName("family:qualifier");
        o.setRequired(true);
        options.addOption(o);

        o = new Option("i", "input", true, "input file path");
        o.setRequired(true);
        o.setArgName("path in hdfs");
        options.addOption(o);

        o = new Option("d", "debug", false, "debug switch");
        options.addOption(o);

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return cmd;
    }

    public int run(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();

        //todo
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        CommandLine cmd = parseArgs(otherArgs);
        String table = cmd.getOptionValue("t");
        String column = cmd.getOptionValue("c");
        String input = cmd.getOptionValue("i");

        //列参数设置到配置文件中
        conf.set("conf.column", column);
        Job job = Job.getInstance(conf, "Import from file" + input + "into table" + table);
        job.setJarByClass(ImportFromFile.class);
        job.setMapperClass(ImportMapper.class);
        //输出格式 到hbase
        job.setOutputFormatClass(TableOutputFormat.class);
        //指定表名
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, table);

        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);

        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(input));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(new ImportFromFile(), args);
        System.exit(status);
    }
}
