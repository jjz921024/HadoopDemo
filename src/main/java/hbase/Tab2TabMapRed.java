package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by Jun on 2017/5/27.
 */
public class Tab2TabMapRed extends Configured implements Tool{

    public static class Tab2TabMapper extends TableMapper<Text,Put>{
        private Text rowkey = new Text();

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            byte[] bytes = key.get();
            rowkey.set(Bytes.toString(bytes));  //key

            //每个map任务对一个key操作
            Put put = new Put(bytes);
            for (Cell cell : value.rawCells()){
                //add column
                if ("cl1".equals(CellUtil.cloneFamily(cell))){
                    put.add(cell);
                }
            }
        }
    }

    public static class Tab2TabReduce extends TableReducer<Text, Put, ImmutableBytesWritable>{
        @Override
        protected void reduce(Text key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
            for (Put put : values) {
                context.write(null, put);
            }
        }
    }

    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);

        TableMapReduceUtil.initTableMapperJob(
                "t1", scan, Tab2TabMapper.class, Text.class, Put.class, job);

        TableMapReduceUtil.initTableReducerJob(
                "t2", Tab2TabReduce.class, job);

        job.setNumReduceTasks(1);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        int status = ToolRunner.run(conf, new Tab2TabMapRed(), args);
        System.exit(status);
    }
}
