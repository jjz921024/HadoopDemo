package hadoop.pack2;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile.Reader;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.util.Tool;

/**
 * Created by jun on 17-5-17.
 */
public class LookupRecordByTemperature extends Configured implements Tool{

    public int run(String[] args) throws Exception {
        if (args.length != 2){
            System.out.println("err");
            return -1;
        }

        Path path = new Path(args[0]);
        IntWritable key = new IntWritable(Integer.parseInt(args[1]));
        Reader[] readers = MapFileOutputFormat.getReaders(null, path, getConf());
        Partitioner<IntWritable, Text> partitioner = new HashPartitioner<IntWritable, Text>();

        Text val = new Text();
        Writable entry = MapFileOutputFormat.getEntry(readers, partitioner, key, val);



        return 0;
    }
}
