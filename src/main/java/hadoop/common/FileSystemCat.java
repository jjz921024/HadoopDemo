package hadoop.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * Created by Jun on 2017/4/27.
 */
public class FileSystemCat {
    public static void main(String[] args) throws Exception{
        String uri = args[0];
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream in = null;
        try {
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);

        } finally {
            IOUtils.closeStream(in);
        }
    }
}
