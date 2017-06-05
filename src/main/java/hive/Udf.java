package hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;


/**
 * Created by Jun on 2017/5/19.
 */
public class Udf extends UDF{
    public Text evaluate(Text a){
        return new Text(a.toString() + '*');
    }
}
