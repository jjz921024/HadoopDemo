package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;


/**
 * Created by Jun on 2017/5/28.
 */
public class HbaseTest {
    public static void main(String[] args) throws IOException {
        filterTable();
    }

    public static void filterTable() {
        Table table = null;
        try {
            table = getTable();
            Scan scan = new Scan();

            //Filter filter = new ColumnPrefixFilter(Bytes.toBytes("name")); //列前缀过滤器,过滤列名
            //Filter filter = new PrefixFilter(Bytes.toBytes("rk0001")); //过滤rowkey
            //Filter filter = new PageFilter(3);  //每次返回3条数据

            ByteArrayComparable comp = new SubstringComparator("haha");
            Filter filter = new SingleColumnValueFilter(Bytes.toBytes("info"),
                    Bytes.toBytes("name"),
                    CompareFilter.CompareOp.EQUAL,
                    comp); //针对列值的过滤器
            scan.setFilter(filter);

            ResultScanner rs = table.getScanner(scan);
            for (Result result : rs) {
                printResult(result);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                IOUtils.closeStream(table);
            }
        }
    }


    public static void scanTable() {
        Table table = null;
        try {
            table = getTable();

            Scan scan = new Scan();
            //设置行键扫描范围
            scan.setStartRow(Bytes.toBytes("rk0002"));
            scan.setStopRow(Bytes.toBytes("rk0004"));
            //设置扫描的列
            scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));

            scan.setCacheBlocks(false); //设置是否缓存数据在本地
            scan.setBatch(2); //设置每次返回的cell个数
            scan.setCaching(2);

            ResultScanner rs = table.getScanner(scan);
            for (Result result : rs) {
                printResult(result);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                IOUtils.closeStream(table);
            }
        }
    }

    public static void printResult(Result result) {
        for (Cell cell : result.rawCells()) {
            System.out.println(Bytes.toString(CellUtil.cloneRow(cell)) + " : " +
                    Bytes.toString(CellUtil.cloneFamily(cell)) + " : " +
                    Bytes.toString(CellUtil.cloneQualifier(cell)) + " : " +
                    Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }

    public static void createNamespace() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);

        Admin admin = connection.getAdmin();

        NamespaceDescriptor nd = NamespaceDescriptor.create("ns1").build();
        admin.createNamespace(nd);

        admin.close();
    }

    public static void getData() throws IOException {
        Table table = getTable();
        Get get = new Get(Bytes.toBytes("rk0001"));

        Result result = table.get(get);
        /*byte[] value = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"));
        System.out.println(new String(value));*/

        /*for (Cell cell : result.rawCells()){
            System.out.println(new String(cell.getRow()) + " : " +
                    new String(cell.getFamily()) + " : " +
                    new String(cell.getQualifier()) + " : " +
                    new String(cell.getValue()));
        }*/
        for (Cell cell : result.rawCells()) {
            System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
            System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }

    public static void putData() throws IOException {
        Table table = getTable();

        Put put = new Put(Bytes.toBytes("rk0001"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("jjz"));

        table.put(put);
    }

    public static Table getTable() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        //HTable table = new HTable(conf, Bytes.toBytes("t1_api"));
        Connection conn = ConnectionFactory.createConnection();

        Table table = conn.getTable(TableName.valueOf("t1_api"));


        return table;
    }

    public static void createTable() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);

        Admin admin = connection.getAdmin();

        boolean b = admin.tableExists(TableName.valueOf("t1_api"));
        if (b) {
            admin.disableTable(TableName.valueOf("t1_api"));
            admin.deleteTable(TableName.valueOf("t1_api"));
        }

        HTableDescriptor desc = new HTableDescriptor("t1_api");
        desc.addFamily(new HColumnDescriptor("info"));

        admin.createTable(desc);
        admin.close();
        connection.close();
    }
}
