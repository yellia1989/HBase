import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

public class demo {
    public static void main(String[] args) throws IOException {
        createTable();
        insertOne();
        get();
        scan();
        drop();
    }

    private static void createTable() throws IOException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "bigdata01");

        HBaseAdmin client = new HBaseAdmin(conf);
        if (client.isTableAvailable("student")) return;

        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("student"));
        htd.addFamily(new HColumnDescriptor("info"));
        htd.addFamily(new HColumnDescriptor("grade"));

        client.createTable(htd);

        client.close();
    }

    private static  void insertOne() throws IOException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "bigdata01");

        HTable student = new HTable(conf, "student");

        List<Put> puts = new ArrayList<Put>();
        for (int i = 0; i < 10; ++i) {
            Put put = new Put(Bytes.toBytes("s100"+i));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("20"));
            puts.add(put);
        }
        student.put(puts);

        student.close();
    }

    private static void get() throws IOException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "bigdata01");

        HTable student = new HTable(conf, "student");

        Get get = new Get(Bytes.toBytes("s0001"));
        Result result = student.get(get);

        String age = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")));
        System.out.println("age: " + age);

        student.close();
    }

    private static void scan() throws IOException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "bigdata01");

        HTable student = new HTable(conf, "student");

        Scan scan = new Scan();
        ResultScanner scanner = student.getScanner(scan);
        for (Result r : scanner) {
            String age = Bytes.toString(r.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")));
            System.out.println("age: " + age);
        }

        student.close();
    }

    private static void drop() throws IOException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "bigdata01");

        HBaseAdmin client = new HBaseAdmin(conf);
        client.disableTable("student");
        client.deleteTable("student");

        client.close();
    }
}
