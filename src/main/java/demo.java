import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

public class demo {
    public static void main(String[] args) throws IOException {
        createTable();
        insertOne();
        get();
        scan();
        scanValueFilter();
        scanColumnNameFilter();
        scanMultipleColumnNameFilter();
        rowFilter();
        testFilter();
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
            int age = new Random().nextInt(100);
            int score = new Random().nextInt(100);
            put.addColumn(Bytes.toBytes("grade"), Bytes.toBytes("china"), Bytes.toBytes(""+score));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("" + age));
            puts.add(put);
        }
        student.put(puts);

        student.close();
    }

    private static void get() throws IOException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "bigdata01");

        HTable student = new HTable(conf, "student");

        Get get = new Get(Bytes.toBytes("s1001"));
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
            String china = Bytes.toString(r.getValue(Bytes.toBytes("grade"), Bytes.toBytes("china")));
            System.out.println("age: " + age + ", china: " + china);
        }

        student.close();
    }

    private static void scanValueFilter() throws IOException {
        System.out.println("==========scanValueFilter========");
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "bigdata01");

        HTable student = new HTable(conf, "student");

        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes("info"),
                Bytes.toBytes("age"),
                CompareFilter.CompareOp.GREATER_OR_EQUAL,
                Bytes.toBytes("90"));

        Scan scan = new Scan();
        scan.setFilter(singleColumnValueFilter);
        ResultScanner scanner = student.getScanner(scan);
        for (Result r : scanner) {
            String age = Bytes.toString(r.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")));
            String china = Bytes.toString(r.getValue(Bytes.toBytes("grade"), Bytes.toBytes("china")));
            System.out.println("age: " + age + ", china: " + china);
        }

        student.close();
    }

    private static void scanColumnNameFilter() throws IOException {
        System.out.println("==========scanColumnNameFilter========");
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "bigdata01");

        HTable student = new HTable(conf, "student");

        ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes("age"));

        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner scanner = student.getScanner(scan);
        for (Result r : scanner) {
            String age = Bytes.toString(r.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")));
            String china = Bytes.toString(r.getValue(Bytes.toBytes("grade"), Bytes.toBytes("china")));
            System.out.println("age: " + age + ", china: " + china);
        }

        student.close();
    }

    private static void scanMultipleColumnNameFilter() throws IOException {
        System.out.println("==========scanMultipleColumnNameFilter========");
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "bigdata01");

        HTable student = new HTable(conf, "student");

        byte[][] columns = {Bytes.toBytes("age"), Bytes.toBytes("china")};
        MultipleColumnPrefixFilter filter = new MultipleColumnPrefixFilter(columns);

        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner scanner = student.getScanner(scan);
        for (Result r : scanner) {
            String age = Bytes.toString(r.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")));
            String china = Bytes.toString(r.getValue(Bytes.toBytes("grade"), Bytes.toBytes("china")));
            System.out.println("age: " + age + ", china: " + china);
        }

        student.close();
    }

    private static void rowFilter() throws IOException {
        System.out.println("==========rowFilter========");
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "bigdata01");

        HTable student = new HTable(conf, "student");

        RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                new RegexStringComparator("s1001"));

        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner scanner = student.getScanner(scan);
        for (Result r : scanner) {
            String age = Bytes.toString(r.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")));
            String china = Bytes.toString(r.getValue(Bytes.toBytes("grade"), Bytes.toBytes("china")));
            System.out.println("age: " + age + ", china: " + china);
        }

        student.close();
    }

    private static void testFilter() throws IOException {
        System.out.println("==========testFilter========");
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "bigdata01");

        HTable student = new HTable(conf, "student");

        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("info"),
                Bytes.toBytes("age"),
                CompareFilter.CompareOp.GREATER_OR_EQUAL,
                Bytes.toBytes("80"));

        SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes("grade"),
                Bytes.toBytes("china"),
                CompareFilter.CompareOp.GREATER_OR_EQUAL,
                Bytes.toBytes("80"));

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        filterList.addFilter(filter1);
        filterList.addFilter(filter2);

        Scan scan = new Scan();
        scan.setFilter(filterList);
        ResultScanner scanner = student.getScanner(scan);
        for (Result r : scanner) {
            String age = Bytes.toString(r.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")));
            String china = Bytes.toString(r.getValue(Bytes.toBytes("grade"), Bytes.toBytes("china")));
            System.out.println("age: " + age + ", china: " + china);
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
