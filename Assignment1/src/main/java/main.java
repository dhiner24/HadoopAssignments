import org.apache.hadoop.conf.Configuration;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

//Rename the class appropriately. i.e. HbaseWriterMain.
//Add comments(Class and function comments) on what this code is going to do.
//Follow object oriented approach, create a separate class for HBaseWriter and call it from main function.
public class main {
    public static final String encoding = "UTF-8";
    public static final String hdfsUrl = "hdfs://localhost:8020";
    public static String internalUrl = "/PeopleData/";
    private static String columnFamily = "Name";
    private static Configuration config = new Configuration();

    public static String getPath(String url) {
        return hdfsUrl + url;
    }

    public static HashMap<Integer, String> getColumnMapping() {
        HashMap<Integer, String> hm = new HashMap<>();
        hm.put(0, "Name");
        hm.put(1, "Age");
        hm.put(2, "Company");
        hm.put(3, "Building_code");
        hm.put(4, "Phone_Number");
        hm.put(5, "Address");
        return hm;
    }
    //Avoid static functions unless it is a utility class.
    public static void insertDataToHbase(String[] record, int rowId) throws IOException {

        Table table = null;
        Connection connection = null;
        HashMap<Integer, String> hm = getColumnMapping();

        try
        {
            Configuration conf = HBaseConfiguration.create();
            connection = ConnectionFactory.createConnection(conf);
            table = connection.getTable(TableName.valueOf("people"));
            Put p = new Put(Bytes.toBytes(String.valueOf(rowId)));
            for(int i = 0; i < record.length; ++ i) {
                String qualifier = hm.get(i);
                if(qualifier != null)
                    p.addColumn(Bytes.toBytes(columnFamily),
                            Bytes.toBytes(qualifier),
                            Bytes.toBytes(record[i]));
            }

            table.put(p);

        }catch (Exception e) { //Dont catch generic exception, catch specific exceptions.
            e.printStackTrace();
        } finally {
            if(table != null)
                table.close();
            if(connection != null)
                connection.close();
        }
    }

    //Avoid static functions.
    public static void storeInHBASE(FileSystem hdfs, String uri) throws IOException {

        config.set("fs.defaultFS", hdfsUrl);
        FileStatus[] fileStatus = hdfs.listStatus(new Path(uri));
        Path[] paths = FileUtil.stat2Paths(fileStatus);

        FileSystem fileSystem = FileSystem.get(config);

        int rowId = 1;
        for (Path path : paths) {
            FSDataInputStream inputStream = fileSystem.open(path);
            //Use meaningfull variable names.
            String out = IOUtils.toString(inputStream, encoding).split("\n")[1];
            String[] record = out.split(",");
            insertDataToHbase(record, rowId);
            rowId++;
            inputStream.close();
        }
        fileSystem.close();

    }

    public static void main(String[] args) throws Exception {

    String uri = getPath(internalUrl);

    FileSystem fs = FileSystem.get(new URI(hdfsUrl),config);

    storeInHBASE(fs,uri);

    }
}
