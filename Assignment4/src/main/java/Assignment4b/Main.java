package Assignment4b;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static Assignment4b.util.Constants.*;

public class Main {


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        List<Scan> scans = new ArrayList();
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        scan.setAttribute(SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(EMPLOYEE_TABLE));
        scans.add(scan);
        scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        scan.setAttribute(SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(BUILDING_TABLE));
        scans.add(scan);

        Configuration config = new Configuration();
        Job job = Job.getInstance(config);
        job.setJobName("MRTableReadWrite");
        job.setJarByClass(Main.class);

        TableMapReduceUtil.initTableMapperJob(
                scans,
                MapperClass.class,
                IntWritable.class,
                Result.class,
                job
        );
        TableMapReduceUtil.initTableReducerJob(
                EMPLOYEES_WITH_CAFETRIA_CODE_TABLE,
                ReducerClass.class,
                job
        );

        Path output = new Path(ASSIGNMENT_4_2_HDFS_OUTPUT_PATH);
        FileSystem fs = FileSystem.get(URI.create(ASSIGNMENT_4_2_HDFS_OUTPUT_PATH), config);

        // delete existing directory
        if (fs.exists(output)) {
            fs.delete(output, true);
        }
        FileOutputFormat.setOutputPath(job, new Path(ASSIGNMENT_4_2_HDFS_OUTPUT_PATH));

        boolean b = job.waitForCompletion(true);
        System.out.println(b);
        if (job.isSuccessful()) {
            System.out.println("Cafeteria code added to employee table");
        }
    }
}
