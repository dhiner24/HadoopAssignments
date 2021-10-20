package Assignment4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

import static Assignment4.util.Constants.*;

public class MapDriver {

    public MapDriver()
    {
        System.out.println("Inside Map driver class");
    }


    public void runDriver() throws IOException, InterruptedException, ClassNotFoundException {
        Class<EmployeeMapper> employeeMapperClass = EmployeeMapper.class;
        Class<BuildingMapper> buildingMapperClass = BuildingMapper.class;
        driver(EMPLOYEE_TABLE_NAME, EMPLOYEE_HDFS_INPUT_PATH, EMPLOYEE_HFILE_OUTPUT_PATH, employeeMapperClass,null);
        //driver(BUILDING_TABLE_NAME, BUILDING_HDFS_INPUT_PATH, BUILDING_HFILE_OUTPUT_PATH, null,buildingMapperClass);
    }

    private void driver(String tableToInsert, String hdfsInputPath, String hfileOutputPath, Class<EmployeeMapper> employeeMapperClass,Class<BuildingMapper> buildingMapperClass)
            throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();
        configuration.set(DEFAULT_FS, HDFS_INPUT_URL);
        configuration.set(HDFS_IMPL, org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.set(FILE_IMPL, org.apache.hadoop.fs.LocalFileSystem.class.getName());
        Path output = new Path(hfileOutputPath);
        FileSystem hdfs = FileSystem.get(URI.create(hfileOutputPath), configuration);
        if (hdfs.exists(output)) {                                      // delete existing directory
            hdfs.delete(output, true);
        }
        Job job = Job.getInstance(configuration);
        job.setJarByClass(MapDriver.class);
        job.setJobName(BULK_LOADING_MESSAGE + tableToInsert);
        job.setInputFormatClass(WholeFileInputFormat.class);
        //job.setInputFormatClass(TextInputFormat.class);
        //TextInputFormat.setInputPaths(job, hdfsInputPath);

        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, hdfsInputPath);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        if(employeeMapperClass!=null) {
            job.setMapperClass(employeeMapperClass);
        }
        else {
            job.setMapperClass(buildingMapperClass);
        }
        FileOutputFormat.setOutputPath(job, new Path(hfileOutputPath));
        job.setMapOutputValueClass(Put.class);
        TableName tableName = TableName.valueOf(tableToInsert);

        Connection conn = ConnectionFactory.createConnection(configuration);
        Table tablee = conn.getTable(tableName);
        RegionLocator regionLocator = conn.getRegionLocator(tableName);

        HFileOutputFormat2.configureIncrementalLoad(job, tablee, regionLocator);

        boolean b = job.waitForCompletion(true);
        System.out.println(b);
        if (job.isSuccessful()) {
            //It bulk uploads data into the the table
            try {
                Configuration config = HBaseConfiguration.create();
                BulkLoadHFiles.create(config).bulkLoad(tableName, new Path(hfileOutputPath));
                System.out.println("successful upload to "+tableToInsert+" table");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
