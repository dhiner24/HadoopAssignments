package Assignment4b;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import proto.building.Building;
import proto.employee.Employee;

import java.io.IOException;
import java.util.Arrays;

import static Assignment4b.util.Constants.EMPLOYEE_TABLE_BYTES;

public class MapperClass extends TableMapper<IntWritable, Result> {

    public MapperClass(){
        System.out.println("Reached mapper Class");
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result result, Mapper<ImmutableBytesWritable, Result, IntWritable, Result>.Context context) throws IOException, InterruptedException {
        TableSplit tableSplit = (TableSplit) context.getInputSplit();
        byte[] tableName = tableSplit.getTableName();
        if(Arrays.equals(tableName, EMPLOYEE_TABLE_BYTES)){
            Employee employee = Employee.parseFrom(result.value());
            System.out.println("Employee : " + employee);
            int building_code = employee.getBuildingCode();
            context.write(new IntWritable(building_code), result);
        }else{
            Building building = Building.parseFrom(result.value());
            System.out.println("Building : " +building);
            int building_code = building.getBuildingCode();
            context.write(new IntWritable(building_code), result);
        }
    }
}
