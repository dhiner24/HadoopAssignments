package Assignment4;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import proto.employee.Employee;
import proto.employee.EmployeeList;

import java.util.Arrays;

import static Assignment4.Constants.*;

public class EmployeeMapper extends Mapper<NullWritable, BytesWritable, ImmutableBytesWritable, Put> {

    ImmutableBytesWritable TABLE_NAME_TO_INSERT = new ImmutableBytesWritable(Bytes.toBytes(EMPLOYEE_TABLE_NAME));

    public void map(NullWritable key, BytesWritable value, Context context) {
        try {
            byte b[]=value.getBytes();
            EmployeeList employeeList=EmployeeList.parseFrom(Arrays.copyOf(value.getBytes(), value.getLength()));
            for(Employee employee:employeeList.getEmployeesList()){
                int employee_id= employee.getEmployeeId();
                byte byteArray[]=employee.toByteArray();
                Put put = new Put(Bytes.toBytes(employee_id));
                put.addColumn(Bytes.toBytes(EMPLOYEE), Bytes.toBytes(EMPLOYEE_DETAIL), byteArray);
                context.write(TABLE_NAME_TO_INSERT, put);
            }
        } catch(Exception exception) {
            exception.printStackTrace();
        }
    }
}
