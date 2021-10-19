package Assignment4;

public class Constants {

    public static final String userDirectory = System.getProperty("user.dir");
    public static final String PATH = userDirectory + "/src/main/resources/";

    public static final String EMPLOYEE_TABLE_NAME = "employee";
    public static final String BUILDING_TABLE_NAME = "building";

    public static final String EMPLOYEE_HDFS_OUTPUT_PATH = "hdfs://localhost:8020/dataDirectory/employeeSerializeFile";
    public static final String BUILDING_HDFS_OUTPUT_PATH = "hdfs://localhost:8020/dataDirectory/buildingSerializeFile";
    public static final String BUILDING_SERIALIZED_FILE = "buildingSerializedFile";
    public static final String EMPLOYEE_SERIALIZED_FILE = "employeeSerializedFile";

    public static final String COLUMN_FAMILY_BUILDING = "building";
    public static final String TABLE_ALREADY_EXISTS = "Table already exists";
    public static final String TABLE_CREATED = " table created";
    public static final String COLUMN_FAMILY_EMPLOYEE = "employee";

    public static final String EMPLOYEE_HFILE_OUTPUT_PATH = "hdfs://localhost:8020/dataDirectory/employee_hfile/";
    public static final String BUILDING_HFILE_OUTPUT_PATH = "hdfs://localhost:8020/dataDirectory/building_hfile/";
    public static final String EMPLOYEE_HDFS_INPUT_PATH = EMPLOYEE_HDFS_OUTPUT_PATH;
    public static final String BUILDING_HDFS_INPUT_PATH = BUILDING_HDFS_OUTPUT_PATH;
    public static final String DEFAULT_FS = "fs.defaultFS";
    public static final String HDFS_INPUT_URL = "hdfs://localhost:8020/";
    public static final String HDFS_IMPL = "fs.hdfs.impl";
    public static final String FILE_IMPL = "fs.file.impl";
    public static final String BULK_LOADING_MESSAGE = "Bulk Loading HBase Table::";
    public static final String EMPLOYEE = "employee";
    public static final String EMPLOYEE_DETAIL = "employee_details";
    public static final String BUILDING = "building";
    public static final String BUILDING_DETAILS = "building_details";
}
