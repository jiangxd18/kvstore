import cn.helium.kvstore.common.KvStoreConfig;

import java.util.HashMap;

//存放整个项目的全局变量
public class Mydata {

    //hdfs
    public static final String hdfsURL = KvStoreConfig.getHdfsUrl();
    //public static final String hdfsURL = "hdfs://localhost:9000";
    //logs,给的100MB本地空间的地址/opt/localdisk/Logs
    //public static final String logAddress = "/Users/donng/logs";
    public static final String logAddress = "/opt/localdisk/Logs";

    //定义每个数据块的大小为3MB
    public static final int MAX_BLOCK_SIZE = 1 * 1024 * 1024;


    /////各种索引表的文件数据

    //日志hash
    public static HashMap<String, MyLog> logMap = new HashMap<>();

    //每个文件当中现在写到的偏移量是多少
    public static HashMap<Integer, Long> offset = new HashMap<>();

    //以key的长度为关键字,地址为值，建立索引文件的hash
    public static HashMap<Integer,String> indexFiles = new HashMap<>();

    //以key的长度为关键字，地址为值，建立索引文件的hash
    public static HashMap<Integer,String> dataFiles = new HashMap<>();



}
