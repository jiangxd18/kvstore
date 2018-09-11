import cn.helium.kvstore.common.KvStoreConfig;
import cn.helium.kvstore.processor.Processor;
import cn.helium.kvstore.rpc.RpcClientFactory;
import cn.helium.kvstore.rpc.RpcServer;
import com.google.gson.Gson;

import java.nio.ByteBuffer;
import java.util.*;

public class Myprocessor implements Processor {
    //可调用的api
    //RpcServer.getRpcServerId() 获取当前的kvpod的id
    //public static byte[] inform(int index, byte[] info)
    //RpcClientFactory.inform()进程之间的通信
    // public static int getServersNum() 获取当前进程的数量KvStoreConfig.getServersNum()
    //KvStoreConfig.getHdfsUrl()获得hdfs的url


    //已经写日志中的数据，但是还没有落盘，建立索引，在落盘后可以删除
    private HashMap<String, byte[]> cacheInfo = new HashMap<>();

    //记录进程1
    private HashMap<Integer, MyTable> first = new HashMap<>();

    //编码和解码的缓存buffer,暂时分2kB
    private ByteBuffer buffer = ByteBuffer.allocate(1024);

    //保存hdfs日志数据的路径
    //key的编码方式为keylen_kvpodid
    private Map<String, String> processLog = new HashMap<>();
    //保存hdfs实际数据的路径
    //key的编码方式为keylen_kvpodid
    private Map<String, String> processData = new HashMap<>();

    //根据从hdfs中读到的索引地址，建立确定的数据索引，key为keylen_kvpodid 值为数据键内容和偏移长度
    private HashMap<String, HashMap<String, byte[]>> indexTable = new HashMap<>();

    //构造函数
    public Myprocessor() {
        //进程初始化，主要做的任务是为当前的进程建立完整的索引
        //1. 为没有落盘的存在日志地址中的日志数据建立一张完整的索引表
        //2. 把已经持久化的索引文件读出来，建立一张索引表

        try {
            //
            List<String> localLog = MyLocalFile.listAll(Mydata.logAddress);
            List<String> hdfsdata = MyFile.listAll(Mydata.logAddress);

            int length = localLog.size();

            //System.out.println(length);
            //代表现在起的进程中，还没有进程put过，本地日志中没有数据
            if (length == 0) return;

            //说明已经有进程写过数据，那么从持久化数据库中读取数据，建立完整索引
            for (String dataofhdfs : hdfsdata) {

                //hdfs中的数据有两种一种是索引数据，一种是真实的数据
                //索引数据address/index_keylen_kvpodid
                //数据的格式 address/data_keylen_kvpodid

                String[] sign = dataofhdfs.split("_");
                int keyLen = Integer.parseInt(sign[1]);
                String id = keyLen + "_" + sign[2];

                if (sign[0].contains("index")) {
                    //索引文件地址索引
                    processLog.put(id, dataofhdfs);
                } else if (sign[0].contains("address"))
                    //数据文件地址索引
                    processData.put(id, dataofhdfs);

            }

            //已经写在日志中，但还没有落盘的数据直接建立一张map索引
            //不管是不是当前进程的索引，都将这个索引放在这张表中
            for (String path : localLog) {
                try {
                    byte[] filedata = MyLocalFile.readFile(path);
                    //特殊情况
                    if (filedata == null) continue;

                    int filelen = filedata.length;

                    //解码
                    int index = 0;
                    while (index < filelen) {
                        int totalLen = decodeComplete(filedata, index);
                        //读取totalLen+4 即4B 32位
                        int keyLen = decodeComplete(filedata, index + 4);

                        //同上,读取了两个int数据
                        index += 8;

                        byte[] tmpkey = Arrays.copyOfRange(filedata, index, index + keyLen);

                        byte[] value = Arrays.copyOfRange(filedata, index + keyLen, index + totalLen);

                        cacheInfo.put(new String(tmpkey), value);
                        index += totalLen;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            ////////可能有点儿问题
            //根据已经从hdfs获取到的索引文件地址建立新的一张索引表
            for (String logName : processLog.keySet()) {
                ////hdfs数据读取
                byte[] content = MyFile.readHDFSFile(processLog.get(logName));
                //特殊情况
                if (content == null)
                    continue;
                int size = content.length;
                //解码
                int offset = 0;
                int keyoffset = 0;

                HashMap<String, byte[]> tmp = new HashMap<>();

                while (offset < size) {
                    int lengthofContent = content[offset++];

                    keyoffset = offset + lengthofContent - 8 - 4;
                    byte[] key_content = Arrays.copyOfRange(content, offset, keyoffset);

                    offset = offset + lengthofContent;
                    byte[] offset_len = Arrays.copyOfRange(content, keyoffset, offset);
                    tmp.put(new String(key_content), offset_len);
                }

                indexTable.put(logName, tmp);

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    //
    @Override
    public Map<String, String> get(String key) {
        //get我们讨论了三种情况
        //第一种情况是现在执行get的该进程本身就put过这条数据，并且还在本地的日志文件当中，那么直接根据启进程
        //时读取到的日志文件的数据去查找


        //第二种情况是日志中没有，也就是说这部分数据和索引已经落盘完成了，那么去读取hdfs中的完整索引数据

        //第三种情况是hdfs中的完整索引也没有找到，那么这种情况下只能说明put是其他的进程完成的，并且这部分数据还没有被落盘
        //那么，就去相邻的进程去查找

        //如果三种情况都没有，那么就返回空值，没有找到

        //1.查看本地日志文件中有没有数据，记录的是当前进程的缓存
        byte[] values = cacheInfo.getOrDefault(key, null);
        //本地日志中找到了,返回值
        if (values != null) return decodeBuffer(values);

        //获取当前的kvpod数量
        int num = KvStoreConfig.getServersNum();

        byte[] res = null;
        ////如果当前进程中没有找到，那么去找从hdfs中读取的完整索引
        for (String id : indexTable.keySet()) {
            int keyLen = Integer.parseInt(id.split("_")[0]);
            // 因为索引中的索引数据是按照key的长度区分写的，所以如果当前的key的长度不等于要找的关键字的长度，就可以跳过了
            //数据肯定不在这个文件中
            if (keyLen != key.length()) continue;

            Map<String, byte[]> vals = indexTable.get(id);

            //特殊情况
            if (vals == null) continue;

            //去判断索引的内容
            if ((res = vals.getOrDefault(key, null)) != null) {
                //文件中的偏移量
                long offset = decodeOffset(res);
                int _len = decodeComplete(res, 8);
                String datafile = processData.get(id);
                try {
                    values = MyFile.readByOffset(datafile, offset, _len);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return decodeBuffer(values);
            }


        }


//        for(int i = 0 ; i < num ; i++){
//            String id = key.length() + "_" + i;
//
//            byte[] data = null;
//
//            HashMap<String,byte[]> vals = indexTable.get(id);
//
//            if (vals == null) continue;//这是一种比较特殊的情况
//
//            if((data = vals.getOrDefault(key,null)) != null){
//                long offset = decodeOffset(data);
//                int _len = decodeComplete(data,8);
//                String datafile = processData.get(id);
//                try {
//                    values = MyFile.readByOffset(datafile, offset, _len);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//                return decodeBuffer(values);
//            }
//        }

        //没有的话查找其他进程中还没有落盘的日志文件中是否有该key
        for (int i = 0; i < num; ++i) {
            if (RpcServer.getRpcServerId() != i) {
                try {
                    byte[] result = RpcClientFactory.inform(i, key.getBytes());
                    if (result.length != 0) return decodeBuffer(result);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        }

        //没有找到该索引
        return null;
    }

    //
    @Override
    public synchronized boolean put(String key, Map<String, String> value) {


        //放到随机的一个log文件中，默认是每个kv分10个log文件
        //int num  = (int)(Math.random()*10);

        //重新选择日志的写入方式，整个kvstore的解码基于keylength
        //log的文件名为address/kvpodid_keylen_count
        String logid = RpcServer.getRpcServerId() + "_" + key.length();

        //先去查找现在我要写入的日志文件已经存在了没
        MyLog thislog = Mydata.logMap.getOrDefault(logid, null);

        //如果当前的日志文件中不存在,创建并且写入当前的日志索引
        if (thislog == null) {
            thislog = new MyLog(logid);
            Mydata.logMap.put(logid, thislog);
        }

        //写入到当前以keylen为键的日志文件当中
        //写入之前先进行编码
        byte[] encodeValue = encodeBuffer(value);
        byte[] encodeLog = encodeComplete(key, encodeValue);


        //写入数据之前必须判断现在的这个日志文件能不能写入文件，如果不能写的话，就需要进行日志文件的切换
        //为每一个日志文件建立了一张索引表
        MyTable thistable = getCurrentTable(key.length());

        if (!thistable.isFull(encodeValue)) {
            thislog.writeLog(encodeLog);
            thistable.put(key.getBytes(), encodeValue);
        } else {
            thistable.persist();
            thislog.changeLog();
            thislog.writeLog(encodeLog);
            thislog.delLog();
            thistable.put(key.getBytes(), encodeValue);
        }
        return true;
    }

    @Override
    public synchronized boolean batchPut(Map<String, Map<String, String>> records) {
        //直接批量调用put,效率并不好
        for (Map.Entry<String, Map<String, String>> entry : records.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
        return true;
    }

    @Override
    public byte[] process(byte[] input) {
        String key = new String(input);
        byte[] res = cacheInfo.getOrDefault(key, new byte[0]);
        return res;
    }

    //
    @Override
    public int count(Map<String, String> map) {
        return 0;
    }

    @Override
    public Map<Map<String, String>, Integer> groupBy(List<String> list) {
        return null;
    }

    //编码
    //两种方案，1.为了开发方便，直接使用gson; 2.使用自己的编码转化成字节流
    //
    public String encodeGson(Map<String, String> value) {
        Gson gson = new Gson();
        String ans = gson.toJson(value);
        return ans;
    }

    ///tested
    ////编码 key的length和val的len先put进去，然后再跟其内容
    public byte[] encodeBuffer(Map<String, String> map) {
        //limit=capacity position=0
        //buffer.clear();
        //开始写入,因为每个消息的长度最多200B 所以以一个字节做长度索引，往缓存中存取数据
        buffer.clear();
        Set<String> keys = map.keySet();
        for (String k : keys) {
            byte[] key_bytes = k.getBytes();
            byte[] value_bytes = map.get(k).getBytes();
            byte key_len = (byte) key_bytes.length;
            byte value_len = (byte) value_bytes.length;
            buffer.put(key_len);
            buffer.put(value_len);
            buffer.put(key_bytes);
            buffer.put(value_bytes);
        }
        buffer.flip();
        int len = buffer.limit();
        byte[] contents = new byte[len];
        buffer.get(contents, 0, len);
        return contents;
    }

    public byte[] encodeComplete(String key, byte[] values) {
        buffer.clear();
//        int len =  key.length()+values.length;
//        int key_len = key.length();
        int len = key.length() + values.length;
        int key_len = key.length();
        buffer.putInt(len);
        buffer.putInt(key_len);
        buffer.put(key.getBytes());
        buffer.put(values);
        buffer.flip();
        int length = buffer.limit();
        byte[] results = new byte[length];
        buffer.get(results, 0, length);
        return results;
    }

    //解码
    //对应编码的两种方案 1.gson 2.解码
    public Map<String, String> decodeGson(String val) {
        Map<String, String> map = new HashMap<>();
        Gson gson = new Gson();
        map = gson.fromJson(val, HashMap.class);
        return map;
    }


    //tested
    private synchronized Map<String, String> decodeBuffer(byte[] content) {
//        Map<String, String> maps = gson.fromJson(new String(content), HashMap.class);
        Map<String, String> maps = new HashMap<>();
        buffer.clear();
        buffer.put(content);
        buffer.flip();
        int len = buffer.limit();
        int index = 0;
        while (index < len) {
            int key_len = buffer.get() & 0XFF;
            int value_len = buffer.get() & 0XFF;
//            int key_len = encodeBuffer.getInt();
//            int value_len = encodeBuffer.getInt();
            byte[] key_bytes = new byte[key_len];
            byte[] value_bytes = new byte[value_len];
            buffer.get(key_bytes);
            buffer.get(value_bytes);
            String key = new String(key_bytes);
            String value = new String(value_bytes);
            maps.put(key, value);
            index = index + 2 + key_len + value_len;
        }
        return maps;
    }

    public int decodeComplete(byte[] val, int index) {
        int off_0 = val[index] & 0xFF;
        int off_1 = val[index + 1] & 0xFF;
        int off_2 = val[index + 2] & 0xFF;
        int off_3 = val[index + 3] & 0xFF;
        return (off_0 << 24) | (off_1 << 16) | (off_2 << 8) | off_3;
    }

    public long decodeOffset(byte[] bytes) {

        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.put(bytes, 0, 8);
        buffer.flip();//need flip
        return buffer.getLong();
    }

    ////
    private synchronized MyTable getCurrentTable(int key) {

        MyTable table = first.getOrDefault(key, null);

        if (table == null) {
            table = new MyTable(key);
            first.put(key, table);
            return table;
        }
        return null;
    }
}
