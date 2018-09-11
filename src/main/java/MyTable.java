import cn.helium.kvstore.rpc.RpcServer;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.Future;

public class MyTable {



    private int length = 0;
    private ByteBuffer block  = ByteBuffer.allocate(Mydata.MAX_BLOCK_SIZE);

    //索引值,记录key,value的长度
    private ByteBuffer index = ByteBuffer.allocate(Mydata.MAX_BLOCK_SIZE * 2);

    //记录当前的位移量
    private long currentOffset = 0L;

    //
    private int currentDataLen = 0;

    //空闲状态
    private volatile MyState state = MyState.free;


    //同步等待
    private Future<Boolean> task;

    //keylength
    public MyTable(int length){
        this.length = length;
        //没有找到文件的话记录为0
        //现在日志文件已经写到哪里了放在一张offset的表中
        currentOffset = Mydata.offset.getOrDefault(length, 0L);
    }
    public boolean put(byte[] key, byte[] val){
        //无法写入
        if(val.length + currentDataLen > Mydata.MAX_BLOCK_SIZE) {
            //满了的话要进行落盘
            this.persist();
            return false;
        }
        else{
            block.put(val);
            currentDataLen += val.length;
            //设置索引记录各个字段的长度,8表示64位 为long类型,用于记录currentOffset,4用来记录val的长度
            //总长度
            index.put((byte)(key.length + 8 + 4));
            //该条数据在文件中现在写入的偏移量
            index.putLong(currentOffset);
            ///键
            index.put(key);
            //数据的长度
            index.putInt(val.length);

            //更新当前的偏移
            currentOffset += val.length;
            return true;
        }
    }

    //持久化存储
    public void persist(){
        //开始准备
        this.state = MyState.in;
        Mydata.offset.put(length, currentOffset);

        //去得到索引
        index.flip();
        byte[] indexContent = new byte[index.limit()];
        index.get(indexContent,0,indexContent.length);

        //得到数据
        block.flip();
        byte[] valueContent = new byte[block.limit()];
        block.get(valueContent,0,block.limit());

        this.putData(length, valueContent, indexContent);


        this.state = MyState.free;
        this.clear();
    }

    public void clear(){
        this.index.clear();
        this.block.clear();
        this.currentDataLen = 0;
    }

    public void putData(int len, byte[] valContents, byte[] indexContents){

        //去找现在这个文件路径是不是存在，如果不存在的话就新建

        String fileIndex = Mydata.indexFiles.getOrDefault(len, null);
        String fileData = null;

        if(fileIndex == null){
            fileIndex = Mydata.logAddress + "/" + "index_" + len + "_" + RpcServer.getRpcServerId();
            fileData = Mydata.logAddress + "/" + "data_" + len + "_" + RpcServer.getRpcServerId();

            Mydata.indexFiles.put(len, fileIndex);
            Mydata.dataFiles.put(len, fileData);
        }

        try{

            //落盘
            //持久化到hdfs
            MyFile.append(fileData, valContents, valContents.length);
            MyFile.append(fileIndex, indexContents,indexContents.length);

        }
        catch (Exception e){
            e.printStackTrace();
        }
    }


    //判断一个文件是否被写满
    public boolean isFull(byte[] val){
        if(val.length + currentDataLen > Mydata.MAX_BLOCK_SIZE) return true;
        return false;
    }

    public MyState getTableState(){
        return state;
    }

    public Future<Boolean> getTask(){
        return task;
    }

}
