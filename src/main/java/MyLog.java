import java.io.File;
import java.io.RandomAccessFile;

//本地日志，用来存放临时的数据，也就是没有落盘的数据
public class MyLog {
    //利用randomAccessFile类进行读写
    private RandomAccessFile file;
    //当前写日志的地址
    private String url = Mydata.logAddress;
    //需要删除的日志地址
    private String delUrl = "";
    //日志编号
    private int count = 0;

    private String id = "";
    private MyLocalFile myFile = new MyLocalFile();
    private File fs;

    public MyLog(String logid){
        //初始化
        count = 0;
        id = logid;
        //创建打开文件
        url += "/log_" + logid + "_" + count;

        //创建randomAccessFile的对象
        try{
            file = new RandomAccessFile(url, "rw");
        }
        catch (Exception e){
            e.printStackTrace();
        }

    }

    //向日志文件中写数据
    public void writeLog(byte[] log){
        try{
            file.write(log);
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
    //删除日志文件
    public void delLog(){
        try{
            MyLocalFile.delFile(delUrl);
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
    //
    public void changeLog(){
        try{
            delUrl = url;
            url = Mydata.logAddress + "/log_" + id + "_" + (++count);
            //关闭文件
            file.close();
            //打开新的文件
            file = new RandomAccessFile(url, "rw");
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

}
