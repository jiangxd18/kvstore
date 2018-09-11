import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

public class MyLocalFile {
    ///////////对本地文件的操作，主要是针对100M的localdisk

    static {
        mkdirFile(Mydata.logAddress);
    }

    //打开文件,如果不存在的话就新建
    public static void mkdirFile(String address){
        File file = new File(address);
        if(!file.exists()) file.mkdir();
    }

    //读取
    public static byte[] readFile(String address){
        File file = new File(address);

        if(!file.exists()) return null;
        Long len = file.length();
        //读取数据
        byte[] result = new byte[len.intValue()];
        try{
            FileInputStream in = new FileInputStream(file);
            in.read(result);
            in.close();
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return result;
    }

    //列出所有的文件

    public static List<String> listAll(String name){
        File f = new File(name);
        List<String> lists = new ArrayList<>();
        if(!f.exists()) return lists;
        File[] files = f.listFiles();
        for(File file :files) lists.add(file.getPath());
        return lists;
    }

    //删除给定地址的文件
    public static void delFile(String address) throws Exception{
        File file = new File(address);
        if(file.exists())
            file.delete();
    }
}
