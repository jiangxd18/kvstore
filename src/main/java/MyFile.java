import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class MyFile {

    //试着打开本地的文件
    static{
        try{
            mkdir(Mydata.logAddress);
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }


    /////////对hdfs的文件操作


    ///创建文件夹

    public static boolean mkdir(String dir) throws IOException {
        if (StringUtils.isBlank(dir)) {
            return false;
        }
        dir = Mydata.hdfsURL + dir;
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dir), conf);
        if (!fs.exists(new Path(dir))) {
            fs.mkdirs(new Path(dir));
            fs.close();
            return true;
        }else{
            fs.close();
            return false;
        }
    }

    //写文件到hdfs
    //localStr为你想要上传的本地文件的地址/../../.. hdfsSrc为你想要上传到的hdfs地址hdfs://localhost:9000/..

    public void writeHdfs(String localSrc, String hdfsSrc) throws Exception{
        Configuration conf = new Configuration();

        Path src = new Path(localSrc);
        Path dest = new Path(hdfsSrc);

        FileSystem fs = dest.getFileSystem(conf);

        //不存在则创建文件夹,如果文件存在的情况下是直接替换的
        if(!fs.exists(dest)){
            fs.mkdirs(dest);
        }

        fs.copyFromLocalFile(src, dest);
        fs.close();
    }

    //写数据到hdfs文件
    public void appendHdfs(String content, String hdfsSrc) throws Exception{
        Configuration conf = new Configuration();
        Path dest = new Path(hdfsSrc);

        FileSystem fs = dest.getFileSystem(conf);

        InputStream in = new ByteArrayInputStream(content.getBytes());
        FSDataOutputStream out = fs.append(dest);
        IOUtils.copyBytes(in, out, 4096, true);
    }


    //列出现在hdfs中所有的文件
    public static List<String> listAll(String dir) throws IOException {
        if (StringUtils.isBlank(dir)) {
            return new ArrayList<String>();
        }
        dir = Mydata.hdfsURL + dir;
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dir), conf);
        FileStatus[] stats = fs.listStatus(new Path(dir));

        List<String> names = new ArrayList<String>();

        for(int i = 0; i < stats.length; ++i) {
            if (stats[i].isFile()) {
                // regular file
                names.add(stats[i].getPath().toString());
            } else if (stats[i].isDirectory()) {
                // dir
                names.add(stats[i].getPath().toString());
            } else if (stats[i].isSymlink()) {
                // is s symlink in linux
                names.add(stats[i].getPath().toString());
            }
        }

        fs.close();
        return names;
    }

    //从hdfs读取数据
    public void readHdfs(String hdfsSrc) throws Exception{
        Configuration conf = new Configuration();
        Path dest = new Path(hdfsSrc);
        FileSystem fs = dest.getFileSystem(conf);

        FSDataInputStream hdfsInStream = fs.open(dest);

        byte[] ioBuffer = new byte[4096];

        int readLen = hdfsInStream.read(ioBuffer);

        //有数据
        while(readLen != -1){
            //直接输出到控制台
            System.out.write(ioBuffer, 0, readLen);
            readLen = hdfsInStream.read(ioBuffer);
        }
        hdfsInStream.close();
        fs.close();
    }

    //read return the val
    public static byte[] readHDFSFile(String hdfsFile) throws Exception {
        if (StringUtils.isBlank(hdfsFile)) {
            return null;
        }

        if(!hdfsFile.contains("hdfs")) {
            hdfsFile = Mydata.hdfsURL + hdfsFile;
        }

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(URI.create(hdfsFile), conf);
        // check if the file exists
        Path path = new Path(hdfsFile);
        if (fs.exists(path)) {
            FSDataInputStream is = fs.open(path);
            // get the file info to create the buffer
            FileStatus stat = fs.getFileStatus(path);
            // create the buffer
            byte[] buffer = new byte[Integer.parseInt(String.valueOf(stat.getLen()))];
            is.readFully(0, buffer);
            is.close();
            fs.close();

            return buffer;
        } else {
            throw new Exception("Not Found!");
        }
    }


    //创建文件夹

    public static boolean createNewHDFSFile(String newFile, byte[] content) throws IOException {
        if (StringUtils.isBlank(newFile) || null == content) {
            return false;
        }
        newFile = Mydata.hdfsURL + newFile;
        Configuration config = new Configuration();
        FileSystem hdfs = FileSystem.get(URI.create(newFile), config);
        FSDataOutputStream os = hdfs.create(new Path(newFile));
        os.write(content);
        os.close();
        hdfs.close();
        return true;
    }

    //向文件的末尾添加文件

    public static void append(String hdfsFile ,byte[] content,int contentLength) throws Exception {
        if (StringUtils.isBlank(hdfsFile)) {
            return ;
        }
        if(content == null || content.length==0){
            return ;
        }
        String _hdfsFile = Mydata.hdfsURL + hdfsFile;
        Configuration conf = new Configuration();
        // solve the problem when appending at single datanode hadoop env
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        FileSystem fs = FileSystem.get(URI.create(_hdfsFile), conf);
        // check if the file exists
        Path path = new Path(_hdfsFile);
        if (fs.exists(path)) {
            try {
//                InputStream in = new ByteArrayInputStream(content);
                OutputStream out = fs.append(new Path(_hdfsFile));
//                IOUtils.copyBytes(in, out, 4096, true);
                out.write(content,0,contentLength);
//                out.write(content,offset,content.length);
                out.close();
                fs.close();
//                in.close();
//                fs.close();
            } catch (Exception ex) {
                fs.close();
                throw ex;
            }
        } else {
            createNewHDFSFile(hdfsFile, content);
        }
    }

    ////seek
    public static byte[] readByOffset(String hdfsFile , long offset ,int len) throws Exception{
        if (StringUtils.isBlank(hdfsFile)) {
            return null;
        }
        if(!hdfsFile.contains("hdfs")) {
            hdfsFile = Mydata.hdfsURL + hdfsFile;
        }
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(hdfsFile), conf);
        // check if the file exists
        Path path = new Path(hdfsFile);
        byte[] contents = new byte[len];
        if (fs.exists(path)) {
            FSDataInputStream is = fs.open(path);
            is.skip(offset);
            is.read(contents);
        }
        return contents;
    }

}
