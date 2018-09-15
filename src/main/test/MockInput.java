import org.junit.Before;
import org.junit.Test;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;

public class MockInput {

    private TreeMap<Long, String> treeMap = null;
    private FileChannel fileChannel = null;
    private RandomAccessFile randomAccessFile = null;
    private ReentrantLock lock = null;
    private ExecutorService service = null;
    private List<Future<String>> list = null;

    @Before
    public void setUp(){
        //基于红黑树实现
        treeMap = new TreeMap<>();
        try{
            randomAccessFile = new RandomAccessFile("nio.txt", "rw");
            fileChannel = randomAccessFile.getChannel();
        }catch (Exception ex){
            ex.printStackTrace();
        }
        //设置同步锁
        lock = new ReentrantLock();
        service = Executors.newFixedThreadPool(5);
        list = new ArrayList<>();
    }

    @Test
    public void testInput() throws Exception{
        for(int i = 0; i < 1000; i++){

            long key = (long)(Math.random()*1000);
            treeMap.put(key, "just test" + i);
            if(treeMap.size() == 10){
                LinkedHashMap<Long, String> hashMap = new LinkedHashMap<>(treeMap);
                Iterator<Map.Entry<Long,String>> it = treeMap.entrySet().iterator();
                while(it.hasNext()){
                    Map.Entry<Long, String> entry = it.next();
                    hashMap.put(entry.getKey(), entry.getValue());
                }
                Future<String> future = service.submit(new MyInput(lock, randomAccessFile,
                        hashMap, fileChannel));
                list.add(future);
                treeMap.clear();
            }
        }

        for(int i = 0; i < list.size(); i++){
            Future<String> future = list.get(i);
            System.out.println(future.get());
        }
        service.shutdown();
    }


}
class MyInput implements Callable<String> {

    private ReentrantLock lock = null;
    private RandomAccessFile accessFile = null;
    private LinkedHashMap<Long,String> hashMap = null;
    private FileChannel fileChannel = null;

    public MyInput(ReentrantLock lock, RandomAccessFile accessFile,
                   LinkedHashMap<Long,String> hashMap, FileChannel fileChannel){
        this.lock = lock;
        this.accessFile = accessFile;
        this.hashMap = hashMap;
        this.fileChannel = fileChannel;
    }

    @Override
    public String call() throws Exception {

        ByteBuffer buffer = ByteBuffer.allocate(11240);

        Iterator<Map.Entry<Long,String>> it = hashMap.entrySet().iterator();
        buffer.clear();
        while(it.hasNext()){

            Map.Entry<Long,String> entry = it.next();
            Long key = entry.getKey();
            String value = entry.getValue();

            byte keyLen = (byte)String.valueOf(key).getBytes().length;
            short valueLen = (short)value.getBytes().length;

//            buffer.put(keyLen);
//            buffer.putShort(valueLen);

            buffer.put(String.valueOf(key).getBytes());
            buffer.put(value.getBytes());

        }

        buffer.flip();
        try{

            lock.lock();
            long size = fileChannel.size();
            fileChannel.position(size);

            while(buffer.hasRemaining()){
                fileChannel.write(buffer);
            }

            fileChannel.force(true);

        }catch (Exception ex){
            ex.printStackTrace();
            return "failure";
        }finally {
            //尽量把unlock放在finally里
            lock.unlock();
        }

        return "success";
    }
}
