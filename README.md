# 项目背景

会有多个Java进程分布在不同的机器上，多个进程协同合作，使用hdfs作为底端的存储，从而完成整个kv-store的功能。是一个一次写多次读的场景。在写入的过程当中会出现进程重启，进程间通信异常的情况。

# 主要实现需求

| 实现需求 | 描述               | 接口方法                                                    |
| -------- | ------------------ | ----------------------------------------------------------- |
| get      | 查找数据           | Map<String,String> get(String key);                         |
| put      | 存储数据，建立索引 | boolean put(String key, Map<String, String> value);         |
| batchPut | 批量存储           | boolean batchPut(Map<String, Map<String, String>> records); |
| process  | 进程间的通信       | byte[] process(byte[] input);                               |

## 数据的存储（put）

注意：每条Key的长度不会超过20字节，长度是不固定的。（所以如果是1亿条数据是根本不可能把所有数都放在内存当中的）。

**更新**：每条key是字符串类型的，字符中只可能是0~9的数字，在进行分桶的时候先转换成long来做

### 数据的均匀散列（一级索引）

将传进来的String类型的Key先转换成long类型并进行干扰，防止key是周期性波动的数据，只被分到某几个桶影响最后的查找效率。桶的数量是每台机器上64个，程序分布在3台不同的机器上运行。

**针对整型的key**

因为有很多的key，一级索引是根据key来进行散列的，要求尽可能的均匀。这边借鉴了hashMap中的“扰动函数”的思想，即高16位和低16位进行异或（这边因为是Long类型，）。

为什么要用扰动函数呢？这是因为直接hashCode返回的是一个int类型的值，int类型所有加起来大概有40亿的数，所以在分桶的时候不可能分这么多。最后在根据hashCode进行分桶的时候也是用位操作去代替了模操作（但桶的个数必须是2的幂次）来提升效率，所以只有低几位对分桶才是有作用的，为了防止进来的Key的低几位出现规律性波动（或者说高位不一样，相差很大的Key被分到同一个桶）而被分到一个桶，影响最后二级索引文件的大小和查找效率，所以对其使用了干扰函数。混合了原始哈希码的高位和低位，以此加大了低位的随机性。

```java
//hash函数
//干扰函数
public int hashCode(long value){
    return value ^ (value >>> 32);
}
```

```java
//得到分桶的下标
//相当于是hash % length，但只有length为2的幂次时才成立
public int getIndex(int hash, int length){
    return hash & (length - 1);
}
```

**一级索引的结果**

对于每一台机器来说，使用上述算法散列，监测结果，基本上来说都达到了key在每个桶的均匀散列。在最好的情况下，如果key是比较均匀地put到三台机器上的，那么对于1亿条数据来说，每台机器差不多分到了3500w条数据，这3500w条数据平均地分到了每个桶，那么每个桶的大小如果都算key是20字节，那么在只算key的情况下，每个桶的大小大概在10MB（当然实际肯定比这个要大），是可以接受的。



## 缓存机制（redis）

对缓存来说最重要的就是缓存的命中率。

**读取**：redis作为缓存时的一些配置

```java
//设置缓存的最大值
//每条key的长度是10个字节（准确来讲,因为现在的key是可以转换成Long类型的,所以在redis中会转换成long类型），1MB大概能存102条数据,2GB大概能存22万条数据
maxmemory 2048mb
//allkeys-lru和volatile-lru的区别在于volatile-lru是不会删除没有设置expire的键的
maxmemory-policy allkeys-lru
```

缓存命中率使用查看

```java
//连接到redis
telnet localhost 6379
//使用info
info
//注意里面的#Stats
//有keyspace_hits和keyspace_misses,缓存的命中率为
//keyspace_hits/(keyspace_hits+keyspace_misses)
```

**缓存处理的过程**：每次读数据的时候先从缓存读，如果发现没有，再从二级索引中去读，然后将数据放入缓存当中。

**缓存执行的效率**：在一亿条数据下，缓存的效率保持在70%左右

## 进程间的通信方式（process）

process()方法，当进程收到其他进程传来的消息时调用的方法，用来完成进程间的通信。输入输出都是byte[]。进程间的通信主要是用在写操作之后，当读取Key的时候，数据可能还没有落盘到hdfs，并且是其他进程put的，那么当前的服务器是找不到的，需要向其他服务器询问是否有该数据。  

具体的[进程间的通信方式](https://github.com/jiangxd18/kvstore/blob/master/desc/communicate.md)有这些

**最初的实现**：项目中最初采用的方式是利用NIO中的socketChannel和socketServerChannel实现通信

**最后落实**：调用的是封装好的RPC服务，暴露在外面的接口是process，只要在接口里实现自己的逻辑就可以了。

下面是部分代码逻辑

```java
//获取当前的kvpod数量,KvStoreConfig是封装类
int num = KvStoreConfig.getServersNum();
for (int i = 0; i < num; ++i) {
    //获取当前的进程id,进程的标号默认是0、1、2、3....
    //不是当前的进程的话就进行询问
	if (RpcServer.getRpcServerId() != i) {
		try {
            //inform的第一个参数进程号
            //第二个参数是要传送的信息,这个信息会被丢到进程的process进行处理
			byte[] result = RpcClientFactory.inform(i, key.getBytes());
            //如果其他进程未落盘的缓存中有相应的数据就进行返回处理
            //如果通信出现异常,会报IO异常,需要自己处理
			if (result.length != 0) return decodeBuffer(result);
		} catch (IOException e) {
			//异常逻辑处理
		}
	}
}
```

### 进程间通信出现异常

解决方案：询问的进程Thread.sleep(3000)后继续尝试发出请求，如果轮询5次后仍然抛出异常，那么放弃询问，将异常抛出。

## HDFS架构





# 性能测试

| TestCase | 数据量      | 异常         | 测试                  |
| -------- | ----------- | ------------ | --------------------- |
| 1        | 1万条数据   |              | 正确读写              |
| 2        | 1万条数据   | 节点通信异常 | 正确读写,正确处理异常 |
| 3        | 1万条数据   | 节点重启     | 正确读写              |
| 4        | 100万条数据 |              | 正确读写              |
| 5        | 1亿条数据   |              | 正确读写              |

要求是在两个小时内完成所有的case测试，时间一到，会终止所有的进程，实际完成的时间是97分钟。