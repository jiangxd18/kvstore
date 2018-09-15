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

注意：每条Key的长度不会超过20字节，长度是不固定的。（所以如果是1亿条数据是根本不可能把所有数都放在内存当中的）。Value的内容不会超过10kb。

**更新**：每条key是字符串类型的，字符中只可能是0~9的数字，在进行分桶的时候先转换成long来做

### 数据的均匀散列（一级索引）

将传进来的String类型的Key先转换成long类型并进行干扰，防止key是周期性波动的数据，只被分到某几个桶影响最后的查找效率。桶的数量是每台机器上**32**个，程序分布在3台不同的机器上运行。

**针对整型的key**

因为有很多的key，一级索引是根据key来进行散列的，要求尽可能的均匀。这边借鉴了hashMap中的“扰动函数”的思想，即高16位和低16位进行异或（这边因为是Long类型，所以异或上高32位和低32位）。

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

对于每一台机器来说，使用上述算法散列，监测结果，基本上来说都达到了key在每个桶的均匀散列。在最好的情况下，如果key是比较均匀地put到三台机器上的，那么对于1亿条数据来说，每台机器差不多分到了3500w条数据，这3500w条数据平均地分到了每个桶（每个桶会有110w条），那么每个桶的大小如果都算key是20字节，那么在只算key的情况下，每个桶的大小大概在50MB左右（因为每一条要放两条Key，一个是1000条数据内开始的大小，另一个是1000条数据内截止的大小），是可以接受的。

### 二级索引的建立

#### 方案一（简单但是不高效）

利用redis中的sorted set。**sorted set的key名字为当前进程名_桶号**，每次数据来的时候，先放到相应的sorted set中，以key为sorted set中的score，value为sorted set中的value。当放入之后，使用jedis操作的zcard key返回当前的桶中的元素个数，如果已经是大于等于1000个了，那么进行持久化并建立二级索引操作。

这边使用redis只是为了方便，其实使用TreeMap这些也是一样的。

```java
//jedisTemplate(如果没有被spring管理直接使用jedisPool获取连接操作即可)
//jedisTemplate是spring提供的方法,项目里直接使用jedis

//每次放入key，value
jedis.zadd("Thread1_01", key * 1.0, value);
//去检测桶的大小
long size = jedis.zcard("Thread_01");
if(size >= 0){
    Set<Tuple> mySet = jedis.zrangeWithScores("Thread_01", 0, -1);
    for(Tuple tuple: mySet){
        System.out.println(tuple.getScore() + ":" + tuple.getElement());
    }
}
//1. 建立索引 2. 落盘到hdfs
```

**索引的建立**

根据sorted set排好序的键值，简历索引文件和数据文件，索引文件每个桶只有一份即index_Thread1_01，记录一千条数据的key_start和key_end（之后查找会根据要查找的键是否在这个范围内判断是否需要读取这一块）

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

HDFS采用了Master/Slave体系结构构建分布式系统。Master运行namenode主进程，是不存储实际数据的，只存储元数据（文件块的位置、大小、拥有者信息等），datanode的大小为64MB（默认大小，可以调整）用来存储实际数据。

HDFS中文件的存储方式是将文件按block划分，如果一个文件超过了一个block，那么会被切分成多个block，并存储在不同的dataNode上，一个block只会存储一个文件，也就是说如果block默认的大小是64MB，如果文件大小只有10MB写入这个block，虽然文件的实际空间占有还是只有10MB，但是这个block不能再写其他文件。

## 性能优化

1. 如果每次写入的时候是写入1000条，每条因为Key不超过20B，value不超过10KB，所以大致写入的时候是10MB，读的时候也是10MB读取。一个block的大小是64MB，这样会存在很多小文件，对namenode带去压力，如何解决？

   假设我配置减少64MB默认block的大小。那么会带来两个问题：第一是寻道的时间变长，因为数据块在硬盘上是非连续存储的，需要去移动磁头定位，文件块一小，如果读取的数据量变大，就要读很多的块，寻道时间大大增加；第二是namenode通常只有一个，其内存FSImage文件中记录了datanode中数据块的信息，如果小文件一多，那么记录的元数据就会变多，如果像上面的情况1000条写一个block，那么在一亿的条件下会产生10w条元数据，如果数据量再大，会产生更多的元数据，namenode会受不了。


# 性能测试

| TestCase | 数据量      | 异常         | 测试                  |
| -------- | ----------- | ------------ | --------------------- |
| 1        | 10万条数据  |              | 正确读写              |
| 2        | 10万条数据  | 节点通信异常 | 正确读写,正确处理异常 |
| 3        | 10万条数据  | 节点重启     | 正确读写              |
| 4        | 100万条数据 |              | 正确读写              |
| 5        | 1亿条数据   |              | 正确读写              |

要求是在两个小时内完成所有的case测试，时间一到，会终止所有的进程，实际完成的时间是97分钟。