# 进程间的通信方式

### 匿名管道

管道是一种半双工的通信方式，数据只能单向流动，而且进程间需要有亲缘关系，比如说父子进程。

实现方式：

* 父进程创建管道，得到两个文件描述符指向管道的两端
* 父进程fork出子进程，子进程也有两个文件描述符指向同一个管道
* 父进程关闭读端fd[0]，子进程关闭写端fd[1]，这样父进程就可以往管道里写，子进程就可以读，管道本质上是个环形队列，数据从写端流入，从读端流出，就实现了进程间的通信

### 有名管道

半双工，允许没有亲缘关系的进程间通信

### 信号

像Object中的wait()、notify()、notifyAll()方法，Condition中的await()、signal()、signalAll()，用于通知某个事件已经发生了。

### 信号量

Semaphore，是一个计数器，作为不同线程/进程间的同步手段

```java
Semaphore sp = new Semaphore(5);
//获取一个许可
sp.acquire();
//释放一个许可
sp.releas();
```

### 共享内存

Java进程间的共享内存是通过NIO的内存映射文件实现的(MappedByteBuffer)，不同进程的内存映射文件关联到同一个物理文件，并保持内存和文件的双向同步

### 消息队列

rocketMQ

### 套接字

Java中socket和serverSocket  

还有NIO中的socketChannel和serverSocketChannel，并结合Selector使用



