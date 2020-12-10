package apps.smartfwd.src.main.java;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 接收主机上传的5元组等信息，并且每当收到一组数据，都会开启一个线程去请求分类模块.
 */
public class HostModuleThread implements Runnable {
    private final Logger log = LoggerFactory.getLogger(ClassifyModuleThread.class);
    // 线程安全的队列，用于存储五元组和分类信息
    private ConcurrentLinkedQueue<String> flowClq;
    public HostModuleThread(ConcurrentLinkedQueue<String> flowClq) {
        this.flowClq = flowClq;
    }

    @Override
    public void run() {
        ServerSocketChannel serverSocketChannel = null;
        Selector selector = null;
        ExecutorService executorService = Executors.newCachedThreadPool();
//        ExecutorService executorService = Executors.newFixedThreadPool(500);
        ByteBuffer buffer = ByteBuffer.allocate(2048);
         try {
             serverSocketChannel = ServerSocketChannel.open();
             serverSocketChannel.bind(new InetSocketAddress("0.0.0.0", 1026));
             serverSocketChannel.configureBlocking(false);
             selector = Selector.open();
             serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

             while (selector.select() > 0) {
                 Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                 while (iterator.hasNext()) {
                     SelectionKey next = iterator.next();
                     iterator.remove();
                     if (next.isAcceptable()) {
                         SocketChannel accept = serverSocketChannel.accept();
                         accept.configureBlocking(false);
                         accept.register(selector, SelectionKey.OP_READ);
                     } else if (next.isReadable()) {
                         SocketChannel channel = (SocketChannel) next.channel();
                         int len = 0;
                         StringBuilder stringBuilder = new StringBuilder();
                         while ((len = channel.read(buffer)) >= 0) {
                             buffer.flip();
                             String res = new String(buffer.array(), 0, len);
                             buffer.clear();
                             stringBuilder.append(res);
                         }
                         String sss = stringBuilder.toString();
//                         log.info(sss);
                         // 实例化请求分类模块的线程
                         ClassifyModuleThread classifyModuleThread = new ClassifyModuleThread(sss, flowClq);
                         // 运行线程
                         executorService.submit(classifyModuleThread);
                         channel.close();
                     }
                 }

             }
         } catch (IOException e) {
             e.printStackTrace();
         } finally {
             // 释放相关的资源
             executorService.shutdown();
             if (selector != null) {
                 try {
                     selector.close();
                 } catch (IOException e) {
                     e.printStackTrace();
                 }
             }
             if (serverSocketChannel != null) {
                 try {
                     serverSocketChannel.close();
                 } catch (IOException e) {
                     e.printStackTrace();
                 }
             }
         }
    }
}
