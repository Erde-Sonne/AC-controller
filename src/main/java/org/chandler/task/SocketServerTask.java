package apps.smartfwd.src.main.java.org.chandler.task;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

public class SocketServerTask extends Task {
    public interface Handler{
        void handle(String payload);
    }
    Handler handler;
    int port;
    public SocketServerTask(int port, Handler handler){
        this.port=port;
        this.handler=handler;
    }
    @Override
    public void run() {
        ServerSocketChannel serverSocketChannel = null;
        Selector selector = null;
        ByteBuffer buffer = ByteBuffer.allocate(2048);
        try {
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.bind(new InetSocketAddress("0.0.0.0", this.port));
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
                        String payload = stringBuilder.toString();
                        handler.handle(payload);
                        channel.close();
                    }
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
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
