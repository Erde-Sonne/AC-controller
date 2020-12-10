package apps.smartfwd.src.main.java;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 请求分类的线程，将收到的数据发送，并接收分类信息，将信息存储到concurrentLinkedQueue中.
 */
public class ClassifyModuleThread implements Runnable {
    private final Logger log = LoggerFactory.getLogger(ClassifyModuleThread.class);
    private final String jsonString;
    private ConcurrentLinkedQueue<String> flowClq;
    public ClassifyModuleThread(String jsonString, ConcurrentLinkedQueue<String> flowClq) {
        this.jsonString = jsonString;
        this.flowClq = flowClq;
    }

    @Override
    public void run() {
        SocketChannel socketChannel = null;
        try {
            socketChannel = SocketChannel.open();
            socketChannel.connect(new InetSocketAddress("172.17.0.1", 1025));
            ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
            JsonNode jsonNode = new ObjectMapper().readTree(jsonString);
            JsonNode jsonPart1 = jsonNode.get("specifier");
            JsonNode stats = jsonNode.get("stats");
            String sendString = "{\"stats\":" + stats.toString() + "}" + "*";
            byteBuffer.put(sendString.getBytes());
            byteBuffer.flip();
            socketChannel.write(byteBuffer);
            byteBuffer.clear();
            int len = 0;
            StringBuilder stringBuilder = new StringBuilder();
            while ((len = socketChannel.read(byteBuffer)) >= 0) {
                byteBuffer.flip();
                String res = new String(byteBuffer.array(), 0, len);
                byteBuffer.clear();
                stringBuilder.append(res);
            }
            JsonNode resJson = new ObjectMapper().readTree(stringBuilder.toString());
            String res = resJson.get("res").toString();
            Iterator<JsonNode> iterator = jsonPart1.iterator();
            ArrayList<String> arrayList = new ArrayList<>();
            while (iterator.hasNext()) {
                String str = iterator.next().toString();
                arrayList.add(str.substring(1, str.length() - 1));
            }
            arrayList.add(res);
//            log.info(res);
            if (arrayList.size() == 6) {
                StringBuilder out = new StringBuilder();
                for (int i = 0; i < 5; i++) {
                    out.append(arrayList.get(i)).append("-");
                }
                out.append(arrayList.get(5));
                flowClq.offer(out.toString());
            }
            socketChannel.close();
        } catch (IOException e) {
            if (socketChannel != null) {
                try {
                    socketChannel.close();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
            e.printStackTrace();
            log.error("---!host upload info error!---");
        }
    }
}
