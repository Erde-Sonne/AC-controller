package apps.smartfwd.src.main.java.task;

import apps.smartfwd.src.main.java.task.base.AbstractStoppableTask;
import apps.smartfwd.src.main.java.utils.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class SocketClientTask extends AbstractStoppableTask {
    public interface ResponseHandler {
        void handle(String response);
    }
    private final Logger logger = LoggerFactory.getLogger(getClass());
    String payload;
    ResponseHandler responseHandler;
    String ip;
    int port;
    public SocketClientTask(String payload, ResponseHandler responseHandler, String ip, int port){
        this.payload=payload;
        this.responseHandler = responseHandler;
        this.ip=ip;
        this.port=port;

    }
    @Override
    public void run() {
        try {
            SocketChannel socketChannel = SocketChannel.open();
            //for docker test only
            //todo delete this
            InetAddress address = InetAddress.getByName(this.ip);
            socketChannel.connect(new InetSocketAddress(this.ip,this.port));
            ByteBuffer byteBuffer = ByteBuffer.allocate(512 * 1024);
            byteBuffer.put(this.payload.getBytes());
            byteBuffer.flip();
            socketChannel.write(byteBuffer);
            while (byteBuffer.hasRemaining()) {
                socketChannel.write(byteBuffer);
            }
            byteBuffer.clear();
            //接收数据
            int len = 0;
            StringBuilder stringBuilder = new StringBuilder();
            while ((len = socketChannel.read(byteBuffer)) >= 0) {
                byteBuffer.flip();
                String res = new String(byteBuffer.array(), 0, len);
                byteBuffer.clear();
                stringBuilder.append(res);
            }
            String payload = stringBuilder.toString();
            socketChannel.close();
            responseHandler.handle(payload);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 发送消息到kafka中
     * @param topic
     * @param data
     */
/*    public static void sendToKafka(String topic, String data) {
        ProducerRecord<String , String> record = new ProducerRecord<String, String>(topic, data);
        //发送消息
        Producer.producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (null != e){
                    e.printStackTrace();
                }
            }
        });
    }*/

    /**
     * 向指定 URL 发送POST方法的请求
     *
     * @param url
     *            发送请求的 URL
     * @param param
     *            请求参数，请求参数应该是 name1=value1&name2=value2 的形式。
     * @return 所代表远程资源的响应结果
     */
    public static String sendPost(String url, String param) {
        PrintWriter out = null;
        BufferedReader in = null;
        String result = "";
        try {
            URL realUrl = new URL(url);
            // 打开和URL之间的连接
            URLConnection conn = realUrl.openConnection();
            // 设置通用的请求属性
            conn.setRequestProperty("accept", "*/*");
            conn.setRequestProperty("connection", "Keep-Alive");
            conn.setRequestProperty("user-agent",
                    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            // 发送POST请求必须设置如下两行
            conn.setDoOutput(true);
            conn.setDoInput(true);
            // 获取URLConnection对象对应的输出流
            out = new PrintWriter(conn.getOutputStream());
            // 发送请求参数
            out.print(param);
            // flush输出流的缓冲
            out.flush();
            // 定义BufferedReader输入流来读取URL的响应
            in = new BufferedReader(
                    new InputStreamReader(conn.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {
                result += line;
            }
        } catch (Exception e) {
            System.out.println("发送 POST 请求出现异常！"+e);
            e.printStackTrace();
        }
        //使用finally块来关闭输出流、输入流
        finally{
            try{
                if(out!=null){
                    out.close();
                }
                if(in!=null){
                    in.close();
                }
            }
            catch(IOException ex){
                ex.printStackTrace();
            }
        }
        return result;
    }
}
