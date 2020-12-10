package apps.smartfwd.src.main.java.org.chandler.task;


public class PeriodicalSocketClientTask extends PeriodicalTask {
    public interface ResponseHandler {
        void handle(String payload);
    }
    public interface RequestGenerator {
        String payload();
    }
    String ip;
    int port;
    ResponseHandler handler;
    RequestGenerator payloadGenerator;
    public PeriodicalSocketClientTask(String ip, int port, RequestGenerator payloadGenerator, ResponseHandler handler){
        this.ip=ip;
        this.port=port;
        this.handler=handler;
        this.payloadGenerator=payloadGenerator;
        this.worker=()->{

        };
    }
}
