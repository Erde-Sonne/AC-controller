package apps.smartfwd.src.main.java.org.chandler.task;

import java.util.concurrent.ExecutorService;

public abstract class Task implements Runnable {
    ExecutorService service;
    Thread worker;
    public void start(){
        if(null==service){
            worker=new Thread(this);
            worker.start();
        }else{
            service.submit(this);
        }
    }
}
