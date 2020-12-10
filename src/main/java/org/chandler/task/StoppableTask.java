package apps.smartfwd.src.main.java.org.chandler.task;

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public  abstract class StoppableTask extends Task {
    AtomicBoolean isRunning;
    boolean stopRequested=false;
    Future<?> handle;

    public void start(){
        if(null==service){
            worker=new Thread(this);
            worker.start();
        }else{
            handle=service.submit(this);
        }
    }
    public void stop(){
        if(null==service){
            worker.interrupt();
        }else{
            handle.cancel(true);
        }
    }
    public boolean isRunning(){
        return isRunning.get();
    }


}
