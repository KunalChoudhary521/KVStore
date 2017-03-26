package app_kvServer;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.*;

/**
 * Created by yy on 2017-03-21.
 */
public class heartbeat implements Runnable {
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;
    private static Logger logger;
    public int port;
    public String host;
    OutputStream writeToSock;
    InputStream input;
    private Socket sock;
    private int rep_num;
    private KVServer server;

    public heartbeat(int rn, KVServer sv, Logger logger) {
      sock= null;
      writeToSock=null;
      input = null;
      this.rep_num=rn;
      this.server = sv;
        heartbeat.logger = logger;
        this.host = "";
        this.port = -1;
    }
    @Override
    public void run() { 
      logger.info("heartbeat: setting up the heartbeat socket");
    
      while (true) {
          try {
              Thread.sleep(10000);
              update_rep();
              Thread.sleep(1000);//possibly change
              if(this.server.ECSAddress != null){
                if(get_resp() == null){
                  //Send a message to ECS that this heartbeat failed
                  
                  logger.info("heartbeat: replica " + this.host + " , " + this.port + " is not responding. Notify ECS.");
                                  
                  InetSocketAddress ecs;
                  
                  ecs = this.server.ECSAddress;
                                    
                  logger.info("heartbeat: send message to " + ecs.getAddress() + " 8000");
                  
                  Socket ecsSock = new Socket(ecs.getAddress(), 8000);
                  
                  String message = "FAIL-"+this.host+"-"
                  +this.port; //address of the replica that failed
                  byte[] msg = message.getBytes();                  

                  ecsSock.getOutputStream().write(msg, 0, message.length());
                  
                  byte[] z = new byte[1];
                  z[0] = 0;
                  
                  ecsSock.getOutputStream().write(z, 0, 1);
                  ecsSock.getOutputStream().flush();                  
                  
                  ecsSock.getOutputStream().close();
                  ecsSock.close();
                }
              }
          }catch (Exception ex){
            logger.error("heartbeat: " + "run");
          }
      }
    }
    private String get_resp() throws IOException {
        int index = 0;
        byte[] msgBytes = null, tmp = null;
        byte[] bufferBytes = new byte[BUFFER_SIZE];
		if (input.available()==0){
		    return null;
        }
		/* read first char from stream */
        byte read = (byte) input.read();
        boolean reading = true;

        while(read != 0 && reading) {/* carriage return */
			/* if buffer filled, copy to msg array */
            if(index == BUFFER_SIZE) {
                if(msgBytes == null){
                    tmp = new byte[BUFFER_SIZE];
                    System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
                } else {
                    tmp = new byte[msgBytes.length + BUFFER_SIZE];
                    System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
                    System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
                            BUFFER_SIZE);
                }

                msgBytes = tmp;
                bufferBytes = new byte[BUFFER_SIZE];
                index = 0;
            }

			/* only read valid characters, i.e. letters and numbers */
            if((read > 0 && read < 127)) {
                bufferBytes[index] = read;
                index++;
            }

			/* stop reading is DROP_SIZE is reached */
            if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
                reading = false;
            }

			/* read next char from stream */
            read = (byte) input.read();
        }

        if(msgBytes == null){
            tmp = new byte[index];
            System.arraycopy(bufferBytes, 0, tmp, 0, index);
        } else {
            tmp = new byte[msgBytes.length + index];
            System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
            System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
        }

        msgBytes = tmp;

		/* build final String */
        TextMessage msg = new TextMessage(msgBytes);

            logger.info("Receive message:\t '" + msg.getMsg() + "'");

        return msg.getMsg();
    }
    private void update_rep(){
        this.server.to_update_with_lock[rep_num].lock();
        byte[] kvPairBArray;
        if (!this.server.to_update_with[rep_num].isEmpty()) {
            kvPairBArray = new byte[this.server.to_update_with[rep_num].length() + "KV-HeartBeat-".length() + 2];//+1 for stream termination
            System.arraycopy("KV-HeartBeat-".getBytes(), 0, kvPairBArray, 0, "KV-HeartBeat-".length());
            kvPairBArray["KV-HeartBeat-".length()] = 0;
            System.arraycopy(this.server.to_update_with[rep_num].getBytes(), 0, kvPairBArray, "KV-HeartBeat-".length() + 1, this.server.to_update_with[rep_num].length());
        }
        else{
            kvPairBArray = new byte["heartbeat".length()+1];
            System.arraycopy("heartbeat".getBytes(), 0, kvPairBArray, 0, "heartbeat".length());
        }
        logger.info("HB"+Integer.toString(rep_num)+" update message is ready to be sent!");
        try {
            writeToSock.write(kvPairBArray, 0, kvPairBArray.length);
            writeToSock.flush();
        }catch (Exception ex){
            logger.error("heartbeat: " + "update_rep");
        }
        this.server.to_update_with[rep_num] = "";
        this.server.to_update_with_lock[rep_num].unlock();
    }

    public void update_rep_info(String host, int port) {
        if (this.host.equals(host) && this.port == port) {
            return;
        }
        if (this.sock != null) {
          if(this.sock.isConnected()){
            try {
                this.sock.close();
                this.input.close();
                this.writeToSock.close();
            } catch (Exception ex) {
                logger.error("problems closing socket, line 174 of heartbeat.java");
            }
          }
        }        
               
        this.host = host;
        this.port = port;
                
        try {
            sock = new Socket(host, port);
            writeToSock = sock.getOutputStream();
            input = sock.getInputStream();
        } catch (Exception ex) {
            logger.error("problems openning socket, line 182 of heartbeat.java");
        }
      
        logger.info("heartbeat: " + this.host);
        logger.info("heartbeat: " + this.port);
    }
}
