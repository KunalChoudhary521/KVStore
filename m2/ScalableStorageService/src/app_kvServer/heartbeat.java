package app_kvServer;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

/**
 * Created by yy on 2017-03-21.
 */
public class heartbeat implements Runnable {
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;
    private static Logger logger = Logger.getRootLogger();
    public int port;
    public String host;
    OutputStream writeToSock;
    InputStream input;
    private Socket sock;
    private int rep_num;
    private KVServer server;
    public heartbeat(int rn, KVServer sv){
        port =0;
        host = "";
        sock= null;
        writeToSock=null;
        input = null;
        this.rep_num=rn;
        this.server = sv;
    }
    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(60000);
                update_rep();
                Thread.sleep(50);//possibly change
                if(!get_resp().equals("Ack")){
                    //Send a message to ECS that this heartbeat failed
                }
            }catch (Exception ex){

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
            logger.error(ex);
        }
        this.server.to_update_with[rep_num] = "";
        this.server.to_update_with_lock[rep_num].unlock();
    }
    public void update(int np, String nh){
        if (this.port != np || !this.host.equalsIgnoreCase(nh)){
            try {
                if (sock.isConnected()) {
                    sock.close();
                }
                this.port = np;
                this.host = nh;
                sock = new Socket(host, port);
                writeToSock =  sock.getOutputStream();
                input = sock.getInputStream();
            }catch(Exception ex){
                logger.error(ex);
            }
        }
    }
}