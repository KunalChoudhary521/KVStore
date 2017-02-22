package app_kvEcs;

public class sshMain
{
    public static void main(String[] args)
    {
        sshSession mySsh = new sshSession();
        String user = "", host = "ug222.eecg.toronto.edu";//128.100.13.<>
        int port = 22;

        mySsh.connectSsh(user,host,port);

        String[] serverCmds = {
                "java -jar ms1-server.jar 8000 100 LRU ../TestFile.txt true",
                "java -jar ms1-server.jar 8001 100 FIFO ../TestFile.txt true",
                "java -jar ms1-server.jar 8002 100 LFU ../TestFile.txt true"
        };
        String directory = "cd ece419/m1/BasicStorageServer-stub";

        mySsh.runServer(directory, serverCmds);

        //mySsh.stopAllServer();

        mySsh.session.disconnect();//can be moved to destructor
    }

}