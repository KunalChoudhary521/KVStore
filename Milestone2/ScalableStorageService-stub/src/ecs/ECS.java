package ecs;

import common.Metadata;
import common.TextMessage;
import common.md5;
import common.messages.KVAdminMessage;
import common.messages.RespToECS;
import org.apache.log4j.Logger;

import java.io.*;
import java.math.BigInteger;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

//analogous to KVStore.java
public class ECS implements ECSCommInterface
{

    private Logger logger = Logger.getRootLogger();

    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

    private Path configFile;//path to ecs.config
    private Path mDataFile;//path to ecsMetaData
    private static final String metaDataFile = "ecsmeta.config";
    private Socket ecsSocket;
    private TreeMap<BigInteger, Metadata> hashRing;
    private Process serverProc;

    public ECS(String confFile)
    {
        logger.info("Current Directory: " + System.getProperty("user.dir"));
        this.configFile = Paths.get(confFile);
        this.hashRing = new TreeMap<>();

        try {
            if (!Files.exists(Paths.get(metaDataFile)))
            {
                this.mDataFile = Files.createFile(Paths.get(metaDataFile));
            }
            else
            {
                this.mDataFile = Paths.get(metaDataFile);
                hashRingFromFile();//make hashRing from existing metadata file
            }
        } catch (IOException ex1) {
            logger.error("Error! Unable to create Metadata file for ECS");
        }
        catch (NoSuchAlgorithmException ex2) {
            logger.error("Error! Unable to re-create hash ring from ECS's Metadata file");
        }

    }

    public TreeMap<BigInteger, Metadata> getHashRing() { return this.hashRing; }

    @Override
    public KVAdminMessage initService(int numberOfNodes, String logLevel, int cacheSize,
                                      String strategy)
    {
        //call addNode <numberOfNodes> times

        return null;
    }

    @Override
    public KVAdminMessage start()
    {
        //unlock read(get) & write(put)
        Metadata temp = hashRing.firstEntry().getValue();
        int startFails = 0;
        for (Map.Entry<BigInteger, Metadata> entry : hashRing.entrySet())
        {
            try {
                temp = entry.getValue();
                startNode(temp.address, temp.port);
            } catch (IOException ex) {
                logger.error("Error! Unable to start KVServer <" +
                        temp.address + ":" + temp.port + ">");
                startFails++;
            }
        }

        if(startFails > 0) {
            return new RespToECS(null, -1,KVAdminMessage.StatusType.START_ERROR);
        } else {
            return new RespToECS(null, -1,KVAdminMessage.StatusType.START_SUCCESS);
        }
    }

    //TODO: startNode, stopNode, shutDownNode can be combined into 1 function
    private void startNode(String address, int port) throws IOException
    {
        //unlock read(get) & write(put)
        String msg = "ECS,START_SERVER";
        TextMessage response;

        connect(address, port);
        sendMessage(new TextMessage(msg));

        response = receiveMessage();
        disconnect();

        if(response.getMsg().equals("SERVER_STARTED")) {
            logger.info("KVServer <" + address + ":" + port + "> STARTED!");
        } else {
            throw new IOException();
        }
    }

    @Override
    public KVAdminMessage stop()
    {
        Metadata temp = hashRing.firstEntry().getValue();
        int stopFails = 0;
        for (Map.Entry<BigInteger, Metadata> entry : hashRing.entrySet())
        {
            try {
                temp = entry.getValue();
                stopNode(temp.address, temp.port);
            } catch (IOException ex) {
                logger.error("Error! Unable to stop KVServer <" +
                        temp.address + ":" + temp.port + ">");
                stopFails++;
            }
        }
        if(stopFails > 0) {
            return new RespToECS(null, -1,KVAdminMessage.StatusType.STOP_ERROR);
        } else {
            return new RespToECS(null, -1,KVAdminMessage.StatusType.STOP_SUCCESS);
        }
    }
    public void stopNode(String address, int port) throws IOException
    {
        //lock read(get) & write(put)
        String msg = "ECS,STOP_SERVER";
        TextMessage response;

        connect(address, port);
        sendMessage(new TextMessage(msg));

        response = receiveMessage();
        disconnect();

        if(response.getMsg().equals("SERVER_STOPPED")) {
            logger.info("KVServer <" + address + ":" + port + "> STOPPED!");
        } else {
            throw new IOException();
        }
    }

    @Override
    public void shutDown()
    {
        Metadata temp = hashRing.firstEntry().getValue();
        for (Map.Entry<BigInteger, Metadata> entry : hashRing.entrySet())
        {
            try {
                temp = entry.getValue();
                shutDownNode(temp.address, temp.port);
            } catch (IOException ex) {
                logger.error("Error! Unable to shutdown KVServer <" +
                        temp.address + ":" + temp.port + ">");
            }
        }

        hashRing.clear();//remove all KVServer from the ring
        try {
            resetECSFiles();
        } catch (IOException ex) {
            logger.error("Error! Unable to reset ECS's config files");
        }

    }
    private void shutDownNode(String address, int port) throws IOException
    {
        String msg = "ECS,SHUTDOWN";
        TextMessage response;

        connect(address, port);
        sendMessage(new TextMessage(msg));

        response = receiveMessage();
        disconnect();

        if(response.getMsg().equals("SHUTDOWN_SUCCESS")) {
            logger.info("KVServer <" + address + ":" + port + "> successfully shutdown");
        } else {
            throw new IOException();
        }
    }

    /**
     * Design Decision: If initService calls this function, then
     * multiple KVServer are required to be initialized. In that case,
     * the following 3 actions are taken ONLY ONCE AFTER calling
     * this function <numOfNodes> times to reduce # of message being
     * passed between ECS & KVServers:
     * 1    Writing meta data to to ecsmeta.config
     * 2    sending meta data to all KVServers
     * 3    starting KVServers
     *
     * If this function is called via "add" command via ECSClient,
     * then the actions above are taken per function call as usual.
     */
    @Override
    public KVAdminMessage addNode(String logLevel, int cacheSize,
                                  String strategy, boolean addOneNode)
    {
        String serverAddress;//use address to run SSH session
        int serverPort;
        try
        {
            //1 Choose one of the available KVServers
            String[] serverInfo = chooseServer();
            if(serverInfo != null)
            {
                serverAddress = serverInfo[0];
                serverPort = Integer.parseInt(serverInfo[1]);

                //2 Add the KVServer to the hashRing
                addToRing(serverAddress,serverPort);

                //3 Update Metadata file
                if(addOneNode) {
                    WriteMetaDataToFile();
                }
                //4 Run a KVServer process on <serverAddress:serverPort>
                runServerProc(serverPort, logLevel, cacheSize, strategy);

                //5 stop KVServer (put & get is denied because this server doesn't have metadata)
                stopNode(serverAddress,serverPort);

                //TODO: 6 Transfer affected KV-pairs to the new KVServer(send addr & port of successor)


                //7 Send updated Metadata file to all servers (including the new one)
                if(addOneNode) {
                    sendMetadata();

                    //8 start KVServer (put & get is allowed)
                    startNode(serverAddress,serverPort);

                }

                return new RespToECS(serverAddress, serverPort,KVAdminMessage.StatusType.ADD_SUCCESS);
            }
            else
            {
                logger.error("Error! No KVServer available in ecs.config");
            }

        } catch (IOException ex1) {
            logger.error("Error! Unable to run KVServer");
        }  catch (NoSuchAlgorithmException ex3) {
            logger.error("Error! Unable to add KVServer to the hash ring");
        }

        return new RespToECS(null, -1,KVAdminMessage.StatusType.ADD_ERROR);
    }

    public void addToRing(String address, int port) throws NoSuchAlgorithmException
    {
        Metadata temp = new Metadata(address, port);
        BigInteger currEndHash;
        Metadata nextServer, prevServer;

        currEndHash =  md5.HashInBI(address + ":" + port);
        temp.endHash = currEndHash.toString(16);

        if(hashRing.isEmpty())
        {
            temp.startHash = currEndHash.add(new BigInteger("1",10)).toString(16);
        }
        else if(hashRing.containsKey(currEndHash))
        {
            logger.error("Error! KVServer <" + address + ":" + port + "> already in hash ring");
            return;//don't add a KVServer to the ring if it already exists
        }
        else if(hashRing.higherKey(currEndHash) == null)//no hash is higher than currEndHash
        {
            prevServer = hashRing.firstEntry().getValue();
            temp.startHash = prevServer.startHash;
            prevServer.startHash = currEndHash.add(new BigInteger("1",10)).toString(16);
        }
        else//server is the first in the ring or somewhere in the middle
        {
            nextServer = hashRing.higherEntry(currEndHash).getValue();
            temp.startHash = nextServer.startHash;
            nextServer.startHash = currEndHash.add(new BigInteger("1",10)).toString(16);
        }

        hashRing.put(currEndHash, temp);//add in the hash ring
    }

    public void WriteMetaDataToFile() throws IOException
    {
        ArrayList<String> fileContent = new ArrayList<>();

        for(Map.Entry<BigInteger,Metadata> entry : hashRing.entrySet())
        {
            Metadata temp = entry.getValue();
            fileContent.add(temp.address + ";" + temp.port + ";" +
                            temp.startHash + ";" + temp.endHash);
        }

        Files.write(mDataFile, fileContent, StandardCharsets.UTF_8);
    }

    public void resetECSFiles() throws IOException
    {
        //Reset ecs's metadata file
        Files.deleteIfExists(mDataFile);
        Files.createFile(mDataFile);

        //Reset ecs's config file by writing "A" (for available) for each KVServer in it
        ArrayList<String> fileContent = new ArrayList<>(Files.readAllLines(configFile,
                StandardCharsets.UTF_8));

        for (int i = 0; i < fileContent.size(); i++)
        {
            String[] line = fileContent.get(i).split(",");
            fileContent.set(i, line[0] + "," + line[1] + "," + "A");
        }

        Files.write(configFile, fileContent, StandardCharsets.UTF_8);
    }

    public void UpdateConfigFile(String address, int port) throws IOException
    {
        ArrayList<String> fileContent = new ArrayList<>(Files.readAllLines(configFile,
                StandardCharsets.UTF_8));

        for (int i = 0; i < fileContent.size(); i++)
        {
            String[] line = fileContent.get(i).split(",");
            if(line[0].equals(address) && Integer.parseInt(line[1]) == port) {
                fileContent.set(i, line[0] + "," + line[1] + "," + "A");
            }
        }

        Files.write(configFile, fileContent, StandardCharsets.UTF_8);
    }

    /**
     * Send ecsmeta.config file to all running KVServer. the config file's
     * contents can be derived from hashRing data structure or be read from
     * ecsmeta.config. The design decision made here is to use hashRing to
     * create metadata message because memory access is faster than I/O access.
     * @throws IOException
     *                      unable to send metadata file to KVServer(s) <address:port>
     */
    public void sendMetadata() throws IOException
    {
        StringBuilder createMsg = new StringBuilder();
        createMsg.append("ECS,METADATA,");
        Metadata temp;

        for(Map.Entry<BigInteger,Metadata> entry : hashRing.entrySet())
        {
            temp = entry.getValue();
            String dataLine = temp.address + ";" + temp.port + ";" +
                    temp.startHash + ";" + temp.endHash + ",";//comma(,) for string.split on KVServer-side
            createMsg.append(dataLine);
        }


        TextMessage response;
        String address, metadatMsg = createMsg.toString();
        int port;
        for(Map.Entry<BigInteger,Metadata> entry : hashRing.entrySet())
        {
            temp = entry.getValue();
            address = temp.address;
            port = temp.port;

            connect(address, port);
            sendMessage(new TextMessage(metadatMsg));

            response = receiveMessage();
            disconnect();

            if (response.getMsg().equals("RECEIVED_METADATA")) {
                logger.info("Metadata received by KVServer <" + address + ":" + port + ">");
            }
            else {
                logger.error("Error! Unable to send metadata to KVServer <" +
                                address + ":" + port + ">");
            }
        }
    }

    /**
     * Re-create hashRing structure from ecsmeta.config file
     * to record information about the currently running KVServers
     * @throws IOException
     *                      fails to read ecs' metadata file
     * @throws NoSuchAlgorithmException
     *                      fails to hash <address:port>
     */
    private void hashRingFromFile() throws IOException, NoSuchAlgorithmException
    {
        ArrayList<String> fileContent = new ArrayList<>(Files.readAllLines(mDataFile,
                StandardCharsets.UTF_8));

        for (int i = 0; i < fileContent.size(); i++)
        {
            String[] line = fileContent.get(i).split(";");
            hashRing.put(md5.HashInBI(line[0] + ":" + line[1]),
                         new Metadata(line[0], Integer.parseInt(line[1]),line[2], line[3]));
        }
    }

    @Override
    public KVAdminMessage removeNode(String address, int port)
    {
        Metadata successorInfo;
        try {
            BigInteger serverHash =  md5.HashInBI(address + ":" + port);

            if (hashRing.isEmpty() || !hashRing.containsKey(serverHash)) {
                logger.error("KVServer<" + address + ":" + port + "> NOT in Hash Ring");
                return new RespToECS(null, -1, KVAdminMessage.StatusType.REMOVE_ERROR);
            }

            //1 Remove node form the hashRing & update hashRange of successor node
            /*
            successorServer: the server that has the next hash to the serverHash.
            If serverHash has a hash higher than all KVServers, then it is the
            KVServer with the lowest hash (due to wrap-around).
             */
            if(hashRing.higherKey(serverHash) == null) {
                successorInfo = hashRing.firstEntry().getValue();
            } else {
                successorInfo = hashRing.higherEntry(serverHash).getValue();
            }
            successorInfo.startHash = hashRing.get(serverHash).startHash;
            hashRing.remove(serverHash);//remove server to be deleted from hash ring

            //2 Update both ECS Config files
            WriteMetaDataToFile();
            UpdateConfigFile(address, port);

            //3 lockWrite on KVServer to be removed(puts are denied, gets are allowed)
            if(!setWriteStatus(address, port,"LOCKWRITE"))
            {
                return new RespToECS(null, -1,KVAdminMessage.StatusType.REMOVE_ERROR);
            }

            //TODO: 4 Transfer all KV-pairs to the successor KVServer(send addr & port of successor)


            //5 Shutdown (stopServer) the node to be removed
            shutDownNode(address, port);

            //6 Send updated Metadata file to all servers
            sendMetadata();

            //No need to unlock KVServer that is going to be removed

        } catch (NoSuchAlgorithmException ex1) {
            logger.error("Unable to hash KVServer<" + address + ":" + port + "> ");
        } catch (IOException ex2) {
            logger.error("Error! Unable to update Metadata");
        } catch (Exception ex3) {
            logger.error("Error! Unable to shutdown KVServer<" + address + ":" + port + ">");
        }

        return new RespToECS(address, port,KVAdminMessage.StatusType.REMOVE_SUCCESS);
    }
	
	private boolean setWriteStatus(String address, int port, String status)
    {
        String msg = "ECS," + status;
        TextMessage response;
        try {
            connect(address, port);
            sendMessage(new TextMessage(msg));

            response = receiveMessage();
            disconnect();

            if(status.equals("LOCKWRITE") && response.getMsg().equals("SERVER_WRITE_LOCK")) {
                logger.info("Write LOCKED on KVServer <" + address + ":" + port + ">");
                return true;
            } else if(status.equals("UNLOCKWRITE") && response.getMsg().equals("SERVER_WRITE_UNLOCK")) {
                logger.info("Write UNLOCKED on KVServer <" + address + ":" + port + ">");
                return true;
            } else {
                logger.error("Error! " + status + " FAILED on KVServer <" + address + ":" + port + ">");
            }

        } catch (IOException ex) {
            logger.error("Error! Unable to set write status on KVServer <" + address + ":" + port + ">");
        }
        return false;
    }

    private String[] chooseServer() throws IOException
    {
        ArrayList<String> fileContent = new ArrayList<>(Files.readAllLines(configFile,
                                    StandardCharsets.UTF_8));

        String address = null, port = null, isAvailable;
        for (int i = 0; i < fileContent.size(); i++)
        {
            String[] line = fileContent.get(i).split(",");
            isAvailable = line[2];
            //pick the first address:port that ends with "A"
            if (isAvailable.equals("A"))
            {
                fileContent.set(i, line[0] + "," + line[1] + "," + "NA");
                address = line[0];
                port = line[1];
                break;
            }
        }

        Files.write(configFile, fileContent, StandardCharsets.UTF_8);
        return (address == null || port == null) ? null : (new String[] {address, port});
    }

    //TODO use SSH
    public void runServerProc(int port, String logLevel,
                               int cacheSize, String strategy) throws IOException
    {
        //java -jar ms2-server.jar 9000 ALL 10 LRU
        String runCmd = "java -jar ms2-server.jar " + port + " "
                        + logLevel + " " + cacheSize + " " + strategy;
        Runtime run = Runtime.getRuntime();

        this.serverProc = run.exec(runCmd);

        logger.info("Waiting For KVServer process to begin ...");
        try {
            Thread.sleep(3000);//wait (in ms) for KVServer to run
        } catch (InterruptedException ex) {
            logger.error("Error! Thread unable to sleep after running KVServer process");
        }

        logger.info("KVServer process running!!");
    }

    /**
     * Connect to KVServer @ <address:port>
     */
    public void connect(String address, int port) throws IOException
    {
        this.ecsSocket = new Socket(address,port);
        //output = this.ecsSocket.getOutputStream();
        //input = this.ecsSocket.getInputStream();
        logger.info("Connection established via ECS");
    }

    /**
     * Disconnect from KVServer that ECS is currently connected to
     */
    public void disconnect()
    {
        logger.info("trying to close ECS connection ...");

        try {
            if (ecsSocket != null) {
                //input.close();
                //output.close();
                ecsSocket.close();
                ecsSocket = null;
                logger.info("ECS connection closed!");
            }
        } catch (Exception ex) {
            logger.error("Unable to close ECS connection!");
        }
    }

    /**
     * Method sends a TextMessage using this socket.
     * @param msg the message that is to be sent.
     * @throws IOException some I/O error regarding the output stream
     */
    public void sendMessage(TextMessage msg) throws IOException
    {
        OutputStream output = this.ecsSocket.getOutputStream();

        byte[] msgBytes = msg.getMsgBytes();
        output.write(msgBytes, 0, msgBytes.length);
        output.flush();
        logger.info("SENT TO \t<"
                + ecsSocket.getInetAddress().getHostAddress() + ":"
                + ecsSocket.getPort() + ">: '"
                + msg.getMsg() +"'");
    }

    private TextMessage receiveMessage() throws IOException
    {
        InputStream input = this.ecsSocket.getInputStream();

        int index = 0;
        byte[] msgBytes = null, tmp = null;
        byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
        byte read = (byte) input.read();
        boolean reading = true;

        while(read != 13 && reading) {/* carriage return */
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
            if((read > 31 && read < 127)) {
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
        logger.info("RECEIVED FROM \t<"
                + ecsSocket.getInetAddress().getHostAddress() + ":"
                + ecsSocket.getPort() + ">: '"
                + msg.getMsg().trim() + "'");
        return msg;
    }
}