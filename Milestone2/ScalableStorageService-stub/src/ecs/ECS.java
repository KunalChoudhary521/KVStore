package ecs;

import common.Metadata;
import common.TextMessage;
import common.md5;
import common.messages.KVAdminMessage;
import common.messages.RespToECS;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
        return null;
    }

    @Override
    public KVAdminMessage stop()
    {
        return null;
    }

    @Override
    public void shutDown()
    {

    }

    @Override
    public KVAdminMessage addNode(String logLevel, int cacheSize, String strategy)
    {
        String serverAddress;//use address to run SSH session
        int serverPort;
        try
        {
            //1 Choose one of the available KVServers
            String[] serverInfo = (chooseServer()).split(":");
            if(serverInfo[0] != null && serverInfo[1] != null)
            {
                serverAddress = serverInfo[0];
                serverPort = Integer.parseInt(serverInfo[1]);

                //2 Add the KVServer to the hashRing
                addToRing(serverAddress,serverPort);

                //3 Update Metadata file
                updateMetaData();

                //4 Run a KVServer process on <serverAddress:serverPort>
                //runServerProc(serverPort, logLevel, cacheSize, strategy);

                /*5 lockWrite on KVServer so it doesn't server puts from KVClients
                    (Maybe have KVServer lockWrite by default)
                 */

                //6 Transfer affected KV-pairs to the new KVServer(send new KVServer addr & port of successor)

                //7 Send updated Metadata file to all servers (including the new one)

                //8 unlockWrite on this KVServer and remove KV-pairs from old KVServer

                return new RespToECS(serverAddress, serverPort,KVAdminMessage.StatusType.ADD_SUCCESS);
            }
            else
            {
                logger.error("Error! No KVServer available in ecs.config");
            }

        } catch (IOException ex1) {
            logger.error("Error! Unable to run KVServer");
        } catch (NullPointerException ex2) {
            logger.error("Error! No KVServer available in ecs.config");
        } catch (NoSuchAlgorithmException ex3) {
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

    //TODO: performance improvement -> create metadata message here, which will be sent to all KVServers
    public void updateMetaData() throws IOException
    {
        ArrayList<String> fileContent = new ArrayList<>();

        for(Map.Entry<BigInteger,Metadata> entry : hashRing.entrySet())
        {
            Metadata temp = entry.getValue();
            fileContent.add(temp.address + "," + temp.port + "," +
                            temp.startHash + "," + temp.endHash);
        }

        Files.write(mDataFile, fileContent, StandardCharsets.UTF_8);
    }

    /**
     * Re-create hashRing structure from ecsmeta.config file
     * to record informatio about the currently running KVServers
     * @throws IOException
     */
    private void hashRingFromFile() throws IOException, NoSuchAlgorithmException
    {
        ArrayList<String> fileContent = new ArrayList<>(Files.readAllLines(mDataFile,
                StandardCharsets.UTF_8));

        for (int i = 0; i < fileContent.size(); i++)
        {
            String[] line = fileContent.get(i).split(",");
            hashRing.put(md5.HashInBI(line[0] + ":" + line[1]),
                         new Metadata(line[0], Integer.parseInt(line[1]),line[2], line[3]));
        }
    }

    @Override
    public KVAdminMessage removeServer(String address, int port)
    {
        return null;
    }
	
	private void lockWrite(String address, int port)
    {

    }

    private void unlockWrite(String address, int port)
    {

    }

    private String chooseServer() throws IOException
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
        return (address == null || port == null) ? null : address + ":" + port;
    }

    //TODO use SSH
    private void runServerProc(int port, String logLevel,
                               int cacheSize, String strategy) throws IOException
    {
        Process proc;

        //java -jar ms2-server.jar 9000 ALL 10 LRU
        String runCmd = "java -jar ms2-server.jar " + port + " "
                        + logLevel + " " + cacheSize + " " + strategy;
        Runtime run = Runtime.getRuntime();

        proc = run.exec(runCmd);
    }

    /**
     * Connect to KVServer @ <address:port>
     */
    public void connect(String address, int port) throws Exception
    {
        this.ecsSocket = new Socket(address,port);
        //output = this.ecsSocket.getOutputStream();
        //input = this.ecsSocket.getInputStream();
        logger.info("Connection established");
    }

    /**
     * Disconnect from KVServer that ECS is currently connected to
     */
    public void disconnect()
    {
        logger.info("trying to close connection ...");

        try {
            if (ecsSocket != null) {
                //input.close();
                //output.close();
                ecsSocket.close();
                ecsSocket = null;
                logger.info("connection closed!");
            }
        } catch (Exception ex) {
            logger.error("Unable to close connection!");
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