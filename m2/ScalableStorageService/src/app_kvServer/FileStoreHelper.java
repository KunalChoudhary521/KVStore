package app_kvServer;

import org.apache.log4j.Logger;

import java.io.*;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;
import app_kvEcs.md5;

/**
 * Created by Zabeeh on 1/26/2017.
 */


/**
 * An API for storing and getting KVPs from a file on disk
 */
public class FileStoreHelper {

    private String fileLocation;
    private boolean log;
    private static Logger logger = Logger.getRootLogger();

    private ReentrantLock originalFileLock;

    /**
     * Various result statuses
     * */
    public enum FileStoreStatusType {
        PUT_SUCCESS,
        PUT_FAIL,
        DELETE_SUCCESS,
        DELETE_FAIL,
        UPSERT_SUCCESS,
        UPSERT_FAIL
    };

    /**
     * Constructor just takes a fileName and initializes a lock for all file access
     * */
    public FileStoreHelper(String fileLocation, boolean log){
        this.fileLocation = fileLocation;
        this.log = log;
        originalFileLock = new ReentrantLock();
    }

    /**
     * Searches a kvp in a file
     * @param key a string key
     * @return string value, null when not found
     * */
    public String FindFromFile(String key) throws Exception{
        try {

            String fileName = md5.HashS(key);

            if(log){
                logger.info("File location: " + fileLocation);
            }

            if(log){
                logger.info("Acquiring lock - Get");
            }
            originalFileLock.lock();

            if(log){
                logger.info("Opening streams");
            }

            File file = new File(fileLocation+"\\"+fileName);

            if(!file.exists()){
                return null;
            } else {
                FileInputStream in = new FileInputStream(file);
                BufferedReader reader = new BufferedReader(new InputStreamReader(in));

                String line = null;

                // read each line from the file
                if (log) {
                    logger.info("Opened stream, beginning reading line by line");
                }
                line = reader.readLine();
                if (!line.isEmpty()) {
                    int keyIndex = line.indexOf("key=\"");
                    keyIndex += "key\"".length() + 1;
                    String currKey = "";
                    char currChar = 0;
                    // read the key
                    do {
                        currChar = line.charAt(keyIndex);
                        if (currChar != '\"') {
                            currKey += currChar;
                        }
                        keyIndex++;
                    } while (currChar != '\"');

                    if (log) {
                        logger.info("key from file: " + currKey);
                    }
                    // compare the key from the line to what was passed in
                    if (currKey.equals(key)) {

                        if (log) {
                            logger.info("Found the key constructing the value.");
                        }
                        // get the value of the xml entry
                        String value = "";
                        keyIndex++;
                        int to_start = keyIndex;
                        do {
                            keyIndex++;
                        } while (!line.substring(keyIndex, keyIndex + 8).equals("</entry>"));

                        //if key matched return it otherwise continue reading the file

                        if (log) {
                            logger.info("Returning the value.");
                        }
                        reader.close();
                        in.close();
                        System.gc(); //FORCES the file to be closed
                        return line.substring(to_start, keyIndex);
                    }
                    throw new Exception("Value was invalid.");
                }
                throw new Exception("Value was invalid.");
            }
        } catch (Exception ex) {
            if(log){
                logger.info(ex.getMessage());
            }
            originalFileLock.unlock();
            throw new Exception(ex.getMessage());
        } finally {
            originalFileLock.unlock();
        }
    }

    /**
     * Stores a kvp in the file
     * @param key string key
     * @param value string value
     * @value returns a FileStoreStatusType or throws an exception
     * */
    public FileStoreStatusType PutInFile(String key, String value) throws Exception{
        try {

            String fileName = md5.HashS(key);

            if(log){
                logger.info("Current directory: " + fileLocation);
            }

            if(log){
                logger.info("Acquiring lock - Put");
            }

            originalFileLock.lock();

            if(log){
                logger.info("Opening stream for writing");
            }

            File file = new File(fileLocation+"\\"+fileName);

            if(file.exists()){
                throw new Exception("File already exists, do upsert not put");
            }

            file.createNewFile();

            BufferedWriter writer = new BufferedWriter(new FileWriter(file, true));

            String line = createFilEntryFromKVP(key, value);

            if(log){
                logger.info("Successfully wrote into stream");
            }

            writer.write(line); writer.newLine();
            writer.flush();
            writer.close();
            System.gc();
        } catch (Exception ex) {
            if(log){
                logger.info(ex.getMessage());
            }
            originalFileLock.unlock();
            throw new Exception(ex.getMessage());
        } finally {
            originalFileLock.unlock();
        }
        return FileStoreStatusType.PUT_SUCCESS;
    }

    /**
     * Delete a kvp in the file by creating a new file without the particular kvp
     * @param key string key
     * @value returns a FileStoreStatusType or throws an exception
     * */
    public FileStoreStatusType DeleteInFile(String key) throws Exception{
        try {

            String fileName = md5.HashS(key);

            if(log){
                logger.info("Current directory: " + fileLocation);
            }

            if(log){
                logger.info("Acquiring lock - Delete");
            }

            originalFileLock.lock();

            File file = new File(fileLocation+"\\"+fileName);

            if(file.exists()){
                file.delete();
            } else {
                throw new Exception("File does not exist");
            }
        } catch (Exception ex) {
            if(log){
                logger.info(ex.getMessage());
            }
            originalFileLock.unlock();
            throw new Exception(ex.getMessage());
        } finally {
            originalFileLock.unlock();
        }
        return FileStoreStatusType.DELETE_SUCCESS;
    }

    /**
     * Upserts a kvp in the file by creating a new file with the updated kvp
     * @param key string key
     * @value returns a FileStoreStatusType or throws an exception
     * */
    public FileStoreStatusType UpsertInFile(String key, String value) throws Exception{
        try {
            String fileName = md5.HashS(key);

            if(log){
                logger.info("Current directory: " + fileLocation);
            }

            if(log){
                logger.info("Acquiring lock - Update");
            }

            originalFileLock.lock();

            if(log){
                logger.info("Opening streams");
            }

            File file = new File(fileLocation+"\\"+fileName);

            if(!file.exists()){
                throw new Exception("File does not exist");
            }

            BufferedWriter writer = new BufferedWriter(new FileWriter(file, false));

            String line = createFilEntryFromKVP(key, value);

            writer.write(line);
            writer.flush();
            writer.close();
        } catch (Exception ex) {
            if(log){
                logger.info(ex.getMessage());
            }
            originalFileLock.unlock();
            throw new Exception(ex.getMessage());
        } finally {
            originalFileLock.unlock();
        }
        return FileStoreStatusType.UPSERT_SUCCESS;
    }

    public String[] GetFileHashes(){
        try{

            originalFileLock.lock();
            File[] files = new File(fileLocation).listFiles();
            originalFileLock.unlock();

            String[] fileNames = new String[files.length];
            for(int i =0; i<files.length;i++){
                fileNames[i] = files[i].getName();
            }
            Arrays.sort(fileNames);

            return fileNames;
        } catch (Exception ex){
            originalFileLock.unlock();
            logger.info("Something went wrong while trying to obtain file metadata");
            return null;
        } finally{
            originalFileLock.unlock();
        }
    }

    private String createFilEntryFromKVP(String key, String value){
        return "<entry key=\"" + key + "\">" + value + "</entry>";
    }
}
