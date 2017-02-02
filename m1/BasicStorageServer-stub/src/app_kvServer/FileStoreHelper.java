package app_kvServer;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by Zabeeh on 1/26/2017.
 */


/**
 * An API for storing and getting KVPs from a file on disk
 */
public class FileStoreHelper {

    private String file;
    private boolean log;

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
    public FileStoreHelper(String fileName, boolean log){
        this.file = fileName;
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

            if(log){
                System.out.println("Trying to get current directory");
            }

            String currPath = System.getProperty("user.dir");

            if(log){
                System.out.println("Current directory: " + currPath);
            }

            if(log){
                System.out.println("Acquiring lock - Get");
            }
            originalFileLock.lock();

            if(log){
                System.out.println("Opening streams");
            }
            FileInputStream in = new FileInputStream(file);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));

            String line = null;

            // read each line from the file
            if(log){
                System.out.println("Opened stream, beginning reading line by line");
            }
            while((line = reader.readLine()) != null){
                // if line is not empty parse the xml line
                // read each line from the file
                if(log){
                    System.out.println("line: "+ line);
                }
                if(!line.isEmpty()) {
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

                    if(log){
                        System.out.println("key from file: "+ currKey);
                    }
                    // compare the key from the line to what was passed in
                    if (currKey.equals(key)) {

                        if(log){
                            System.out.println("Found the key constructing the value.");
                        }
                        // get the value of the xml entry
                        String value = "";
                        keyIndex++;
                        int to_start = keyIndex;
                        do {
                            keyIndex++;
                        } while (!line.substring(keyIndex, keyIndex + 8).equals("</entry>"));

                        //if key matched return it otherwise continue reading the file

                        if(log){
                            System.out.println("Returning the value.");
                        }
                        reader.close();
                        in.close();
                        System.gc(); //FORCES the file to be closed
                        return line.substring(to_start, keyIndex);
                    }
                } else {
                    break;
                }
            }

            if(log){
                System.out.println("Could not find the key returning null.");
            }

            reader.close();
            in.close();
            System.gc(); //FORCES the file to be closed

            return null;
        } catch (Exception ex) {
            if(log){
                System.out.println(ex.getMessage());
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
            //String currPath = System.getProperty("user.dir");

            if(log){
                System.out.println("Acquiring lock - Put");
            }

            originalFileLock.lock();

            if(log){
                System.out.println("Opening stream for writing");
            }

            BufferedWriter writer = new BufferedWriter(new FileWriter(file, true));

            String line = createFilEntryFromKVP(key, value);

            if(log){
                System.out.println("Successfully wrote into stream");
            }

            writer.write(line); writer.newLine();
            writer.flush();
            writer.close();
            System.gc();

            return FileStoreStatusType.PUT_SUCCESS;

        } catch (Exception ex) {
            if(log){
                System.out.println(ex.getMessage());
            }
            originalFileLock.unlock();
            throw new Exception(ex.getMessage());
        } finally {
            originalFileLock.unlock();
        }
    }

    /**
     * Delete a kvp in the file by creating a new file without the particular kvp
     * @param key string key
     * @value returns a FileStoreStatusType or throws an exception
     * */
    public FileStoreStatusType DeleteInFile(String key) throws Exception{
        try {

            if(log){
                System.out.println("Acquiring lock - Delete");
            }

            originalFileLock.lock();

            if(log){
                System.out.println("Opening streams");
            }

            FileInputStream in = null;
            BufferedReader reader = null;
            PrintWriter writer = null;
            boolean success = false; //only successful if the key was found


            try {

                in = new FileInputStream(file);
                reader = new BufferedReader(new InputStreamReader(in));

                writer = new PrintWriter("./newFile.txt", "UTF-8");

                if(log){
                    System.out.println("Opened streams");
                }

                String line = null;

                if(log){
                    System.out.println("Opened stream, beginning reading line by line");
                }

                while ((line = reader.readLine()) != null) {
                    if(log){
                        System.out.println("line: "+ line);
                    }
                    if (!line.isEmpty()) {
                        int keyIndex = line.indexOf("key=\"");
                        keyIndex += "key\"".length() + 1;
                        String currKey = "";
                        char currChar = 0;
                        do {
                            currChar = line.charAt(keyIndex);
                            if (currChar != '\"') {
                                currKey += currChar;
                            }
                            keyIndex++;
                        } while (currChar != '\"');

                        if(log){
                            System.out.println("key from file: "+ currKey);
                        }

                        if (currKey.equals(key) == false) {
                            if(log){
                                System.out.println("Not the key to delete, so copy it");
                            }
                            // get the value of the xml entry
                            writer.println(line);
                            writer.flush();
                        } else {
                            if(log){
                                System.out.println("Found the key so, don't copy.");
                            }
                            success = true;
                        }
                    } else {
                        break;
                    }
                }
            } catch (Exception ex)
            {
                System.out.println(ex.getMessage());
            } finally {
                if(reader != null) { reader.close(); }
                if(writer != null) { writer.close();}
                if(in != null) { in.close(); }
                System.gc();
            }

            File old = new File("./TestFile.txt");
            File n = new File("./newFile.txt");

            old.delete();

            File another = new File("./TestFile.txt");
            n.renameTo(another);

            if(success) {
                return FileStoreStatusType.DELETE_SUCCESS;
            } else {
                return FileStoreStatusType.DELETE_FAIL;
            }
        } catch (Exception ex) {
            if(log){
                System.out.println(ex.getMessage());
            }
            originalFileLock.unlock();
            throw new Exception(ex.getMessage());
        } finally {
            originalFileLock.unlock();
        }
    }

    /**
     * Upserts a kvp in the file by creating a new file with the updated kvp
     * @param key string key
     * @value returns a FileStoreStatusType or throws an exception
     * */
    public FileStoreStatusType UpsertInFile(String key, String value) throws Exception{
        try {

            if(log){
                System.out.println("Acquiring lock - Update");
            }

            originalFileLock.lock();

            if(log){
                System.out.println("Opening streams");
            }

            FileInputStream in = null;
            BufferedReader reader = null;
            PrintWriter writer = null;
            boolean success = false;

            try{
                in = new FileInputStream(file);
                reader = new BufferedReader(new InputStreamReader(in));
                writer = new PrintWriter("./newFile.txt","UTF-8");

                String newline = createFilEntryFromKVP(key, value);
                String line = null; //only successful if they key was found

                if(log){
                    System.out.println("Opened all streams");
                }

                while((line = reader.readLine()) != null){
                    if(log){
                        System.out.println("line: " + line);
                    }
                    if(!line.isEmpty()) {
                        int keyIndex = line.indexOf("key=\"");
                        keyIndex += "key\"".length() + 1;
                        String currKey = "";
                        char currChar = 0;
                        do {
                            currChar = line.charAt(keyIndex);
                            if (currChar != '\"') {
                                currKey += currChar;
                            }
                            keyIndex++;
                        } while (currChar != '\"');

                        if(log){
                            System.out.println("key from file: " + currKey);
                        }

                        if (currKey.equals(key) == false) {
                            if(log){
                                System.out.println("This is not the key to update, so just copy it to new file.");
                            }
                            // get the value of the xml entry
                            writer.println(line);
                            writer.flush();
                        } else {
                            if(log){
                                System.out.println("This is not the key to update, so update and copy it to new file.");
                            }
                            success = true;
                            writer.println(newline);
                            writer.flush();
                        }
                    } else {
                        break;
                    }
                }
            }  catch (Exception ex)
            {
                System.out.println(ex.getMessage());
            } finally {
                if(reader != null) { reader.close(); }
                if(writer != null) { writer.close();}
                if(in != null) { in.close(); }
                System.gc();
            }

            File old = new File("./TestFile.txt");
            File n = new File("./newFile.txt");

            old.delete();

            File another = new File("./TestFile.txt");
            n.renameTo(another);

            if(success) {
                return FileStoreStatusType.UPSERT_SUCCESS;
            }else {
                return FileStoreStatusType.UPSERT_FAIL;
            }
        } catch (Exception ex) {
            if(log){
                System.out.println(ex.getMessage());
            }
            originalFileLock.unlock();
            throw new Exception(ex.getMessage());
        } finally {
            originalFileLock.unlock();
        }
    }

    private String createFilEntryFromKVP(String key, String value){
        return "<entry key=\"" + key + "\">" + value + "</entry>";
    }
}
