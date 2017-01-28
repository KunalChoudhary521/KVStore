package app_kvServer;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by Zabeeh on 1/26/2017.
 */
public class FileStoreHelper {

    private String file;

    private ReentrantLock originalFileLock;

    public enum FileStoreStatusType {
        PUT_SUCCESS,
        DELETE_SUCCESS,
        DELETE_FAIL,
        UPSERT_SUCCESS,
        UPSERT_FAIL
    };

    public FileStoreHelper(String fileName){
        this.file = fileName;
        originalFileLock = new ReentrantLock();
    }

    public String FindFromFile(String key) throws Exception{
        try {
            String currPath = System.getProperty("user.dir");

            originalFileLock.lock();

            FileInputStream in = new FileInputStream(file);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));

            String line = null;

            while((line = reader.readLine()) != null){
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

                    if (currKey.equals(key)) {
                        // get the value of the xml entry
                        String value = "";
                        keyIndex++;
                        int to_start = keyIndex;
                        do {
                            keyIndex++;
                        } while (!line.substring(keyIndex, keyIndex + 8).equals("</entry>"));
                        return line.substring(to_start, keyIndex);
                    }
                } else {
                    break;
                }
            }

            reader.close();

            originalFileLock.unlock();
            return null;
        } catch (Exception ex) {
            originalFileLock.unlock();
            throw new Exception(ex.getMessage());
        }
    }

    public FileStoreStatusType PutInFile(String key, String value) throws Exception{
        try {
            //String currPath = System.getProperty("user.dir");

            originalFileLock.lock();

            BufferedWriter writer = new BufferedWriter(new FileWriter(file, true));

            String line = createFilEntryFromKVP(key, value);

            writer.write(line); writer.newLine();
            writer.flush();
            writer.close();

            originalFileLock.unlock();

            return FileStoreStatusType.PUT_SUCCESS;
        } catch (Exception ex) {
            originalFileLock.unlock();
            throw new Exception(ex.getMessage());
        }
    }

    public FileStoreStatusType DeleteInFile(String key) throws Exception{
        try {

            originalFileLock.lock();

            FileInputStream in = new FileInputStream(file);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));

            PrintWriter writer = new PrintWriter("./newFile.txt", "UTF-8");

            String line = null;

            boolean success = false; //only successful if the key was found

            while((line = reader.readLine()) != null) {
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

                    if (currKey.equals(key) == false) {
                        // get the value of the xml entry
                        writer.println(line);
                    } else {
                        success = true;
                    }
                } else {
                    break;
                }
            }

            reader.close();
            writer.flush();
            writer.close();

            File old = new File("./TestFile.txt");
            File n = new File("./newFile.txt");

            Files.delete(old.toPath());

            File another = new File("./TestFile.txt");
            n.renameTo(another);

            originalFileLock.unlock();

            if(success) {
                return FileStoreStatusType.DELETE_SUCCESS;
            } else {
                return FileStoreStatusType.DELETE_FAIL;
            }
        } catch (Exception ex) {
            originalFileLock.unlock();

            throw new Exception(ex.getMessage());
        }
    }

    public FileStoreStatusType UpsertInFile(String key, String value) throws Exception{
        try {
            originalFileLock.lock();

            FileInputStream in = new FileInputStream(file);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));

            PrintWriter writer = new PrintWriter("./newFile.txt","UTF-8");

            String newline = createFilEntryFromKVP(key, value);
            String line = null; //only successful if they key was found

            boolean success = false;

            while((line = reader.readLine()) != null){
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

                    if (currKey.equals(key) == false) {
                        // get the value of the xml entry
                        writer.println(line);
                    } else {
                        success = true;
                        writer.println(newline);
                    }
                } else {
                    break;
                }
            }

            reader.close();
            writer.flush();
            writer.close();

            File old = new File("./TestFile.txt");
            File n = new File("./newFile.txt");

            old.delete();

            File another = new File("./TestFile.txt");
            n.renameTo(another);

            originalFileLock.unlock();

            if(success) {
                return FileStoreStatusType.UPSERT_SUCCESS;
            }else {
                return FileStoreStatusType.UPSERT_FAIL;
            }
        } catch (Exception ex) {
            originalFileLock.unlock();
            throw new Exception(ex.getMessage());
        }
    }

    private String createFilEntryFromKVP(String key, String value){
        return "<entry key=\"" + key + "\">" + value + "</entry>";
    }
}
