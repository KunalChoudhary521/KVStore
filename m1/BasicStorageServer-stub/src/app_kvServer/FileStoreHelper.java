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
            in.close();
            System.gc();

            return null;
        } catch (Exception ex) {
            originalFileLock.unlock();
            throw new Exception(ex.getMessage());
        } finally {
            originalFileLock.unlock();
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
            System.gc();


            return FileStoreStatusType.PUT_SUCCESS;
        } catch (Exception ex) {
            originalFileLock.unlock();
            throw new Exception(ex.getMessage());
        } finally {
            originalFileLock.unlock();
        }
    }

    public FileStoreStatusType DeleteInFile(String key) throws Exception{
        try {

            originalFileLock.lock();

            FileInputStream in = null;
            BufferedReader reader = null;
            PrintWriter writer = null;
            boolean success = false; //only successful if the key was found

            try {

                in = new FileInputStream(file);
                reader = new BufferedReader(new InputStreamReader(in));

                writer = new PrintWriter("./newFile.txt", "UTF-8");

                String line = null;

                while ((line = reader.readLine()) != null) {
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
                            writer.flush();
                        } else {
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
            originalFileLock.unlock();

            throw new Exception(ex.getMessage());
        } finally {
            originalFileLock.unlock();
        }
    }

    public FileStoreStatusType UpsertInFile(String key, String value) throws Exception{
        try {
            originalFileLock.lock();

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
                            writer.flush();
                        } else {
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
