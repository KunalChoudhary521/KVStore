package app_kvServer;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

/**
 * Created by Haashir on 1/26/2017.
 */
public class FileStoreHelper {

    private String file;

    public FileStoreHelper(String fileName){
        this.file = fileName;
    }

    public String FindFromFile(String key) throws Exception{
        try {
            FileInputStream in = new FileInputStream(file);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));

            String line = null;

            while((line = reader.readLine()) != null){
                int keyIndex = line.indexOf("key=\"");
                keyIndex+= "key\"".length() + 1;
                String currKey = "";
                char currChar = 0;
                do{
                    currChar = line.charAt(keyIndex);
                    if(currChar !='\"') {
                        currKey += currChar;
                    }
                    keyIndex++;
                }while(currChar != '\"');
                if(currKey.equals(key)){
                    // get the value of the xml entry

                    return key;
                }
            }

            return null;
        } catch (Exception ex) {
            throw new Exception(ex.getMessage());
        }
    }

    public String storeInFile(String key, String value) {
        String entry = createFilEntryFromKVP(key, value);
        return null;
    }

    private String createFilEntryFromKVP(String key, String value){
        return "<entry key=\"" + key + "\">" + value + "</entry>";
    }
}
