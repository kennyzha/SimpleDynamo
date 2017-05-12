package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.SharedPreferences;
import android.util.Log;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Dynamo {
    private static final String DYNAMO_TAG = "Dynamo";
    private static final int PREF_SIZE = 3;
    public static final String[] NODES = {"5562", "5556", "5554", "5558", "5560"};
    public static final int[] PORTS = {11124, 11112, 11108, 11116, 11120};

    private ConcurrentHashMap<Integer, ConcurrentHashMap<String, String>> portsLog;     // ConcurrentHashMap<port, <key, value>>
    private String[] hashedNodes;
    private SecureRandom secureRandom;

    public Dynamo(){
        secureRandom = new SecureRandom();
        hashedNodes = generateHashedNodes();
        portsLog = new ConcurrentHashMap<Integer, ConcurrentHashMap<String, String>>();
        populatePortsLog();
    }

    private void populatePortsLog() {
        for(int i = 0; i < PORTS.length; i++){
            int curPort = PORTS[i];
            portsLog.put(curPort, new ConcurrentHashMap<String, String>());
            Log.v(DYNAMO_TAG, "Placed port " + curPort + " into portsLog");
        }
    }

    public void insertInPortsLog(String key, String value, int[] prefList){
        for(int i = 0; i < prefList.length; i++){
            portsLog.get(prefList[i]).put(key, value);
        }
    }

    public void deleteFromPortsLog(String key, int[] prefList){
        for(int i = 0; i < prefList.length; i++){
            portsLog.get(prefList[i]).remove(key);
        }
    }

    public void setMsgToPortsLog(Message m, int port){
        StringBuilder keys = new StringBuilder();
        StringBuilder values = new StringBuilder();

        ConcurrentHashMap<String, String> data = portsLog.get(port);
        for(Map.Entry<String, String> entry: data.entrySet()){
            keys.append(entry.getKey() + ":::");
            values.append(entry.getValue() + ":::");
        }
        Log.v(DYNAMO_TAG, "setMsgToPortsLog set Message keys " + keys.toString());
        Log.v(DYNAMO_TAG, "setMsgToPortsLog set Message value to " + values.toString());
        m.setKey(keys.toString());
        m.setValue(values.toString());
    }

    private String[] generateHashedNodes(){
        String[] hashedArr = new String[NODES.length];
        for(int i = 0; i < hashedArr.length; i++){
            hashedArr[i] = genHash(NODES[i]);
        }
        return hashedArr;
    }

    public String[] getHashedNodes(){
        return hashedNodes;
    }

    // Generates an array of PORTS that the given key needs to be replicated to.
    public int[] getPrefList(String key){
        String hashedKey = genHash(key);
        int[] prefList = new int[PREF_SIZE];

        int coordinatorIndex = getCoordinatorIndex(hashedKey);
        Log.e(DYNAMO_TAG, "(getPrefList) Hash value for key " + key + " is " + hashedKey + " and its coordinator is " + PORTS[coordinatorIndex]);
        for(int i = 0; i < prefList.length; i++){
            prefList[i] = PORTS[coordinatorIndex];
            Log.e(DYNAMO_TAG, "PrefList index " + i + " has port " + prefList[i] + " and corresponds with hashNode " + hashedNodes[coordinatorIndex]);
            coordinatorIndex = (coordinatorIndex + 1) % PORTS.length;
        }
        return prefList;
    }

    private int getCoordinatorIndex(String hashedKey){
        for(int i = 1; i < hashedNodes.length; i++){
            if(hashedKey.compareTo(hashedNodes[i]) <= 0 && hashedKey.compareTo(hashedNodes[i-1]) > 0 ){
                return i;
            }
        }
        return 0;
    }

    public boolean isInCorrectPartition(String key, int port){
        int[] prefList = getPrefList(key);

        for(int replica : prefList){
            if(port == replica){
                Log.e(DYNAMO_TAG, " Key " + key + " belongs in port " + port);
                return true;
            }
        }
        return false;
    }

    public boolean writeFinished(int[] preferenceList, int curPort){
        return curPort == preferenceList[PREF_SIZE - 1];
    }

    // Should call writeFinished before calling this method
    public int getNextNodePort(int[] preferenceList, int curPort){
        for(int i = 0; i < preferenceList.length - 1; i++){
            if(preferenceList[i] == curPort)
                return preferenceList[i+1];
        }
        return -1;
    }

    public String genHash(String input){
        MessageDigest sha1;
        try {
            sha1 = MessageDigest.getInstance("SHA-1");
            byte[] sha1Hash = sha1.digest(input.getBytes());
            Formatter formatter = new Formatter();
            for (byte b : sha1Hash) {
                formatter.format("%02x", b);
            }
            return formatter.toString();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            Log.e(DYNAMO_TAG, "NoSuchAlgorithmException in genHash from Dynamo.");
            return null;
        }
    }

    public String genKeyId(){
        return new BigInteger(130, secureRandom).toString(32);
    }

    public HashMap<String, String> query(SharedPreferences sharedPreferences, String key) {
        HashMap<String, String> hm = new HashMap<String, String>();
        if (key.equals("@")) {
            Map<String, ?> allEntries = sharedPreferences.getAll();

            for (Map.Entry<String, ?> entry : allEntries.entrySet()) {
                hm.put(entry.getKey(), entry.getValue().toString());
            }
        } else{
            hm.put(key, sharedPreferences.getString(key, " KEY NOT FOUND"));
        }
        return hm;
    }

    public void insertToSharedPref(SharedPreferences sharedPreferences, Message msg){
        Log.v("insert", "Storing the key " + msg.getKey() + " hashedVal " + genHash(msg.getKey()) + " in port " + msg.getToPort()) ;
        SharedPreferences.Editor editor = sharedPreferences.edit();
        editor.putString(msg.getKey(), msg.getValue());
        editor.apply();
    }

    public void deleteFromSharedPref(SharedPreferences sharedPreferences, String key){
        SharedPreferences.Editor editor = sharedPreferences.edit();

        if(key.equals("@")){
            editor.clear();
        } else{
            editor.remove(key);
            Log.v("Delete", key);
        }
        editor.commit();
    }

    public int getSuccessorPort(int port){
        for(int i = 0; i < PORTS.length; i++){
            if(PORTS[i] == port){
                int successorIndex = (i + 1) % PORTS.length;
                return PORTS[successorIndex];
            }
        }
        return -1;
    }

    public int getPredecessorPort(int port){
        for(int i = PORTS.length - 1; i >= 1; i--){
            if(PORTS[i] == port){
                return PORTS[i - 1];
            }
        }
        return PORTS[PORTS.length - 1];
    }

}
