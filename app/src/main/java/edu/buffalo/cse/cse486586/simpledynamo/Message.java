package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

public class Message {
    private MessageType msgType;
    private String key;
    private String value;
    private int[] prefList;
    private int fromPort;
    private int toPort;

    public Message(MessageType msgType, String key, String value, int[] prefList, int fromPort, int toPort) {
        this.msgType = msgType;
        this.key = key;
        this.value = value;
        this.prefList = prefList;
        this.fromPort = fromPort;
        this.toPort = toPort;
    }

    public MessageType getMsgType() {
        return msgType;
    }

    public void setMsgType(MessageType msgType) {
        this.msgType = msgType;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) { this.key = key;}

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public int[] getPrefList() {
        return prefList;
    }

    public void setPrefList(int[] prefList) {
        this.prefList = prefList;
    }

    public int getPrefListIndex(int port){
        for(int i = 0; i < prefList.length; i++){
            if(prefList[i] == port)
                return i;
        }
        Log.e("MESSAGE", "getPrefListIndex DOES NOT CONTAIN PORT " + port);
        return -1;
    }

    public int getFromPort() {
        return fromPort;
    }

    public void setFromPort(int fromPort) {
        this.fromPort = fromPort;
    }

    public int getToPort() {
        return toPort;
    }

    public void setToPort(int toPort) {
        this.toPort = toPort;
    }

    public String toString(){
        return "MsgType: " + msgType + " key: " + key + " value: " + value + " fromPort: " + fromPort + " toPort: " + toPort;
    }
}