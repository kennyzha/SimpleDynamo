package tasks;

import android.os.AsyncTask;
import android.util.Log;

import com.google.gson.Gson;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

import edu.buffalo.cse.cse486586.simpledynamo.Message;
import edu.buffalo.cse.cse486586.simpledynamo.MessageType;

public class ClientTask extends AsyncTask<String, Message, Void>{
    private static final String CLIENT_TAG = "Client";
    private static final String FAILURE_TAG = "Port Failure";
    private Gson gson = new Gson();

    @Override
    protected Void doInBackground(String... params) {
        String msgJson = params[0];
        Message message = gson.fromJson(msgJson, Message.class);
        try {
            SocketAddress socketAddress = new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    message.getToPort());
            Socket socket = new Socket();
            socket.connect(socketAddress, 100);
            Log.e(CLIENT_TAG, "Client is connected");

            BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
            pw.println(msgJson);

            String response = br.readLine();
            Log.e(CLIENT_TAG, "Message received from server " + response + " original message was " + msgJson);

            if(response == null){
                publishProgress(message);
            }
            br.close();
            pw.close();
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
            Log.e(CLIENT_TAG, "EXCEPTION Message received was  " + msgJson);
            publishProgress(message);
        }
        return null;
    }

    @Override
    protected void onProgressUpdate(Message... values) {
        Message message = values[0];
        Log.e(CLIENT_TAG, "EXCEPTION PORT FAILURE FROM: " + message.getToPort());
        int[] msgPrefList = message.getPrefList();
        int curPortIndex;
        switch(message.getMsgType()){
            case INSERT:
            case DELETE:
                curPortIndex = message.getPrefListIndex(message.getToPort());
                if(curPortIndex != msgPrefList.length - 1){
                    Log.v(FAILURE_TAG, " Skipping over port. Setting toPort to " + msgPrefList[1] + "Original port destination was " + message.getToPort());
                    message.setToPort(msgPrefList[curPortIndex + 1]);
                } else{
                    Log.v(FAILURE_TAG, " Responding to fromPort because last port died. FromPort: " + message.getFromPort());
                    MessageType messageType = (message.getMsgType() == MessageType.INSERT) ? MessageType.INSERT_RESPONSE : MessageType.DELETE_RESPONSE;
                    message.setMsgType(messageType);
                    message.setToPort(message.getFromPort());
                }
                break;
            case QUERY:
                if(message.getKey().equals("@")){
                    message.setMsgType(MessageType.QUERY_RESPONSE);
                    message.setToPort(message.getFromPort());
                    message.setKey("");
                    message.setValue("");
                } else{
                    curPortIndex = message.getPrefListIndex(message.getToPort());
                    if(curPortIndex - 1 >= 0){
                        Log.v(FAILURE_TAG, "Querying instead from port " + msgPrefList[curPortIndex - 1]);
                        message.setToPort(msgPrefList[curPortIndex - 1]);
                    }
                }
                break;
            case UPDATE:
                Log.v(FAILURE_TAG, "UPDATE NOT NECCESSARY. AVDS JUST STARTED UP");
                message.setMsgType(MessageType.UPDATE_RESPONSE);
                message.setToPort(message.getFromPort());
                message.setKey("");
                message.setValue("");
                break;

        }
        Log.v(FAILURE_TAG, " Sending out newly constructed message " + gson.toJson(message));
        new ClientTask().executeOnExecutor(THREAD_POOL_EXECUTOR, gson.toJson(message));
    }
}
