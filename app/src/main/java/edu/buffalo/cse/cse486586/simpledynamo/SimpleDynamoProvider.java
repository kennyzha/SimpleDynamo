package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import com.google.gson.Gson;

import tasks.ClientTask;

public class SimpleDynamoProvider extends ContentProvider {
	private static final int SERVER_PORT = 10000;
	private static final String PREFS_FILE = "kennyzha";
	private static final String ONCREATE_TAG = "onCreate";
	private static final String SERVER_TAG = "Server";
    private static final String QUERY_TAG = " Query";

    private ExecutorService executorService = Executors.newCachedThreadPool();
	private Uri providerUri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider");
	private SharedPreferences sharedPref;
	private Dynamo dynamo = new Dynamo();
	private Gson gson = new Gson();
	private String nodeId;	// e.g "55554"
	private int portNum;	// e.g 111108
	private BlockingQueue<String> blockingQueue = new ArrayBlockingQueue<String>(100);
	private ConcurrentHashMap<String, String> dataLog = new ConcurrentHashMap<String, String>();
    private BlockingQueue<String> deleteBlockingQueue = new ArrayBlockingQueue<String>(100);

	@Override
	public boolean onCreate() {
		try{
			sharedPref = this.getContext().getSharedPreferences(PREFS_FILE, 0);

			TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
			final String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
			final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
			nodeId = portStr;
			portNum = Integer.parseInt(myPort);

			final ServerSocket serverSocket = new ServerSocket(SERVER_PORT);

			new AsyncTask<ServerSocket, Message, Void>() {
				@Override
				protected Void doInBackground(ServerSocket... params) {
					ServerSocket serverSocket = params[0];
					while(true){
						try {
							Log.e(SERVER_TAG, "Waiting for client on port " + serverSocket.getLocalPort());
							Socket socket = serverSocket.accept();
							Log.e(SERVER_TAG, "Connected to " + socket.getRemoteSocketAddress());

							socket.setSoTimeout(1000);
							BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
							PrintWriter pw = new PrintWriter(socket.getOutputStream(),true);

							String msg = br.readLine();
							Message receivedMsg = gson.fromJson(msg, Message.class);
							pw.println("acknowledged");
                            Log.e(SERVER_TAG, receivedMsg.getMsgType() + " msgtype) received. Message received " + msg);
                            switch(receivedMsg.getMsgType()){
								case INSERT:
                                    dynamo.insertToSharedPref(sharedPref, receivedMsg);
                                    // forward it to next port or send back a write completed acknowledgement to original requester
                                    if(dynamo.writeFinished(receivedMsg.getPrefList(), portNum)){
                                        //send back insert completed acknowledgement
                                        Log.e(SERVER_TAG, "Insert write finished. Sending back Insert Response from the current port " + portNum);
                                        receivedMsg.setMsgType(MessageType.INSERT_RESPONSE);
                                        receivedMsg.setToPort(receivedMsg.getFromPort());
                                    } else{
                                        int nextPort = dynamo.getNextNodePort(receivedMsg.getPrefList(), portNum);
                                        receivedMsg.setToPort(nextPort);
                                        Log.e(SERVER_TAG, "(onProgressUpdate) write is not finished. Sending to next port on preference list which is port " + nextPort);
                                    }
									publishProgress(receivedMsg);
									break;
								case INSERT_RESPONSE:
									break;
                                case DELETE:
                                    dynamo.deleteFromSharedPref(sharedPref, receivedMsg.getKey());
                                    if(dynamo.writeFinished(receivedMsg.getPrefList(), portNum)){
                                        Log.e(SERVER_TAG, "Delete write finished. Sending back Delete Response from the current port " + portNum);
                                        receivedMsg.setMsgType(MessageType.DELETE_RESPONSE);
                                        receivedMsg.setToPort(receivedMsg.getFromPort());
                                    } else {
                                        int nextPort = dynamo.getNextNodePort(receivedMsg.getPrefList(), portNum);
                                        receivedMsg.setToPort(nextPort);
                                        Log.e(SERVER_TAG, "Delete write is not finished. Sending to next port on preference list which is port " + nextPort);
                                    }
                                    publishProgress(receivedMsg);
                                    break;
                                case DELETE_RESPONSE:
                                    Log.e(SERVER_TAG, "Placing msg in blocking queue " + msg);
                                    deleteBlockingQueue.put(msg);
                                    break;
                                case QUERY:
                                    String receivedKey = receivedMsg.getKey();
                                    if(receivedKey.equals("@")){
                                        Log.e(SERVER_TAG, "REMOTE @@@@@ (Query msgtype) received. Queried key: " + receivedMsg.getKey());

                                        HashMap<String, String> hm = dynamo.query(sharedPref, receivedKey);
                                        String allKeys = "";
                                        String allValues = "";

                                        for(Map.Entry entry : hm.entrySet()){
                                            allKeys = allKeys + entry.getKey() + ":::";
                                            allValues = allValues + entry.getValue() + ":::";
                                        }
                                        Log.e(SERVER_TAG, " All Keys: " + allKeys + " All value: " + allValues);

                                        receivedMsg.setKey(allKeys);
                                        receivedMsg.setValue(allValues);
                                    } else{
                                        String value = querySharedPref(receivedMsg.getKey());
                                        receivedMsg.setValue(value);
                                        Log.e(SERVER_TAG, "REMOTE (Query msgtype) received. Queried key: " + receivedMsg.getKey() + " value: " + value);

                                    }
                                    receivedMsg.setMsgType(MessageType.QUERY_RESPONSE);
                                    receivedMsg.setToPort(receivedMsg.getFromPort());
                                    publishProgress(receivedMsg);
                                    break;
                                case QUERY_RESPONSE:
                                    blockingQueue.put(msg);
                                    Log.e(SERVER_TAG, "(QueryRESPONSE msgtype) received. Placed message into blocking queue: " + msg);
                                    break;
							}
							br.close();
							pw.close();
							socket.close();
						} catch (IOException e) {
							e.printStackTrace();
							return null;
						} catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
				}
				@Override
				protected void onProgressUpdate(Message... values) {
					new ClientTask().executeOnExecutor(executorService, gson.toJson(values[0]));
				}
			}.executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

			String[] hashedPorts = dynamo.getHashedNodes();
			for (int i = 0; i < hashedPorts.length; i++) {
				Log.e(ONCREATE_TAG,"Port " + Dynamo.PORTS[i] + " Hashed Port " + hashedPorts[i]);
			}
		} catch(IOException e){
            e.printStackTrace();
		}
		return false;
	}
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
        if(selection.equals("@") || selection.equals("*")){
            SharedPreferences.Editor editor = sharedPref.edit();
            editor.clear();
            editor.apply();

            if(selection.equals("*")){
                for(int port : Dynamo.PORTS){
                    if(portNum != port){
                        Message msg = new Message(MessageType.DELETE, "@", null, null, portNum, port );
                        new ClientTask().executeOnExecutor(executorService, gson.toJson(msg));
                    }
                }
            }
        } else{
            int[] prefList = dynamo.getPrefList(selection);
            Log.e("Delete", " Forwarding Key " + selection + " from port " + portNum + " to port " + prefList[0]);
            Message msg = new Message(MessageType.DELETE, selection, null, prefList, portNum, prefList[0]);
            new ClientTask().executeOnExecutor(executorService, gson.toJson(msg));
            try {
                deleteBlockingQueue.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	// Forward it to the first port on prefList. Insert is only called by outside applications
	@Override
	public Uri insert(Uri uri, ContentValues values) {
		String key = values.getAsString("key");
		String val = values.getAsString("value");
		int[] prefList = dynamo.getPrefList(key);

		Log.e("Insert", " Forwarding Key " + key + " from port " + portNum + " to port " + prefList[0]);
		Message msg = new Message(MessageType.INSERT, key, val, prefList, portNum, prefList[0]);
		new ClientTask().executeOnExecutor(executorService, gson.toJson(msg));

		return uri;
	}

	// @ - returns all <key, value> pairs stored in your local partition of the node
	// * - returns all <key, value> pairs stored in your entire DHT
	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
		MatrixCursor mc = new MatrixCursor(new String[]{"key","value"});
		String jsonResponse;
        try {
            if(selection.equals("@") || selection.equals("*")){
                Map<String, ?> allEntries = sharedPref.getAll();

                for (Map.Entry<String, ?> entry : allEntries.entrySet()) {
                    mc.addRow(new String[]{entry.getKey(), entry.getValue().toString()});
                }
                if(selection.equals("*")){
                    for(int port : Dynamo.PORTS){
                        if(portNum != port){
                            Message msg = new Message(MessageType.QUERY, "@", "placeholder", null, portNum, port );
                            new ClientTask().executeOnExecutor(executorService, gson.toJson(msg));
                        }
                    }
                    for(int i = 0; i < Dynamo.PORTS.length - 1; i++){
                        jsonResponse = blockingQueue.take();
                        Message msg = gson.fromJson(jsonResponse, Message.class);
                        String[] allKeys = msg.getKey().split(":::");
                        String[] allValues = msg.getValue().split(":::");
                        Log.v(QUERY_TAG, " ******ALL KEYS " + Arrays.toString(allKeys) + " ALL VALUES " + Arrays.toString(allValues));
                        for(int k = 0; k < allKeys.length; k++){
                            mc.addRow(new String[]{allKeys[k], allValues[k] });
                        }
                    }
                }
            } else{
                    int[] prefLst = dynamo.getPrefList(selection);
                    Message msg = new Message(MessageType.QUERY, selection, "placeholder", prefLst, portNum, prefLst[prefLst.length - 1]);
                    new ClientTask().executeOnExecutor(executorService, gson.toJson(msg, Message.class));

                    jsonResponse = blockingQueue.take();
                    Log.v(QUERY_TAG, "Queried key " + selection + " from port " + prefLst[prefLst.length-1] + " and received response: " + jsonResponse );

                    Message responseMsg = gson.fromJson(jsonResponse, Message.class);
                    mc.addRow(new String[]{selection, responseMsg.getValue()});

            }
        } catch (InterruptedException e) {
        e.printStackTrace();
        mc.addRow(new String[]{selection, "Interrupted exception in query."});
    }
		return mc;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		return 0;
	}

    public String querySharedPref(String key){
        return sharedPref.getString(key, "KEY NOT FOUND");
    }
}
