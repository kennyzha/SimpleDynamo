package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import com.google.gson.Gson;

import tasks.ClientTask;
import tasks.ServerTask;

public class SimpleDynamoProvider extends ContentProvider {
	private static final int SERVER_PORT = 10000;
	private static final String PREFS_FILE = "kennyzha";
	private static final String ONCREATE_TAG = "onCreate";
	private static final String SERVER_TAG = "Server";

	private ExecutorService executorService = Executors.newCachedThreadPool();
	private Uri providerUri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider");
	private SharedPreferences sharedPref;
	private Dynamo dynamo;
	private Gson gson;
	private String nodeId;	// e.g 55554
	private int portNum;	// e.g 111108
	private BlockingQueue<String> blockingQueue = new ArrayBlockingQueue<String>(100);

	@Override
	public boolean onCreate() {
		try{
			dynamo = new Dynamo();
			gson = new Gson();
			sharedPref = this.getContext().getSharedPreferences(PREFS_FILE, 0);

			TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
			final String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
			final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
			nodeId = portStr;
			portNum = Integer.parseInt(myPort);

			final ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
//			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

			new AsyncTask<ServerSocket, Void, Void>() {
				@Override
				protected Void doInBackground(ServerSocket... params) {
					ServerSocket serverSocket = params[0];
					while(true){
						try {
							Log.e(SERVER_TAG, "Waiting for client on port " + serverSocket.getLocalPort());
							Socket socket = serverSocket.accept();
							Log.e(SERVER_TAG, "Connected to " + socket.getRemoteSocketAddress());

							socket.setSoTimeout(1 * 1000);
							BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
							PrintWriter pw = new PrintWriter(socket.getOutputStream(),true);

							String msg = br.readLine();
							Message receivedMsg = gson.fromJson(msg, Message.class);
							pw.println("acknowledged");

							switch(receivedMsg.getMsgType()){
								case INSERT:
									Log.e(SERVER_TAG, "(Insert) request received. Message received " + msg);
									ContentValues contentValues = new ContentValues();
									contentValues.put("key", receivedMsg.getKey());
									contentValues.put("value", receivedMsg.getValue());

									insert(providerUri, contentValues);
									break;
							}

							br.close();
							pw.close();
							socket.close();
						} catch (IOException e) {
							e.printStackTrace();
							return null;
						}
					}
				}
			}.executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

			String[] hashedPorts = dynamo.getHashedNodes();
			for (int i = 0; i < hashedPorts.length; i++) {
				Log.e(ONCREATE_TAG,"Port " + Dynamo.ports[i] + " Hashed Port " + hashedPorts[i]);
			}
//			int[] prefList = dynamo.getPrefList("key5");

		} catch(IOException e){

		}
		return false;
	}
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		String key = values.getAsString("key");
		String val = values.getAsString("value");
		int[] prefList = dynamo.getPrefList(key);

		if(dynamo.isInCorrectPartition(key, portNum)){
			Log.v("insert", "Storing the key " + key + " in port " + portNum) ;
			SharedPreferences.Editor editor = sharedPref.edit();
			editor.putString(key, val);
			editor.apply(); // commit?

			// forward it to next port or send back a write completed acknowledgement to original requester
		} else {
			// forward it to the first port on prefList
			Log.e("Insert", " Key " + key + " doesn't belongs in port " + portNum + " forwarding to port " + prefList[0]);
			Message msg = new Message(MessageType.INSERT, key, val, prefList, portNum, prefList[0]);
			new ClientTask().executeOnExecutor(executorService, gson.toJson(msg));
		}
		return null;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}
}
