package tasks;

import android.os.AsyncTask;
import android.util.Log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;

public class ServerTask extends AsyncTask<ServerSocket, BlockingQueue<String>, Void> {
    private static final String SERVER_TAG = "Server";

    @Override
    protected Void doInBackground(ServerSocket... params) {
        ServerSocket serverSocket = params[0];
        while(true){
            try {
                Log.e(SERVER_TAG, "Waiting for client on port " + serverSocket.getLocalPort());
                Socket socket = serverSocket.accept();
                Log.e(SERVER_TAG, "Connected to " + socket.getRemoteSocketAddress());

                socket.setSoTimeout(2 * 1000);
                BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter pw = new PrintWriter(socket.getOutputStream(),true);

                String msg = br.readLine();
                pw.println("acknowledged");

                Log.e(SERVER_TAG, "Message received " + msg);

                br.close();
                pw.close();
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }
    }

    @Override
    protected void onProgressUpdate(BlockingQueue<String>... values) {
        super.onProgressUpdate(values);
    }

}
