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
import java.net.UnknownHostException;

import edu.buffalo.cse.cse486586.simpledynamo.Message;

public class ClientTask extends AsyncTask<String, Void, Void>{
    private static final String CLIENT_TAG = "Client";
    Gson gson = new Gson();
    @Override
    protected Void doInBackground(String... params) {
        String msgJson = params[0];
        Message msg = gson.fromJson(msgJson, Message.class);
        try {
            SocketAddress socketAddress = new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    msg.getToPort());
            Socket socket = new Socket();
            socket.connect(socketAddress, 2 * 1000);
            Log.e(CLIENT_TAG, "Client is connected");

            BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
            pw.println(msgJson);

            String response = br.readLine();
//            Gson gson = new Gson();
//            Message msg = gson.fromJson(response, Message.class);

            Log.e(CLIENT_TAG, "Message received from server " + response);

            br.close();
            pw.close();
            socket.close();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
