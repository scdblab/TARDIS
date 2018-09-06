package edu.usc.bg.testing;

import java.net.Socket;

import edu.usc.bg.server.RequestHandler;
import edu.usc.bg.server.SocketIO;

public class BGServerShutdownTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//10.0.0.90:55269,10.0.0.90:56269
		String s="10.0.0.90:55269,10.0.0.90:56269";//,10.0.1.70,10.0.1.60,10.0.1.55,10.0.1.50,10.0.1.35,10.0.1.25,10.0.1.15,10.0.1.45,10.0.1.40,10.0.1.20,10.0.1.10,10.0.1.15";
		String tokens[]=s.split(",");
		
		for (int i=0;i<2;i++){
		for (String token: tokens)
		{
			String ip= token.substring(0,token.indexOf(':'));
			int port=Integer.parseInt(token.substring(token.indexOf(':')+1, token.length()));
			System.out.println(ip+":"+port);
			int msg=RequestHandler.SHUTDOWN_SOCKET_POOL_REQUEST;
			if (i==1){
				msg=RequestHandler.FULL_SHUTDOWN_REQUEST;
				
			}
		new KillT(ip,port,msg).start();
		}
		}
	}

} 

class KillT extends Thread
{
	KillT(String i, int p, int msg){
		ip=i;
		port=p;
		message=msg;
	}
	int message;
	int port;
	String ip;
	public void run(){
		try {
			SocketIO socket = new SocketIO (new Socket(ip,port ));
			socket.sendValue(message);
		//	
			sleep(1000);
			socket.closeAll();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(System.out);
		} 
	}
	
}
