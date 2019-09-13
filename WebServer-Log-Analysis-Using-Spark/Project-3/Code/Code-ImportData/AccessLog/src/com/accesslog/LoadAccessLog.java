package com.accesslog;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

public class LoadAccessLog {
	public BufferedReader LoadFile (String filepath){
		File accessLogFile = new File(filepath);
		FileReader accessLogFileReader = null;
		try {
			 accessLogFileReader = new FileReader(accessLogFile);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		BufferedReader accessLogBufferReader = new BufferedReader (accessLogFileReader);
		return accessLogBufferReader;
	}

	public Session CreateKeySpaceTable() {
		//Create Cassandra cluster and connection
		Cluster accessLogCluster = null;
		accessLogCluster = Cluster.builder().addContactPoint("159.65.43.106").build();
		Session currentSession = accessLogCluster.connect();
		
		//Setup the Keyspace and the tables
        currentSession.execute("DROP KEYSPACE IF EXISTS MiniProject3;"); //Delete old keyspace
        currentSession.execute("DROP TABLE IF EXISTS MiniProject3.FullLog;"); //Delete full log table
        currentSession.execute("DROP TABLE IF EXISTS MiniProject3.ip;"); //Delete ip table
        currentSession.execute("DROP TABLE IF EXISTS MiniProject3.url;"); //Delete url table
        
        currentSession.execute("CREATE KEYSPACE MiniProject3 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};");
        
        currentSession.execute(
                "CREATE TABLE MiniProject3.FullLog (id int, ip text, identity text, username text, time text, method text, url text, protocol text, status int, size int, PRIMARY KEY ((id), ip, url));");
        //currentSession.execute("CREATE INDEX id_index ON MiniProject3.FullLog (id);");
        currentSession.execute("CREATE INDEX ip_index ON MiniProject3.FullLog (ip);");
        currentSession.execute("CREATE INDEX url_index ON MiniProject3.FullLog (url);");
        currentSession.execute("CREATE TABLE MiniProject3.ip (ip text PRIMARY KEY, count counter);");
        currentSession.execute("CREATE TABLE MiniProject3.url (url text PRIMARY KEY, count counter);");
        return currentSession;
	}
	
	public void InsertValuesToTables(Session curSession, BufferedReader buffRead) {
		//To run 256 cassandra queries in parallel (asynchronously) 
		final Semaphore CQLAsync = new Semaphore(256);
		PreparedStatement fullLogPrepare = curSession
                .prepare("INSERT INTO MiniProject3.FullLog (id, ip, identity, username, time, method, url, protocol, status, size) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        PreparedStatement ipPrepare = curSession.prepare("UPDATE MiniProject3.ip SET count = count + 1 WHERE ip = ?");
        PreparedStatement urlPrepare = curSession.prepare("UPDATE MiniProject3.url SET count = count + 1 WHERE url = ?");
        int count = 1;
        try {
			while (buffRead.ready()) {
				//For 3 prepare statements
				CQLAsync.acquire();
				CQLAsync.acquire();
				CQLAsync.acquire();
				//Read access_log file line by line
			    String line = buffRead.readLine();
			    Pattern regularExpression = Pattern.compile("([^ ]*) ([^ ]*) (.*) \\[(.*)\\] \"([^ ]*) ([^ ]*) *([^ ]*)\" ([^ ]*) ([^ ]*)");
			    Matcher regularExpressionFind = regularExpression.matcher(line);
			    if (regularExpressionFind.find()) {
			        String ip = regularExpressionFind.group(1);
			        String identity = regularExpressionFind.group(2).equals("-") ? null : regularExpressionFind.group(2);
			        String username = regularExpressionFind.group(3).equals("-") ? null : regularExpressionFind.group(3);
			        String time = regularExpressionFind.group(4);
			        String method = regularExpressionFind.group(5);
			        String url = regularExpressionFind.group(6);
			        String protocol = regularExpressionFind.group(7);
			        Integer status = Integer.valueOf(regularExpressionFind.group(8));
			        Integer size = regularExpressionFind.group(9).equals("-") ? null : Integer.valueOf(regularExpressionFind.group(9));
			        
			        BoundStatement fullLogBind = fullLogPrepare.bind(count, ip, identity, username, time, method, url, protocol, status, size);
			        BoundStatement ipBind = ipPrepare.bind(ip);
			        BoundStatement urlBind = urlPrepare.bind(url);
			        count++;

			        ResultSetFuture resultSetFuture = curSession.executeAsync(fullLogBind);
			        ResultSetFuture ipFuture = curSession.executeAsync(ipBind);
			        ResultSetFuture urlFuture = curSession.executeAsync(urlBind);

			        Futures.addCallback(resultSetFuture, new FutureCallback<ResultSet>() {
			            public void onSuccess(ResultSet resultSet) {
			            	CQLAsync.release();
			            }

			            public void onFailure(Throwable throwable) {
			            	CQLAsync.release();
			                System.out.println("Error: " + throwable.toString());
			            }
			        });

			        Futures.addCallback(ipFuture, new FutureCallback<ResultSet>() {
			            public void onSuccess(ResultSet resultSet) {
			                CQLAsync.release();
			            }

			            public void onFailure(Throwable throwable) {
			            	CQLAsync.release();
			                System.out.println("Error: " + throwable.toString());
			            }
			        });

			        Futures.addCallback(urlFuture, new FutureCallback<ResultSet>() {
			            public void onSuccess(ResultSet resultSet) {
			            	CQLAsync.release();
			            }

			            public void onFailure(Throwable throwable) {
			            	CQLAsync.release();
			                System.out.println("Error: " + throwable.toString());
			            }
			        });
			    }
			    if ((count - 1) % 20000 == 0) {
			        System.out.println("inserted " + (count - 1));
			    }
			}
		} catch (NumberFormatException | IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        System.out.println("inserted " + (count - 1));

        try {
			buffRead.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main (String[] args) {
		long startTime = System.currentTimeMillis();
		LoadAccessLog MyObj = new LoadAccessLog();
		BufferedReader MyBufferReader = MyObj.LoadFile("access_log");
		System.out.println(MyBufferReader);
		Session MySession = MyObj.CreateKeySpaceTable();
		MyObj.InsertValuesToTables(MySession, MyBufferReader);
		long endTime = System.currentTimeMillis();
		long totalTime = (endTime - startTime) / 1000;
		System.out.println("Total run time to load webserver log: " + totalTime + " seconds");
	}
}

