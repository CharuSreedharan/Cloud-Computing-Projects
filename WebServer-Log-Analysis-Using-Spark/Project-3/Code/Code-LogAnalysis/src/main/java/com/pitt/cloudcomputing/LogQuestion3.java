package com.pitt.cloudcomputing;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class LogQuestion3 {

	public static void findMaxUrlHits() {
        Cluster cluster = null;
        try {
            cluster = Cluster.builder().addContactPoint("159.65.43.106").build();
            Session session = cluster.connect();

            ResultSet resultSet = session.execute("SELECT * from miniproject3.url;");
            String url = "";
            long urlCounter = 0;
            for (Row row : resultSet) {
                long rowCounter = row.getLong("count");
                if (rowCounter > urlCounter) {
                	urlCounter = rowCounter;
                    url = row.getString("url");
                }
            }
            System.out.println("The website "+url + " was hit " + urlCounter+" times.");

        } finally {
            if (cluster != null)
                cluster.close();
        }
    }

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        findMaxUrlHits();
        long endTime = System.currentTimeMillis();
        double total = (endTime - startTime) / 1000.0;
        System.out.println("Total running time in seconds: " + total + "s");
    }

}
