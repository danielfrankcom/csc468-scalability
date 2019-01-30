package org.ddjk.workload;

import org.asynchttpclient.AsyncHttpClient;

import static org.asynchttpclient.Dsl.asyncHttpClient;

public class Attacker { 
 
    private static int REQUESTS = 26_000;
    private static final String HOSTNAME = "localhost";
    private static final int PORT = 80;

    private static final String URL = "http://" + HOSTNAME + ":" + PORT;

    public static void main(String args[]) {

        long start = System.nanoTime();
        System.out.println("started");

        AsyncHttpClient asyncHttpClient = asyncHttpClient();

        long finish = System.nanoTime();
        System.out.println("stopped");
        System.out.println("Finished in: "+(finish - start)/1000000 + "ms");

        System.exit(0);
    } 
 
} 
