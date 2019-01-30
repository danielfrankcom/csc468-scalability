package org.ddjk.workload;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;

import java.util.concurrent.Future;


public class Generator {

    private static final int NS_IN_MS = 1_000_000;
    private static final int MS_IN_S = 1_000;

    private static final int NUM_REQUESTS = 26_000;
    private static final String HOSTNAME = "localhost";
    private static final int PORT = 80;

    private static final String URL = "http://" + HOSTNAME + ":" + PORT;

    public static void main(String args[]) {

        final AsyncHttpClient client = Dsl.asyncHttpClient();

        final Request[] requests = new Request[NUM_REQUESTS];

        for (int i = 0; i < NUM_REQUESTS; i++) {
            requests[i] = Dsl.get(URL).build();
        }

        System.out.println("started");

        final long durationNS = execute(client, requests);
        final long durationMS = durationNS / NS_IN_MS;
        final float durationS = durationMS / (float) MS_IN_S;

        final String output = "Finished in: " + durationS + "s.";
        System.out.println(output);

        System.exit(0);
    }

    private static long execute(AsyncHttpClient client, Request... requests) {
        long start = System.nanoTime();

        for (Request request : requests) {
            Future<Response> response = client.executeRequest(request);
        }

        long finish = System.nanoTime();
        return finish - start;

    }
 
} 
