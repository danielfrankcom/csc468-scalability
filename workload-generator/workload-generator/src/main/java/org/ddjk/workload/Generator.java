package org.ddjk.workload;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Future;


public class Generator {

    private static final int NS_IN_MS = 1_000_000;
    private static final int MS_IN_S = 1_000;

    private static final String HOSTNAME = "transaction-server";
    private static final int PORT = 5000;

    private static final String URL = "http://" + HOSTNAME + ":" + PORT;

    public static void main(String args[]) {

        if (args.length != 1) {
            System.out.println("Only parameter should be workload filename.");
            System.exit(1);
        }

        final String[] tokenized = args[0].split("/");
        final String filename = tokenized[tokenized.length - 1];
        final String path = "/workloads/" + filename;

        final File workload = new File(path);
        if (!workload.exists()) {
            System.out.printf("Invalid workload %s provided.\n", path);
            System.exit(1);
        }


        final List<String> lines = new LinkedList<>();
        try {
            final FileReader fileReader = new FileReader(workload);
            try (BufferedReader reader = new BufferedReader(fileReader)) {

                while (true) {

                    final String line = reader.readLine();
                    if (line == null) {
                        break;
                    }

                    // Strip off line number from start of command.
                    final String stripped = line.split(" ")[1];
                    lines.add(stripped);
                }
            }
        } catch (Throwable e){
            e.printStackTrace();
            System.exit(1);
        }


        final Iterator<String> iterator = lines.iterator();
        final int numRequests = lines.size();
        final Request[] requests = new Request[numRequests];

        for (int i = 0; i < numRequests; i++) {
            requests[i] = Dsl.post(URL)
                    .setBody(iterator.next())
                    .build();
        }

        final AsyncHttpClient client = Dsl.asyncHttpClient();

        System.out.println("started");

        final long durationNS = execute(client, requests);
        final long durationMS = durationNS / NS_IN_MS;
        final float durationS = durationMS / (float) MS_IN_S;

        System.out.printf("Finished in: %fs.\n", durationS);

        System.exit(0);
    }

    private static long execute(AsyncHttpClient client, Request... requests) {
        final long start = System.nanoTime();

        final List<Future<Response>> responses = new ArrayList<>(requests.length);
        for (Request request : requests) {
            Future<Response> response = client.executeRequest(request);

            final Response actual;
            try {
                actual = response.get();
            } catch (Throwable e) {
                e.printStackTrace();
                System.out.println("Exception occurred while parsing response");
                break;
            }

            final int statusCode = actual.getStatusCode();
            if (statusCode != 200) {
                System.out.printf("Request returned status code %d.\n", statusCode);
            }
        }

        final long finish = System.nanoTime();
        return finish - start;

    }

} 
