import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonWriter;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ZenodoRequest {

    private String queryApi(HttpClient client, String apiCall) throws IOException, InterruptedException {

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(apiCall))
                .GET()
                .build();

        while (true) {
            // Send query and get result
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            Map<String, List<String>> headers = response.headers().map();
            int code = response.statusCode();

            // Get information about rate limits
            int limit = Integer.parseInt(headers.get("x-ratelimit-limit").get(0));
            int remaining = Integer.parseInt(headers.get("x-ratelimit-remaining").get(0));
            long resetTime = Long.parseLong(headers.get("x-ratelimit-reset").get(0));

            System.out.printf("[Rate limit %d/%d]%n", limit - remaining, limit);

            Instant unixTime = Instant.ofEpochSecond(resetTime);
            if (code == 429) {
                // Rate limit reached
                System.out.printf("*** [Sleeping until %s] ***%n", Date.from(unixTime));

                final Object obj = new Object();
                ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();

                synchronized (obj) {
                    service.schedule(() -> {
                        synchronized (obj) {
                            System.out.println("*** [Resuming excution] ***");
                            obj.notifyAll();
                        }
                    }, resetTime - Instant.now().getEpochSecond(), TimeUnit.SECONDS);
                    obj.wait();
                }
                // Try again
                continue;
            }
            return response.body();
        }
    }

    public ZenodoRequest(String jsonFile) throws IOException, InterruptedException {
        HttpClient client = HttpClient.newBuilder().build();

        // Zenodo's first publication is from 2014
        Instant start = LocalDateTime.of(2014, 1, 1, 0, 0, 0).toInstant(ZoneOffset.UTC);
        Instant maxYear = LocalDateTime.of(2024, 1, 1, 0, 0, 0).toInstant(ZoneOffset.UTC);
        DateTimeFormatter dtf = DateTimeFormatter.ISO_INSTANT;

        // Approximately 0.94 years, chosen because it is a power of 2 and can be bisected easily
        int hours = 8192;

        // Results are unsorted, store them for later
        Map<Integer, JsonObject> allValues = new HashMap<>();

        long totalCount = 0;

        while (true) {
            Instant end = start.plus(hours, ChronoUnit.HOURS);
            System.out.printf("Processing records [%s TO %s]%n", dtf.format(start), dtf.format(end));

            // Limit time frame so we do not request much more than 10000 results
            String query = String.format(Locale.ROOT, "created:[%s TO %s]", dtf.format(start), dtf.format(end));

            // Limit result size to 1 here since we are only interested in the total number of records
            String[][] paramsArray = {
                    {"q", query},
                    {"size", "1"},
                    {"all_versions", "false"},
                    {"type", "dataset"}
            };

            StringJoiner params = new StringJoiner("&");

            for (String[] param : paramsArray) {
                params.add(URLEncoder.encode(param[0], StandardCharsets.UTF_8) + "=" + URLEncoder.encode(param[1], StandardCharsets.UTF_8));
            }

            String apiCall = "https://zenodo.org/api/records/?" + params;

            // Query Zenodo API, returns JSON object as result
            String response = queryApi(client, apiCall);

            long maxRecords;
            long recordCounter = 0;

            JsonObject obj = JsonParser.parseString(response).getAsJsonObject();

            if (obj.has("hits")) {
                maxRecords = obj.getAsJsonObject("hits").getAsJsonPrimitive("total").getAsLong();
                // Zenodo only allows up to 10000 results in a single query and up to 1000 on a single page of results
                if (maxRecords > 10_000) {
                    System.out.printf("Found %d results, reducing interval%n", maxRecords);
                    hours /= 2;
                    continue;
                }

                // Stop if we have reached our end date
                if (start.isAfter(maxYear)) {
                    break;
                }

                // If 0 records were returned, it just means our selected time interval was empty
                // Start one hour before "end" the next time to retrieve records created on a full hour
                if (maxRecords == 0) {
                    start = end.minus(1, ChronoUnit.HOURS);
                    hours = 8192;
                    continue;
                }

            } else {
                // We cannot continue if we do not know how many records exist
                System.err.println("No 'total' value!");
                return;
            }

            hours = 8192;

            // 1000 results per page is the maximum supported by the Zenodo API
            paramsArray = new String[][]{
                    {"q", query},
                    {"size", "1000"},
                    {"all_versions", "false"},
                    {"type", "dataset"}
            };

            params = new StringJoiner("&");

            for (String[] param : paramsArray) {
                params.add(URLEncoder.encode(param[0], StandardCharsets.UTF_8) + "=" + URLEncoder.encode(param[1], StandardCharsets.UTF_8));
            }

            apiCall = "https://zenodo.org/api/records/?" + params;

            while (recordCounter != maxRecords) {
                response = queryApi(client, apiCall);

                JsonObject root = JsonParser.parseString(response).getAsJsonObject();

                // Parse JSON response
                if (root.has("hits")) {
                    JsonObject hits = root.getAsJsonObject("hits");
                    if (hits.has("hits")) {
                        // Get total number of results for the query
                        JsonArray contents = hits.getAsJsonArray("hits");
                        for (int i = 0; i < contents.size(); i++) {
                            JsonObject element = contents.get(i).getAsJsonObject();
                            // "id" is a unique value and exists in every record
                            int id = element.getAsJsonPrimitive("id").getAsInt();
                            if (!allValues.containsKey(id)) {
                                allValues.put(id, element);
                            } else {
                                // Record might already exist since consecutive timeframes overlap by one hour
                                System.err.println("Skipping duplicate element: " + id);
                            }
                            recordCounter++;
                            totalCount++;
                        }
                    }

                    System.out.printf(Locale.ROOT, "Processed %d/%d (total %d)%n", recordCounter, maxRecords, totalCount);

                    // Extract "next" link which contains the URL to request the next page of results
                    // If link does not exist, then either 10000 results have been returned or there are less than 10000 results in total
                    if (root.has("links")) {
                        JsonObject links = root.getAsJsonObject("links");
                        if (links.has("next")) {
                            apiCall = links.getAsJsonPrimitive("next").getAsString();
                        } else {
                            break;
                        }
                    }
                }
            }
            System.out.printf(Locale.ROOT, "Processed %d/%d (total %d) - Finished%n", recordCounter, maxRecords, totalCount);

            // Overlap consecutive timeframes by one hour
            start = end.minus(1, ChronoUnit.HOURS);
        }

        System.out.printf("*** [Writing sorted output to file: '%s'] ***%n", jsonFile);

        try (JsonWriter jsonWriter = new JsonWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(jsonFile), StandardCharsets.UTF_8)))) {
            Gson gson = new Gson();
            jsonWriter.setIndent("  ");
            jsonWriter.setHtmlSafe(false);
            jsonWriter.beginArray();

            allValues.values()
                    .stream()
                    .sorted((o1, o2) -> {
                        String date1 = o1.getAsJsonPrimitive("created").getAsString();
                        String date2 = o2.getAsJsonPrimitive("created").getAsString();
                        Instant i1 = Instant.parse(date1);
                        Instant i2 = Instant.parse(date2);
                        return i1.compareTo(i2);
                    })
                    .forEach(record -> gson.toJson(record, jsonWriter));

            jsonWriter.endArray();
        }

        System.out.println("*** [Done] ***");
    }
}
