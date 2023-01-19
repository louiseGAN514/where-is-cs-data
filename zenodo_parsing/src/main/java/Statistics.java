import com.google.gson.*;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

public class Statistics {

    public Statistics(String dblpFile, String zenodoFile, String statisticsFile) throws IOException {

        Map<String, DblpInput.OrcidInfo> orcidInfoMap = new HashMap<>();
        Map<String, Set<String>> verifiedMatches = new HashMap<>();
        Map<String, Set<String>> unverifiedMatches = new HashMap<>();

        // Read ORCIDs extracted from DBLP into memory
        try (JsonReader jsonReader = new JsonReader(new BufferedReader(new InputStreamReader(new FileInputStream(dblpFile), StandardCharsets.UTF_8)))) {
            jsonReader.beginObject();
            while (jsonReader.hasNext()) {
                String orcid = jsonReader.nextName();
                JsonObject obj = JsonParser.parseReader(jsonReader).getAsJsonObject();
                String key = obj.getAsJsonPrimitive("key").getAsString();
                boolean verified = obj.getAsJsonPrimitive("verified").getAsBoolean();
                orcidInfoMap.put(orcid, new DblpInput.OrcidInfo(key, verified));
            }
            jsonReader.endObject();
        }

        // Stream Zenodo data from dump
        try (
                JsonReader jsonReader = new JsonReader(new BufferedReader(new InputStreamReader(new FileInputStream(zenodoFile), StandardCharsets.UTF_8)));
                JsonWriter jsonWriter = new JsonWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(statisticsFile), StandardCharsets.UTF_8)))
        ) {
            jsonWriter.setIndent("  ");
            jsonWriter.setHtmlSafe(false);

            jsonReader.beginArray();
            jsonWriter.beginArray();

            while (jsonReader.hasNext()) {
                JsonObject entry = JsonParser.parseReader(jsonReader).getAsJsonObject();

                if (!entry.has("doi") || !entry.has("metadata")) {
                    // Currently, 2 record ids without "doi" exist: 14784 and 16192
                    continue;
                }

                String doi = entry.getAsJsonPrimitive("doi").getAsString();
                String created = entry.getAsJsonPrimitive("created").getAsString();
                JsonObject metadata = entry.getAsJsonObject("metadata");

                // ORCIDs may appear in two positions
                for (String type : List.of("creators", "contributors")) {
                    JsonArray arr = metadata.getAsJsonArray(type);
                    if (arr == null) {
                        continue;
                    }
                    for (int i = 0; i < arr.size(); i++) {
                        JsonObject obj = arr.get(i).getAsJsonObject();

                        // Person has no ORCID in Zenodo
                        if (!obj.has("orcid")) {
                            continue;
                        }

                        String orcid = obj.getAsJsonPrimitive("orcid").getAsString();

                        // ORCID is known to DBLP
                        if (orcidInfoMap.containsKey(orcid)) {
                            DblpInput.OrcidInfo orcidInfo = orcidInfoMap.get(orcid);
                            Set<String> set;
                            if (orcidInfo.verified()) {
                                set = verifiedMatches.computeIfAbsent(doi, x -> new HashSet<>());
                            } else {
                                set = unverifiedMatches.computeIfAbsent(doi, x -> new HashSet<>());
                            }
                            set.add(orcid);
                        }
                    }
                }

                // Output statistics
                jsonWriter.beginObject();
                jsonWriter.name("doi");
                jsonWriter.value(doi);

                jsonWriter.name("timestamp");
                jsonWriter.value(Instant.parse(created).getEpochSecond());

                jsonWriter.name("created");
                jsonWriter.value(created);

                jsonWriter.name("verified");
                jsonWriter.value(verifiedMatches.getOrDefault(doi, Set.of()).size());

                jsonWriter.name("unverified");
                jsonWriter.value(unverifiedMatches.getOrDefault(doi, Set.of()).size());

                jsonWriter.endObject();
            }
            jsonWriter.endArray();
            jsonReader.endArray();
        }

        long totalVerified = orcidInfoMap.entrySet().stream().filter(x -> x.getValue().verified()).count();
        long totalUnverified = orcidInfoMap.entrySet().stream().filter(x -> !x.getValue().verified()).count();

        long totalVerifiedDatasetMatches = verifiedMatches.entrySet().size();
        long totalVerifiedAuthorMatches = verifiedMatches.values().stream().map(Set::size).reduce(0, Integer::sum);

        long totalUnverifiedDatasetMatches = unverifiedMatches.entrySet().size();
        long totalUnverifiedAuthorMatches = unverifiedMatches.values().stream().map(Set::size).reduce(0, Integer::sum);

        System.out.println("Total ORCIDs in dblp with human verification             : " + totalVerified);
        System.out.println("Total ORCIDs in dblp extracted from publication metadata : " + totalUnverified);

        System.out.println("Datasets with at least one author with verified ORCID    : " + totalVerifiedDatasetMatches);
        System.out.println("Total number of authors with verified ORCIDs in datasets : " + totalVerifiedAuthorMatches);

        System.out.println("Datasets with at least one known, but unverified ORCID   : " + totalUnverifiedDatasetMatches);
        System.out.println("Dataset authors with known, but unverified ORCIDs        : " + totalUnverifiedAuthorMatches);
    }
}
