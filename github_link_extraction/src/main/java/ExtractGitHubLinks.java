import com.google.gson.stream.JsonWriter;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExtractGitHubLinks {

    // STEP 1: download unarXiv dataset (Version 4)
    // https://doi.org/10.5281/zenodo.4313164

    // STEP 2: unpack *.tar.bz2 archive to some directory and adjust path here
    private static final Path UNARXIV_PATH = Paths.get("D:/unarXive-2020/");

/*
=====
This file contains the compressed intermediate results used in our research:
=====
cs.json.gz
=====
*/

    public static void main(String[] args) {
        Map<String, String> dateMap = new LinkedHashMap<>();
        Map<String, Set<String>> linkMap = new HashMap<>();

        System.out.println("Reading SQLite database...");

        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + UNARXIV_PATH.resolve(Paths.get("database", "refs.db")))) {
            Statement statement = connection.createStatement();

            // Only look at discipline "computer science"
            ResultSet rs = statement.executeQuery("SELECT arxiv_id, date FROM arxivmetadata WHERE discipline = 'cs' ORDER BY date ASC");
            while (rs.next()) {
                dateMap.put(rs.getString(1), rs.getString(2));
            }
            rs.close();

            rs = statement.executeQuery("SELECT uuid, link FROM bibitemlinkmap");
            while (rs.next()) {
                linkMap.computeIfAbsent(rs.getString(1), x -> new HashSet<>()).add(rs.getString(2));
            }
            rs.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        System.out.println("Finished reading database");
        System.out.println("arXiv-ID -> date pairs: " + dateMap.size());
        System.out.println("UUID -> link pairs: " + linkMap.size());

        int counter = 0;
        int counterWithLinks = 0;
        int gitCounter = 0;
        int gitUuidCounter = 0;
        int notFound = 0;
        int tooSmall = 0;

        // UUID reference pattern
        Pattern citePattern = Pattern.compile("\\{\\{cite:([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})}}");

        // GitHub URL reference pattern
        Pattern gitPattern = Pattern.compile("(https?://\\S*?github\\.(com|io)/[\\w.-]*?\\w/[\\w.-]*?\\w)(/.*?)?[\\s\u00A0.;,]");

        LinkedList<Integer> gitWindow = new LinkedList<>();
        LinkedList<Integer> gitUuidWindow = new LinkedList<>();

        long gitWindowTotalArticles;
        long gitWindowTotalReferences;

        long gitUuidWindowTotalArticles;
        long gitUuidWindowTotalReferences;

        String lastDate = "<invalid>";

        try (JsonWriter jsonWriter = new JsonWriter(new FileWriter("cs.json"))) {
            jsonWriter.setIndent("  ");
            jsonWriter.beginObject();

            for (Map.Entry<String, String> entry : dateMap.entrySet()) {
                Set<String> uuidSet = new HashSet<>();
                Set<String> gitSet = new HashSet<>();

                try {
                    Path path = UNARXIV_PATH.resolve(Paths.get("papers", entry.getKey() + ".txt"));

                    // If not found in main directory, file might exist in sub-directory
                    if (!Files.exists(path)) {
                        path = UNARXIV_PATH.resolve(Paths.get("papers", "no_cit", entry.getKey() + ".txt"));
                        if (!Files.exists(path)) {
                            notFound++;
                            continue;
                        }
                    }

                    // Skip garbage files
                    if (Files.size(path) <= 100) {
                        tooSmall++;
                        continue;
                    }

                    int gitWindowCounter = 0;
                    int gitUuidWindowCounter = 0;

                    counter++;

                    String content = Files.readString(path);
                    Matcher citeMatcher = citePattern.matcher(content);
                    Matcher gitMatcher = gitPattern.matcher(content);

                    // Find UUIDs
                    while (citeMatcher.find()) {
                        Set<String> links = linkMap.get(citeMatcher.group(1));
                        if (links != null) {
                            for (String link : links) {
                                Matcher m = gitPattern.matcher(link);
                                if (m.find()) {
                                    gitUuidCounter++;
                                    gitUuidWindowCounter++;
                                    uuidSet.add(m.group(1));
                                }
                            }
                        }
                    }

                    // Find GitHub URLs
                    while (gitMatcher.find()) {
                        gitCounter++;
                        gitWindowCounter++;
                        gitSet.add(gitMatcher.group(1));
                    }

                    gitWindow.add(gitWindowCounter);
                    while (gitWindow.size() > 10_000) {
                        gitWindow.removeFirst();
                    }

                    gitUuidWindow.add(gitUuidWindowCounter);
                    while (gitUuidWindow.size() > 10_000) {
                        gitUuidWindow.removeFirst();
                    }

                    gitWindowTotalArticles = gitWindow.stream().filter(val -> val != 0).count();
                    gitWindowTotalReferences = gitWindow.stream().reduce(0, Integer::sum);

                    gitUuidWindowTotalArticles = gitUuidWindow.stream().filter(val -> val != 0).count();
                    gitUuidWindowTotalReferences = gitUuidWindow.stream().reduce(0, Integer::sum);

                    lastDate = entry.getValue();

                    if (counter % 10_000 == 0) {
                        System.out.printf("========== %d ========== (git: %d+%d, notFound: %d, skipped: %d, matches: %d/%d) [10k-Window: git(%d in %d)+(%d in %d), most recent: %s]%n",
                                counter, gitCounter, gitUuidCounter, notFound, tooSmall, counterWithLinks, counter,
                                gitWindowTotalReferences, gitWindowTotalArticles, gitUuidWindowTotalReferences, gitUuidWindowTotalArticles, lastDate);
                    }

                    jsonWriter.name(entry.getKey());
                    jsonWriter.beginObject();

                    jsonWriter.name("date");
                    jsonWriter.value(lastDate);

                    if (uuidSet.size() > 0 || gitSet.size() > 0) {
                        counterWithLinks++;

                        jsonWriter.name("uuid");
                        jsonWriter.beginArray();
                        for (String s : uuidSet) {
                            jsonWriter.value(s);
                        }
                        jsonWriter.endArray();

                        jsonWriter.name("regex");
                        jsonWriter.beginArray();
                        for (String s : gitSet) {
                            jsonWriter.value(s);
                        }
                        jsonWriter.endArray();
                    }
                    jsonWriter.endObject();

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            jsonWriter.endObject();

            gitWindowTotalArticles = gitWindow.stream().filter(val -> val != 0).count();
            gitWindowTotalReferences = gitWindow.stream().reduce(0, Integer::sum);

            gitUuidWindowTotalArticles = gitUuidWindow.stream().filter(val -> val != 0).count();
            gitUuidWindowTotalReferences = gitUuidWindow.stream().reduce(0, Integer::sum);

            System.out.printf("========== %d ========== (git: %d+%d, notFound: %d, skipped: %d, matches: %d/%d) [10k-Window: git(%d in %d)+(%d in %d), most recent: %s]%n",
                    counter, gitCounter, gitUuidCounter, notFound, tooSmall, counterWithLinks, counter,
                    gitWindowTotalReferences, gitWindowTotalArticles, gitUuidWindowTotalReferences, gitUuidWindowTotalArticles, lastDate);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
