import com.google.gson.stream.JsonWriter;
import me.tongfei.progressbar.ConsoleProgressBarConsumer;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import me.tongfei.progressbar.ProgressBarStyle;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.zip.GZIPInputStream;


public class DblpInput {

    record Person(List<String> names, Set<String> verifiedOrcids, Set<String> orcidsFromPublications) {
    }

    record OrcidInfo(String key, boolean verified) {
    }

    private static final Pattern orcidPattern = Pattern.compile("https://orcid.org/([0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9X])");

    private static final int MAX_CONSOLE_WIDTH = 80;

    private final Map<String, Person> personMap = new HashMap<>();
    private final Map<String, Set<String>> personOrcidsMap = new HashMap<>();
    private final Map<String, String> nameToKeyMap = new HashMap<>();
    private final Map<String, OrcidInfo> orcidMap = new HashMap<>();

    private int personCounter = 0;
    private int publicationCounter = 0;

    public DblpInput(String dblpFile, String jsonFile) throws ParserConfigurationException, SAXException, IOException {

        System.setProperty("entityExpansionLimit", "2500000");

        SAXParserFactory factory = SAXParserFactory.newInstance();
        factory.setValidating(true);
        SAXParser parser = factory.newSAXParser();

        // Only look at "publication" and "person" records
        Set<String> tags = Set.of("book", "article", "phdthesis", "inproceedings", "www", "incollection", "proceedings", "mastersthesis");

        CustomSAXParser saxParser = new CustomSAXParser(tags, this::process);

        ProgressBarBuilder pbb = new ProgressBarBuilder()
                .setConsumer(new ConsoleProgressBarConsumer(System.out, MAX_CONSOLE_WIDTH))
                .continuousUpdate()
                .setUnit("MB", 1_048_576)
                .setStyle(ProgressBarStyle.ASCII);

        // If XML input file is still compressed, transparently uncompress it
        InputStream is = dblpFile.endsWith(".xml.gz")
                ? new GZIPInputStream(ProgressBar.wrap(new FileInputStream(dblpFile), pbb))
                : ProgressBar.wrap(new FileInputStream(dblpFile), pbb);

        // Parser XML
        parser.parse(is, saxParser);

        // Create a list of verified and unverified ORCIDs
        collectOrcids();

        // Store results in output file
        outputJson(jsonFile);

        System.out.println("Total number of persons in dblp      : " + personCounter);
        System.out.println("Total number of publications in dblp : " + publicationCounter);
    }

    private void collectOrcids() {
        // The order of publications and persons in dblp is random
        // Assigning ORCIDs to person keys needs to be delayed until whole XML file is parsed
        personOrcidsMap.forEach((name, orcid) -> {
            if (nameToKeyMap.containsKey(name)) {
                String personKey = nameToKeyMap.get(name);
                if (personMap.containsKey(personKey)) {
                    personMap.get(personKey).orcidsFromPublications().addAll(orcid);
                    return;
                }
            }
            System.err.printf("No dblpKey found for '%s' with ORCID '%s'\n", name, orcid);
        });

        // Verified ORCIDs exist in person profiles and should only appear once
        personMap.forEach((key, person) -> {
            for (String orcid : person.verifiedOrcids) {
                OrcidInfo orcidInfo = new OrcidInfo(key, true);
                if (!orcidMap.containsKey(orcid)) {
                    orcidMap.put(orcid, orcidInfo);
                } else {
                    System.err.println("Verified ORCID duplicate!");
                }
            }
        });

        // Unverified ORCIDs exist in publication metadata
        // If the ORCID is already known to be verified, skip entry
        personMap.forEach((key, person) -> {
            for (String orcid : person.orcidsFromPublications()) {
                OrcidInfo orcidInfo = new OrcidInfo(key, false);
                if (!orcidMap.containsKey(orcid)) {
                    orcidMap.put(orcid, orcidInfo);
                }
            }
        });

        // Free unused memory
        personMap.clear();
        personOrcidsMap.clear();
        nameToKeyMap.clear();
    }

    private void outputJson(String jsonFile) throws IOException {

        try (JsonWriter jsonWriter = new JsonWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(jsonFile), StandardCharsets.UTF_8)))) {
            jsonWriter.setIndent("  ");
            jsonWriter.setHtmlSafe(false);

            jsonWriter.beginObject();

            for (Map.Entry<String, OrcidInfo> entry : orcidMap.entrySet()) {
                jsonWriter.name(entry.getKey());
                jsonWriter.beginObject();

                jsonWriter.name("key");
                jsonWriter.value(entry.getValue().key());

                jsonWriter.name("verified");
                jsonWriter.value(entry.getValue().verified());

                jsonWriter.endObject();
            }

            jsonWriter.endObject();
        }
    }

    private void extractOrcids(NodeList nl) {
        // Unverified ORCIDs are stored as XML attributes for authors/editors
        for (int i = 0; i < nl.getLength(); i++) {
            Element e = (Element) nl.item(i);
            String name = e.getTextContent();
            if (e.hasAttribute("orcid")) {
                // ORCIDs as attributes are stored without "https://orcid.org/..." prefix
                personOrcidsMap.computeIfAbsent(name, x -> new HashSet<>(2)).add(e.getAttribute("orcid"));
            }
        }
    }

    private void process(Node node) {
        Element e = (Element) node.getFirstChild();

        String dblpKey = e.getAttribute("key");

        if (e.getNodeName().equals("www")) {
            if (!dblpKey.startsWith("homepages/")) {
                return;
            }

            // Person record
            NodeList authorNodeList = e.getElementsByTagName("author");
            NodeList urlNodeList = e.getElementsByTagName("url");

            // Persons may have multiple names
            List<String> authorList = IntStream
                    .range(0, authorNodeList.getLength())
                    .mapToObj(authorNodeList::item)
                    .map(Node::getTextContent)
                    .toList();

            // Persons may have multiple URLs
            List<String> urlList = IntStream
                    .range(0, urlNodeList.getLength())
                    .mapToObj(urlNodeList::item)
                    .map(Node::getTextContent)
                    .toList();

            if (authorList.size() != 0) {
                // Person profiles with 0 authors are only used for redirection to other profiles
                personCounter++;
                authorList.forEach(name -> nameToKeyMap.putIfAbsent(name, dblpKey));
                Set<String> verifiedOrcids = new HashSet<>(2);
                for (String url : urlList) {
                    // ORCIDs in person profiles are stored as URLs to "https://orcid.org/..."
                    Matcher m = orcidPattern.matcher(url);
                    if (m.matches()) {
                        verifiedOrcids.add(m.group(1));
                    }
                }
                // Create new Person with list of "verified" ORCIDs and an empty list of "unverified" ORCIDs
                personMap.put(dblpKey, new Person(authorList, verifiedOrcids, new HashSet<>(2)));
            }

        } else {
            // Publication record, only extract ORCIDs and ignore everything else
            publicationCounter++;
            extractOrcids(e.getElementsByTagName("author"));
            extractOrcids(e.getElementsByTagName("editor"));
        }
    }
}
