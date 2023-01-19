import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;

public class Main {
/*
Total number of persons in dblp      : 3153446
Total number of publications in dblp : 6456252

Total ORCIDs in dblp with human verification             : 119733
Total ORCIDs in dblp extracted from publication metadata : 637072
Datasets with at least one author with verified ORCID    : 13007
Total number of authors with verified ORCIDs in datasets : 19192
Datasets with at least one known, but unverified ORCID   : 31581
Dataset authors with known, but unverified ORCIDs        : 55553

=====
These files contain the compressed intermediate results used in our research:
=====
zenodo_dump.json.gz
dblp_orcids.json.gz
statistics.json.gz
=====
*/

    public static void main(String[] args) throws IOException, InterruptedException, ParserConfigurationException, SAXException {

        // STEP 1: download dblp dump (2 files)
        // curl -O https://dblp.org/xml/release/dblp-2023-01-03.xml.gz
        // curl -O https://dblp.org/xml/release/dblp-2019-11-22.dtd

        // STEP 2: Retrieve Zenodo data from web-API
        new ZenodoRequest("zenodo_dump.json");

        // STEP 3: Parse local copy DBLP in XML/DTD files
        // If DTD file does not exist in the current directory, set path accordingly
        //System.setProperty("user.dir", "path/to/dblp.xml/and/dblp.dtd");
        new DblpInput("dblp-2023-01-03.xml.gz", "dblp_orcids.json");

        // STEP 4: Generate statistics
        new Statistics("dblp_orcids.json", "zenodo_dump.json", "statistics.json");

        System.out.println("*** [Finished] ***");
    }
}
