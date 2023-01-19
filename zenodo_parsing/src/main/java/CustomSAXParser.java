import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.*;
import java.util.Set;
import java.util.Stack;
import java.util.function.Consumer;

public class CustomSAXParser extends DefaultHandler {

    private final Stack<Node> path = new Stack<>();
    private final Set<String> queryTags;
    private final DocumentBuilder documentBuilder;
    private final Consumer<Node> nodeConsumer;

    private Document rootNode;
    private boolean isCaptureActive = false;
    private int depth = 0;

    public CustomSAXParser(Set<String> tags, Consumer<Node> consumer) throws ParserConfigurationException {
        queryTags = tags;
        nodeConsumer = consumer;
        documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        rootNode = documentBuilder.newDocument();
        path.push(rootNode);
    }

    @Override
    public void characters(char[] ch, int start, int length) {
        if (isCaptureActive) {
            Node last = path.peek();
            String text = new String(ch, start, length);
            last.appendChild(rootNode.createTextNode(text));
        }
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) {
        depth++;

        if (depth == 2) {
            isCaptureActive = queryTags.contains(qName);
        }

        if (depth >= 2 && isCaptureActive) {
            Element element = rootNode.createElement(qName);

            int attrLen = attributes.getLength();
            for (int i = 0; i < attrLen; i++) {
                element.setAttribute(attributes.getQName(i), attributes.getValue(i));
            }

            Node last = path.peek();
            last.appendChild(element);
            path.push(element);
        }
    }

    public void endElement(String uri, String localName, String qName) {
        if (isCaptureActive) {
            path.pop();
        }

        if (depth == 2 && isCaptureActive) {
            nodeConsumer.accept(rootNode);
            path.clear();
            isCaptureActive = false;
            rootNode = documentBuilder.newDocument();
            path.push(rootNode);
        }

        depth--;
    }
}
