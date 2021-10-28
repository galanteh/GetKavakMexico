package com.galanteh.processors.kavak;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import org.apache.nifi.logging.ComponentLog;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

public abstract class KavakScrappingAbstractPage implements Runnable {

    public boolean verbose = true;
    public String seed_url = null;
    public Document doc = null;
    public BlockingQueue queue = null;
    public ComponentLog logger = null;
    public static final Integer CARS_PER_PAGE = 30;

    /**
     * This is to avoid the brand lawyers crawlers looking for mentions to their website
     * @return
     */
    public static String get_website_url(){
        return "https://www.k-a-v-a-k.com/".replace("-", "");
    }

    /**
     * Get a complete url from a relative one.
     * @param relative_url
     * @return URL
     */
    public static String get_absolute_url(String relative_url)
    {
        if (relative_url.startsWith("/")) {
            return get_website_url() + relative_url;
        }
        else{
            return get_website_url() + "/" + relative_url;
        }
    }

    /**
     * Return the number of cars that are available in the HTML doc
     * @param HTML doc of a page
     * @return number of cars avaible in Kavak
     */
    public static Integer get_total_cars(Document doc) {
        Elements elements = doc.select("span.mx-4");
        if (elements.size() > 0) {
            String result_text = elements.get(0).text().strip();
            String[] parts = result_text.split(" de ");
            result_text = parts[1].replaceAll("\\D+", "");
            return Integer.parseInt(result_text);
        }
        return 0;
    }

    /**
     * Returns the total pages for the pagination
     * @param HTML doc of a page
     * @return number of pages
     */
    public static Integer get_total_pages(Document doc){
        Integer total_cars = get_total_cars(doc);
        if (total_cars > 0) {
            return Math.round(total_cars / CARS_PER_PAGE);
        }
        return 0;
    }

    /**
     * Get the HTML of a page
     * @param url
     * @return HTML page
     * @throws Exception in case we can't get the HTML
     */
    public static Document get_HTML_from_url(String url) throws Exception {
        Document doc = null;
        String userAgent = RandomUserAgent.getRandomUserAgent();

        try {
            doc = Jsoup.connect(url).userAgent(userAgent).timeout(100000).get();
        } catch (IOException ex) {
            throw new Exception(String.format("%s", ex.getMessage()));
        }
        return doc;
    }

    /**
     * Return the Document in a Lazy method.
     * @return Document
     */
    public Document get_doc() throws Exception{
        if (this.doc == null) {
            try {
                if (verbose) {  this.logger.info(String.format("Getting HTML from: %s", this.seed_url)); }
                this.doc = get_HTML_from_url(this.seed_url);
            }
            catch (IOException ex) {
                if (verbose) {  this.logger.error(String.format("%s", ex.getMessage())); };
                throw new Exception(String.format("%s", ex.getMessage()));
            }
        }
        return this.doc;
    }

}
