package com.galanteh.processors.kavak;

import org.apache.nifi.logging.ComponentLog;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KavakScrappingListPage extends KavakScrappingAbstractPage {

    public KavakScrappingListPage(boolean verbose, BlockingQueue queue, ComponentLog logger, String url){
        this.verbose = verbose;
        this.logger = logger;
        this.queue = queue;
        this.seed_url = url;
        try {
            this.doc = get_doc();
        }
        catch (Exception ex) {
            this.doc = null;
        }
    }

    /**
     * Get all the links elements with Cars in the page.
     * @param Document with HTML
     * @return Elements
     */
    public Elements get_car_links(Document doc)
    {
        Elements links = doc.select("a[href]"); // a with href
        Elements links_cars = new Elements();
        String regex = "-compra-de-autos-";
        for (Element link: links) {
            Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(link.attr("href"));
            while (matcher.find()) {
                links_cars.add(link);
            }
        }
        return links_cars;
    }

    @Override
    public void run(){
        Elements links = new Elements();
        try {
            Document doc = this.get_doc();
            if (doc != null) {
                links.addAll(this.get_car_links(doc));
            }
            for (Element link: links) {
                String url = get_absolute_url(link.attr("href"));
                if (verbose) {  this.logger.info(String.format("Found new car link: %s", url)); }
                new Thread(new KavakScrappingItemPage(this.verbose, this.queue, this.logger, url)).start();
            }
        }
        catch (Exception ex) {
            this.logger.error(String.format("%s", ex.getMessage()));
        }
    }


}

