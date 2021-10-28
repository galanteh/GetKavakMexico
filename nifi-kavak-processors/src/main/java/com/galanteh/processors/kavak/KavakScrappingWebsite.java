package com.galanteh.processors.kavak;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.IntStream;
import org.apache.nifi.logging.ComponentLog;
import org.jsoup.nodes.Document;

public class KavakScrappingWebsite implements Runnable {

    public static Integer gap_years = 5;
    public String manufacturer_filter = "";
    public Integer year_min_filter = 0;
    public Integer year_max_filter = 0;
    public Integer odometer_min_filter = 0;
    public Integer odometer_max_filter = 0;
    public BlockingQueue queue = null;
    public ComponentLog logger;
    public Boolean verbose = true;

    public KavakScrappingWebsite(boolean verbose, ComponentLog logger, BlockingQueue queue, String manufacturer_filter, Integer year_min_filter, Integer year_max_filter, Integer odometer_min_filter, Integer odometer_max_filter) {
        this.queue = queue;
        this.manufacturer_filter = manufacturer_filter;
        this.year_min_filter = year_min_filter;
        this.year_max_filter = year_max_filter;
        this.odometer_min_filter = odometer_min_filter;
        this.odometer_max_filter = odometer_max_filter;
        this.logger = logger;
    }

    /**
     * This is to avoid the brand lawyers crawlers looking for mentions to their website
     * @return
     */
    public static String get_website_url(){
        return "https://www.k-a-v-a-k.com/".replace("-", "");
    }

    public static String get_seed_url(){
        return get_website_url() + "/compra-de-autos";
    }

    /**
     * Get the total pages in the website
     * @return Integer
     */
    public Integer get_total_pages(Document doc){
        return KavakScrappingAbstractPage.get_total_pages(doc);
    }

    /**
     * Apply the filter of manufacturer to the seed url
     * https://www.k*v*k.com/comprar-Acura/ano-2019-2018-2017/km-15000-a-20789/compra-de-autos
     * @return URL
     */
    private String get_url_with_filters() {
        String new_url = this.get_seed_url();
        if (this.manufacturer_filter != "") {
            String filter = String.format("comprar-%s/", this.manufacturer_filter);
            StringBuilder builder = new StringBuilder(new_url);
            Integer offset = this.get_seed_url().indexOf("/compra-de-autos");
            builder.insert(offset + 1, filter);
            new_url = builder.toString();
        }
        if (this.year_min_filter == 0 & this.year_max_filter > 0) {
            this.year_min_filter = this.year_max_filter - gap_years;
        }
        if (this.year_max_filter == 0 & this.year_min_filter > 0) {
            this.year_max_filter = this.year_min_filter + gap_years;
        }

        if (this.year_min_filter != 0 || this.year_max_filter != 0){
            // /ano-2019-2018-2017/
            int[] range = IntStream.rangeClosed(this.year_min_filter, this.year_max_filter).toArray();
            String filter = "ano";
            for (int i = 0; i < range.length; i++) {
                filter = filter + String.format("-%s",Integer.toString(range[i]));
            }
            filter = filter + "/";
            StringBuilder builder = new StringBuilder(new_url);
            Integer offset = this.get_seed_url().indexOf("/compra-de-autos");
            builder.insert(offset + 1, filter);
            new_url = builder.toString();
        }
        return new_url;
    }

    /**
     * Returns true if the URL has filters
     * @return Boolean
     */
    private boolean has_filters(){
        return this.manufacturer_filter != "" || this.year_min_filter > 0 ||  this.year_max_filter > 0 || this.odometer_min_filter > 0 || this.odometer_max_filter > 0;
    }

    /**
     * Returns the URL
     */
    private String get_url(){
        if (this.has_filters())
        {
            return this.get_url_with_filters();
        }
        return this.get_seed_url();
    }

    /**
     * Return the list of pages with complete URL
     * @return List of String
     * @throws Exception
     */
    public List<String> get_list_pagination_urls() {
        List<String> result = new ArrayList<String>();
        try {
            Document doc = KavakScrappingAbstractPage.get_HTML_from_url(this.get_url_with_filters());
            Integer total_pages = KavakScrappingAbstractPage.get_total_pages(doc);
            String url_filters = this.get_url_with_filters();
            Integer offset = url_filters.indexOf(this.get_website_url());
            offset = offset + 22;
            String format_url = new StringBuilder(url_filters).insert(offset, "page-%s/").toString();
            for (Integer i : IntStream.rangeClosed(1, total_pages + 1).toArray()) {
                result.add(String.format(format_url, i));
            }
        }
        catch (Exception ex) {
            this.logger.error(String.format("%s", ex.getMessage()));
        }
        return result;
    }

    /**
     * Run the process of Scrapping the website
     * Starts with the seed URL with the filters
     */
    public void run() {
        for (String url_page: this.get_list_pagination_urls() ){
            if (verbose) {  this.logger.info(String.format("URL: %s", url_page)); }
            new Thread(new KavakScrappingListPage(verbose, this.queue, this.logger, url_page)).start();
        }
    }

}
