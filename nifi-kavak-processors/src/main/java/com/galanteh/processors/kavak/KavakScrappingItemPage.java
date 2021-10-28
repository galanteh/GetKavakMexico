package com.galanteh.processors.kavak;

import org.apache.nifi.logging.ComponentLog;
import org.javamoney.moneta.Money;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Pattern;

public class KavakScrappingItemPage extends KavakScrappingAbstractPage {

    public Car car = null;

    public KavakScrappingItemPage(boolean verbose, BlockingQueue queue, ComponentLog logger, String url) {
        this.verbose = verbose;
        this.logger = logger;
        this.queue = queue;
        this.seed_url = url;
        try {
            this.doc = get_doc();
            this.parse_item_from_document(this.doc);
        } catch (Exception ex) {
            this.doc = null;
        }
    }

    public void parse_item_from_document(Document doc) {
        this.car = new Car(this.seed_url);
        if (verbose) {  this.logger.info(String.format("Parsing car info from: %s", this.seed_url)); }
        this.parse_manufacturer_model(doc);
        if (verbose) {  this.logger.info(String.format("Car manufacturer: %s", this.car.manufacturer)); }
        if (verbose) {  this.logger.info(String.format("Car model: %s", this.car.model)); }
        this.parse_year_odometer_location(doc);
        if (verbose) {  this.logger.info(String.format("Car year: %s", this.car.year)); }
        if (verbose) {  this.logger.info(String.format("Car odometer in KM: %s", this.car.odometer)); }
        if (verbose) {  this.logger.info(String.format("Car location: %s", this.car.location)); }
        this.parse_transmission_fuel(doc);
        if (verbose) {  this.logger.info(String.format("Car transmission: %s", this.car.transmission)); }
        this.parse_details_dots(doc);
        if (verbose) {  this.logger.info(String.format("Car condition: %s", this.car.condition)); }
        this.parse_price(doc);
        if (verbose) {  this.logger.info(String.format("Car price: %s", this.car.price)); }
    }

    public void parse_manufacturer_model(Document doc) {
        Elements elements = doc.select("h1.srfs-5");
        if (elements.size() > 0) {
            String line = elements.get(0).text();
            this.car.manufacturer = Car.get_manufacturer_from(line);
            this.car.model = line.replaceAll("(?i)" + this.car.manufacturer, ""); // Case Insensitive
        }
    }

    public void parse_year_odometer_location(Document doc) {
        Elements elements = doc.select("span.srfs-4");
        String line = elements.get(0).text();
        String[] data = line.split(Pattern.quote("|"));
        this.car.year = Integer.parseInt(data[0].trim());
        this.car.odometer = Integer.parseInt(data[1].trim().replace("km", "").replace(",", "").trim());
        this.car.location = data[2].trim();
    }

    public void parse_transmission_fuel(Document doc) {
        String data = "";
        String[] fuel_type = {"Diesel", "Eléctrico", "Gasolina"};
        Elements elements = doc.select("p.line-normal.mb-0.xrfs-3");
        data = elements.get(0).text();
        this.car.transmission = data.trim();
        data = elements.get(2).text();
        Integer i = 0;
        while (!Arrays.asList(fuel_type).contains(data.trim()))
        { data = elements.get(i).text();
        i = i + 1; }
        this.car.fuel = data.trim();
    }

    public void parse_details_dots(Document doc) {
        Elements elements = doc.select("div.dot-inner");
        this.car.details_dots = elements.size();
        this.car.initialize_condition();
    }

    public void parse_price(Document doc) {
        Elements elements = doc.select("p.srfs-5.font-weight-bold.text-black-3.d-inline-flex.align-items-center.line-normal.mb-0");
        String price_text = elements.get(0).text().replace("$", "").replace(",", "").replace(".", "").trim();
        this.car.price = Money.of(Integer.parseInt(price_text), "MXN");
    }

    @Override
    public void run(){
        this.queue.add(this.car);
    }

}
