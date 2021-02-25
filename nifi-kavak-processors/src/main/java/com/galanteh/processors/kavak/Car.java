package com.galanteh.processors.kavak;

import javax.money.MonetaryAmount;
import java.nio.charset.Charset;
import java.util.*;

public class Car extends ScrappedItem {

    public String manufacturer;
    public String model;
    public Integer year;
    public Integer odometer;
    public String location;
    public String transmission;
    public String fuel;
    public String condition;
    public Integer details_dots;
    public MonetaryAmount price;
    public final String WEBSITE = "KAVAK Mexico";
    public static final List<String> MANUFACTURERS = Collections.unmodifiableList(
            new ArrayList<String>() {{
                add("ACURA");
                add("ALFA ROMEO");
                add("AUDI");
                add("BMW");
                add("BUICK");
                add("CADILLAC");
                add("CHEVROLET");
                add("CHRYSLER");
                add("DODGE");
                add("FIAT");
                add("FORD");
                add("GMC");
                add("HONDA");
                add("HYUNDAI");
                add("INFINITI");
                add("JAGUAR");
                add("JEEP");
                add("KIA");
                add("LAND ROVER");
                add("LINCOLN");
                add("MAZDA");
                add("MERCEDEZ BENZ");
                add("MINI");
                add("MITSUBISHI");
                add("NISSAN");
                add("PEUGEOT");
                add("PORSCHE");
                add("RENAULT");
                add("SEAT");
                add("SMART");
                add("SUBARU");
                add("SUZUKI");
                add("TOYOTA");
                add("VOLKSWAGEN");
                add("VOLVO");
            }});


    public static String get_manufacturer_from(String line){
        String[] words = line.trim().split(" ");
        List<String> matches = new ArrayList<String>();

        for(String search: words) {
            for (String manufacturer: MANUFACTURERS) {
                if (search.toLowerCase().contains(manufacturer.toLowerCase())) {
                    matches.add(manufacturer);
                }
            }
        }
        if (matches.size() == 0) { return null;};
        return matches.get(0);
    }

    public Car(String url) { super(url); }

    public Car(String manufacturer, String model, Integer year, Integer odometer,Integer details_dots, MonetaryAmount price) {
        super("");
        this.title = String.format("%s %s", manufacturer, model);
        this.content = String.format("%s %s %d %d", manufacturer, model, year, odometer);
        this.author = WEBSITE;
        this.manufacturer = manufacturer.trim();
        this.model = model.trim();
        this.year = year;
        this.odometer = odometer;
        this.details_dots = details_dots;
        this.price = price;
        this.initialize_condition();
    }

    public Car(String manufacturer, String model, Integer year, Integer odometer, String location, String transmission, String fuel, Integer details_dots, MonetaryAmount price) {
        this(manufacturer, model, year, odometer, details_dots, price);
        this.location = location;
        this.transmission = transmission;
        this.fuel = fuel;
        this.details_dots = details_dots;
        this.initialize_condition();
    }

    /**
     * Options:
     * new == 0
     * like new == 1
     * excellent == 2
     * good > 2 & < 5
     * fair >= 5 && < 10
     * salvage >= 10
     */
    public void initialize_condition() {
        if (this.details_dots == 0) {
            this.condition = "new";
        }
        if (this.details_dots == 1) {
            this.condition = "like new";
        }
        if (this.details_dots == 2) {
            this.condition = "excellent";
        }
        if (this.details_dots > 2 && this.details_dots < 5) {
            this.condition = "good";
        }
        if (this.details_dots >= 5 && this.details_dots < 10) {
            this.condition = "fair";
        }
        if (this.details_dots >= 10) {
            this.condition = "salvage";
        }
    }

    private String get_string_representation() {
        String url_car = this.url;
        if (url_car == null) { url_car = "";}
        String manufacturer = this.manufacturer;
        if (manufacturer == null) { manufacturer = "";}
        String model = this.model;
        if (model == null) { model = "";}
        String year = "";
        if (this.year == null) { year = "";} else { year = this.year.toString();};
        String odometer = "";
        if (this.odometer == null) { odometer = "";} else { odometer = this.odometer.toString();};
        String location = "";
        if (this.location == null) { location = "";} else { location = this.location.toString();};
        String transmission = "";
        if (this.transmission == null) { transmission = "";} else { transmission = this.transmission.toString();};
        String fuel = "";
        if (this.fuel == null) { fuel = "";} else { fuel = this.fuel.toString();};
        String condition = "";
        if (this.condition == null) { condition = "";} else { condition = this.condition.toString();};
        String price = "";
        if (this.price == null) { price = "";} else { price = this.price.toString();};
        return String.format("%s \n %s %s %s %s km %s %s %s %s $%s", url_car, manufacturer, model, year, odometer, location, transmission, fuel, condition, price );
    }

    public byte [] getBytes(Charset charset) {
        return this.get_string_representation().getBytes(charset);
    }

}
