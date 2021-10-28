package com.galanteh.processors.kavak;


import org.javamoney.moneta.Money;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import org.apache.nifi.logging.ComponentLog;

public class KavakScrappingWebsiteTest {

    private KavakScrappingWebsite ksw;

    /**
     * This is to avoid the brand lawyers crawlers looking for mentions to their website
     * @return
     */
    public static String get_website_url(){
        return "https://www.k-a-v-a-k.com/".replace("-", "");
    }

    @Before
    public void setUp() {
        ksw = new KavakScrappingWebsite(false, null, null, "", 0, 0,0,0);
    }

    @Test
    public void TestGrabbingTotalPages() {
        String html = "<app-pagination _ngcontent-serverapp-c26=\"\" _nghost-serverapp-c38=\"\"><div _ngcontent-serverapp-c38=\"\" class=\"d-flex w-100 justify-content-center align-items-center my-2\"><a _ngcontent-serverapp-c38=\"\" class=\"mfs-10 pointer\" style=\"display: none;\"> &lt; </a><span _ngcontent-serverapp-c38=\"\" class=\"mx-4\"> 0 - 30 de 6573 Resultados </span><a _ngcontent-serverapp-c38=\"\" class=\"mfs-10 pointer\" style=\"display: block;\"> &gt; </a></div></app-pagination>";
        Document doc = Jsoup.parse(html);
        Elements elements = doc.select("span.mx-4");
        for (Element e : elements) {
            String results = e.text().strip();
            assertEquals(results, "0 - 30 de 6573 Resultados");
        }
    }

    @Test
    public void TestParsingTotalPages() {
        String result_text = "0 - 30 de 6573 Resultados";
        String[] parts = result_text.split(" de ");
        result_text = parts[1].replaceAll("\\D+","");
        assertEquals(result_text,"6573" );
    }

    @Test
    public void TestExtractLinks() {
        String html = "<a _ngcontent-sc53=\"\" href=\"/nissan-march-hatch-back-sense-2020-compra-de-autos-27622\">";
        Document doc = Jsoup.parse(html);
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
        assertEquals(links_cars.size(), 1);
        assertEquals(links_cars.get(0).attr("href"), "/nissan-march-hatch-back-sense-2020-compra-de-autos-27622");
    }

    @Test
    public void TestPaginationUrls(){
        try {
            ksw.year_max_filter = 2020;
            ksw.year_min_filter = 2017;
            ksw.manufacturer_filter = "Acura";
            List<String> pages = ksw.get_list_pagination_urls();
            assertEquals(get_website_url() + "page-1/ano-2017-2018-2019-2020/comprar-Acura/compra-de-autos",pages.get(0));
        }
        catch (Exception e) { fail(e.getMessage());}
    }

    @Test
    public void TestGetCarDetails() {
        String html = "<div _ngcontent-serverapp-c63=\"\" class=\"mb-0\"><h1 _ngcontent-serverapp-c63=\"\" class=\"srfs-5 mrfs-xl-1 font-weight-bold text-black-3 mb-1 d-block\"> Hyundai Grand i10 Hatch Back GL MID </h1><!----><!----><span _ngcontent-serverapp-c63=\"\" class=\"srfs-4 font-weight-bold sub-gray-1 mb-0 d-block ng-star-inserted\"> 2017  |  51,150 km  |  Puebla </span></div>";
        Document doc = Jsoup.parse(html);
        Elements manufacturer_elements = doc.select("h1.srfs-5");
        assertEquals(manufacturer_elements.get(0).text().contains("Hyundai"), true);
        assertEquals(1, manufacturer_elements.size());

        Elements year_elements = doc.select("span.srfs-4");
        assertEquals(1, year_elements.size());
        String text = year_elements.get(0).text();
        assertEquals(text, "2017 | 51,150 km | Puebla");
    }

    @Test
    public void TestGetYearOdometerLocationFromLine(){
        String line = "2017 | 51,150 km | Ciudad de Mexico";
        String[] elements = line.split(Pattern.quote("|"));
        Integer year = Integer.parseInt(elements[0].trim());
        Integer odometer = Integer.parseInt(elements[1].trim().replace("km", "").replace(",", "").trim());
        String location = elements[2].trim();
        assertEquals(year.longValue(), 2017);
        assertEquals(odometer.longValue(), 51150);
        assertEquals(location, "Ciudad de Mexico");
    }

    @Test
    public void TestGetManufacturer() {
        String manufacturer = Car.get_manufacturer_from(" Hyundai Grand i10 Hatch Back GL MID ");
        assertEquals("HYUNDAI", manufacturer);
    }

    @Test
    public void TestTransmissionFuelParsing() {
        String html = "<div _ngcontent-serverapp-c21=\"\" class=\"d-flex flex-column w-100\"><div _ngcontent-serverapp-c21=\"\" class=\"d-flex w-100 mb-2\"><h2 _ngcontent-serverapp-c21=\"\" class=\"font-weight-bold mb-0 srfs-3\"> Características del auto </h2></div><div _ngcontent-serverapp-c21=\"\" class=\"d-flex w-100\"><div _ngcontent-serverapp-c21=\"\" class=\"container-fluid px-0\"><div _ngcontent-serverapp-c21=\"\" class=\"row no-gutters mx-n1\"><!----><!----><!----><!----><div _ngcontent-serverapp-c21=\"\" class=\"col-auto col-xl-4\"><div _ngcontent-serverapp-c21=\"\" class=\"d-flex my-2 w-100 px-1\"><div _ngcontent-serverapp-c21=\"\" class=\"d-flex flex-column flex-grow-1 ml-2\"><h6 _ngcontent-serverapp-c21=\"\" class=\"line-normal mb-1 xrfs-3\"><span _ngcontent-serverapp-c21=\"\" class=\"sub-gray-1\"> Transmisión </span></h6><p _ngcontent-serverapp-c21=\"\" class=\"line-normal mb-0 xrfs-3\"> Manual </p></div></div></div><!----><!----><!----><!----><!----><div _ngcontent-serverapp-c21=\"\" class=\"col-auto col-xl-4\"><div _ngcontent-serverapp-c21=\"\" class=\"d-flex my-2 w-100 px-1\"><div _ngcontent-serverapp-c21=\"\" class=\"d-flex flex-column flex-grow-1 ml-2\"><h6 _ngcontent-serverapp-c21=\"\" class=\"line-normal mb-1 xrfs-3\"><span _ngcontent-serverapp-c21=\"\" class=\"sub-gray-1\"> Tracción </span></h6><p _ngcontent-serverapp-c21=\"\" class=\"line-normal mb-0 xrfs-3\"> 4x2 </p></div></div></div><!----><!----><!----><!----><!----><div _ngcontent-serverapp-c21=\"\" class=\"col-auto col-xl-4\"><div _ngcontent-serverapp-c21=\"\" class=\"d-flex my-2 w-100 px-1\"><div _ngcontent-serverapp-c21=\"\" class=\"d-flex flex-column flex-grow-1 ml-2\"><h6 _ngcontent-serverapp-c21=\"\" class=\"line-normal mb-1 xrfs-3\"><span _ngcontent-serverapp-c21=\"\" class=\"sub-gray-1\"> Caballos de fuerza </span></h6><p _ngcontent-serverapp-c21=\"\" class=\"line-normal mb-0 xrfs-3\"> 88 </p></div></div></div><!----><!----><!----><!----><!----><div _ngcontent-serverapp-c21=\"\" class=\"col-auto col-xl-4\"><div _ngcontent-serverapp-c21=\"\" class=\"d-flex my-2 w-100 px-1\"><div _ngcontent-serverapp-c21=\"\" class=\"d-flex flex-column flex-grow-1 ml-2\"><h6 _ngcontent-serverapp-c21=\"\" class=\"line-normal mb-1 xrfs-3\"><span _ngcontent-serverapp-c21=\"\" class=\"sub-gray-1\"> Tipo de combustible </span></h6><p _ngcontent-serverapp-c21=\"\" class=\"line-normal mb-0 xrfs-3\"> Gasolina </p></div></div></div><!----><!----><!----><!----><!----><div _ngcontent-serverapp-c21=\"\" class=\"col-auto col-xl-4\"><div _ngcontent-serverapp-c21=\"\" class=\"d-flex my-2 w-100 px-1\"><div _ngcontent-serverapp-c21=\"\" class=\"d-flex flex-column flex-grow-1 ml-2\"><h6 _ngcontent-serverapp-c21=\"\" class=\"line-normal mb-1 xrfs-3\"><span _ngcontent-serverapp-c21=\"\" class=\"sub-gray-1\"> Número de pasajeros </span></h6><p _ngcontent-serverapp-c21=\"\" class=\"line-normal mb-0 xrfs-3\"> 5 </p></div></div></div><!----><!----><!----><!----><!----><!----><!----><div _ngcontent-serverapp-c21=\"\" class=\"col-auto col-xl-4\"><div _ngcontent-serverapp-c21=\"\" class=\"d-flex my-2 w-100 px-1\"><div _ngcontent-serverapp-c21=\"\" class=\"d-flex flex-column flex-grow-1 ml-2\"><h6 _ngcontent-serverapp-c21=\"\" class=\"line-normal mb-1 xrfs-3\"><span _ngcontent-serverapp-c21=\"\" class=\"sub-gray-1\"> Coordenadas </span></h6><p _ngcontent-serverapp-c21=\"\" class=\"line-normal mb-0 xrfs-3\"> ID: A-027 </p></div></div></div><!----><!----><!----><!----><!----><!----><div _ngcontent-serverapp-c21=\"\" class=\"col-auto col-xl-4\"><div _ngcontent-serverapp-c21=\"\" class=\"d-flex my-2 w-100 px-1\"><div _ngcontent-serverapp-c21=\"\" class=\"d-flex flex-column flex-grow-1 ml-2\"><h6 _ngcontent-serverapp-c21=\"\" class=\"line-normal mb-1 xrfs-3\"><span _ngcontent-serverapp-c21=\"\" class=\"sub-gray-1\"> Ubicación </span></h6><p _ngcontent-serverapp-c21=\"\" class=\"line-normal mb-0 xrfs-3\"> KAVAK Puebla </p></div></div></div><!----><!----></div><!----></div></div></div>";
        Document doc = Jsoup.parse(html);
        Elements elements = doc.select("p.line-normal.mb-0.xrfs-3");
        assertEquals(elements.size(), 7);
        assertEquals(elements.get(0).text(), "Manual");
        assertEquals(elements.get(3).text(),"Gasolina");
    }

    @Test
    public void TestPriceParsing(){
        String html = "<div _ngcontent-serverapp-c19=\"\" class=\"d-flex align-items-center mb-2\"><p _ngcontent-serverapp-c19=\"\" class=\"srfs-5 font-weight-bold text-black-3 d-inline-flex align-items-center line-normal mb-0\"> $147,999 </p><!----><!----><p _ngcontent-serverapp-c19=\"\" class=\"regular-price ml-2\"><span _ngcontent-serverapp-c19=\"\" class=\"sub-gray-1 text-center line-normal mb-0\"> $149,999 </span></p></div>";
        Document doc = Jsoup.parse(html);
        Elements elements = doc.select("p.srfs-5.font-weight-bold.text-black-3.d-inline-flex.align-items-center.line-normal.mb-0");
        String price_text = elements.get(0).text().replace("$","").replace(",","").replace(".","").trim();
        assertEquals(price_text, "147999");
    }

}
