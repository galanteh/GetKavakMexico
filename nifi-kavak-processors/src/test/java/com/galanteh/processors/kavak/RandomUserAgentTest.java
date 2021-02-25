package com.galanteh.processors.kavak;

import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;

public class RandomUserAgentTest {

    private RandomUserAgent rua;

    @Before
    public void setUp() {
        rua = new RandomUserAgent();
    }

    @Test
    public void TestBrowser() {
        String browser  = rua.getRandomUserAgent();

        // Internet Explorer
        if (browser.contains("MSIE")){
            assertTrue(browser.contains("MSIE"));
        }
        // Firefox
        if (browser.contains("Gecko")){
            assertTrue(browser.contains("Gecko"));
        }
        // Chrome
        if (browser.contains("Chrome")){
            assertTrue(browser.contains("Chrome"));
        }
        // Safari
        if (browser.contains("AppleWebKit")){
            assertTrue(browser.contains("AppleWebKit"));
        }
        // Opera
        if (browser.contains("Presto")){
            assertTrue(browser.contains("Presto"));
        }
    }


}
