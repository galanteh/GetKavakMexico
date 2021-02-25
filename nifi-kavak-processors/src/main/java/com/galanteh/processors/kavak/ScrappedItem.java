package com.galanteh.processors.kavak;

import java.util.List;
import java.time.LocalDateTime;

public class ScrappedItem {
    public String url;
    public LocalDateTime datetime;
    public String title;
    public String content;
    public String author;

    public ScrappedItem(String url) {
        this.url = url;
        this.datetime = LocalDateTime.now();
    }

    public ScrappedItem(String url, String title, LocalDateTime datetime, String content, String author ) {
        this(url);
        this.title = title;
        this.datetime = datetime;
        this.content = content;
        this.author = author;
    }

}
