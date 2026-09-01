package org.myorg.urls;

public record Url(String shortUrl, String longUrl, Integer accessCount) {
    public Url {
        if (accessCount == null) {
            accessCount = 0;
        }
    }

    public Url(String shortUrl, String longUrl) {
        this(shortUrl, longUrl, 0);
    }
}
