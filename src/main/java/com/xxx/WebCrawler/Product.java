package com.xxx.WebCrawler;

import java.util.Map;
import java.util.Set;

import lombok.Data;

@Data
public class Product 
{
    long id;
    String url;
    String brand;
    String title;
    String imageUrl;
    String about;
    Set<String> sizePriceList;
    String saving;
    String price;
    Map<Long, String> relatedItems;
}
