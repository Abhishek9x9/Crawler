package com.xxx.WebCrawler;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;


public class PageInfoExtractor 
{
    private static final String  urlPrefix = "http://bigbasket.com"; 
    
    public static Product getProduct(String url) throws IOException
    {
        Product product = new Product();
        product.setRelatedItems(new HashMap<Long, String>());
        product.setSizePriceList(new HashSet<String>());
        
        long productId = getProductIdFromUrl(url);
        // setting product id 
        product.setId(productId);
        // setting product url
        product.setUrl(url);
        
        Document doc = Jsoup.connect(url)     
                       .userAgent("Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:25.0) Gecko/20100101 Firefox/25.0")
                       .referrer("http://www.google.com")              
                       .get();
        Elements elements ;
        Element singleElement;
        singleElement = doc.select("div.uiv2-brand-name").first();
        // brand name
        product.setBrand(singleElement.child(0).text());
        
        singleElement = doc.select("div.uiv2-product-name").first();
        // product title
        product.setTitle(singleElement.child(0).text());
        
        singleElement = doc.select("div.uiv2-product-large-img-container").first();
        // product Image url
        product.setImageUrl(singleElement.child(0).attr("href"));
        
        elements = doc.select("div#uiv2-tab1");
        if(null != elements && elements.size() > 0)
        {
            singleElement = elements.first();
            if(null != singleElement)
            {
                product.setAbout(singleElement.child(0).text());
            }
        }
        
        elements = doc.select("div.uiv2-size-variants");
        if(null != elements && elements.size() > 0)
        {
            
            for (Element element : elements)
            {
                if(null != singleElement)
                {
                    if(null != element.getElementsByTag("label"))
                    {
                        product.getSizePriceList().add(element.getElementsByTag("label").text().trim().replaceAll(" +", " "));
                    }
                }
            }
        }
        
        elements = doc.select("div.uiv2-savings");
        if(null != elements && elements.size() > 0)
        {
            singleElement = elements.first();
            if(null != singleElement)
            {
                product.setSaving(singleElement.text().trim().replaceAll(" +", " "));
            }
        }
        else
        {
            product.setSaving("0");
        }
        elements = doc.select("div.uiv2-price");
        if(null != elements && elements.size() > 0)
        {
            singleElement = elements.first();
            if(null != singleElement)
            {
                product.setPrice(singleElement.text().trim().replaceAll(" +", " "));
            }
        }
        
        elements = doc.select("a[href]");
        for(Element link: elements) 
        {
            if(link.attr("href").matches(".*pd/\\d+.*"))
            {   
                Long refProductId = getProductIdFromUrl(link.attr("href"));
                String newUrl = link.attr("href");
                if(!newUrl.startsWith(urlPrefix))
                {
                        newUrl = urlPrefix + ( newUrl.startsWith("/") ? newUrl : "/" + newUrl); 
                }
                if(null == product.getRelatedItems())
                {
                    product.setRelatedItems(new HashMap<Long, String>());
                }
                product.getRelatedItems().put(refProductId, newUrl);
            }
        }
        return product;
    }
    
    public static long getProductIdFromUrl(String url)
    {
        String str = url.substring(url.indexOf("pd/") + "pd/".length());
        int lastIndex = str.indexOf('/') == -1 ? str.length() : str.indexOf('/');
        str = str.substring(0, lastIndex);
        return Long.parseLong(str);
    }
}
