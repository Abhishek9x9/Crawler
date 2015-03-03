package com.xxx.Inventory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.xxx.WebCrawler.Product;

@Slf4j
public class ProductCSVReader 
{   
    private final Gson gson = new Gson();
    private String csvFilePath;
    private Map<Long, Product> idToProductMap;
    private int corruptedItems ;
    public ProductCSVReader(String filePath)
    {
        corruptedItems = 0;
        this.csvFilePath = filePath;
        idToProductMap = new HashMap<Long, Product>();
        loadData();
    }
    
    private void loadData()
    {
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = "@";
     
        try {
     
            br = new BufferedReader(new FileReader(csvFilePath));
            while ((line = br.readLine()) != null) 
            {
                String[] productData = line.split(cvsSplitBy);
                try
                {
                int length = productData.length;
                if(length < 10)
                {
                    corruptedItems++;
                    continue;
                }
                //id,url,brand,title,image_url,about,saving,price,size_price_list,related_item
                Product product = new Product();
                product.setId(Long.parseLong(productData[0]));
                product.setUrl(productData[1]);
                product.setBrand(productData[2]);
                product.setTitle(productData[3]);
                product.setImageUrl(productData[4]);
                product.setAbout(productData[5]);
                product.setSaving(productData[6]);
                product.setPrice(productData[7]);
                String json = productData[8];
                json = removeTrailingQuotes(json);
                json = removeMultipleQuotesWithSingle(json);
                Type jsonType;
                jsonType = new TypeToken<Set<String>>() {
                    private static final long serialVersionUID = 1L;

                }.getType();
                
                product.setSizePriceList((Set<String>) ("".equals(json) ? null : gson.fromJson(json, jsonType)));
                jsonType = new TypeToken<Map<Long, String>>() {
                    private static final long serialVersionUID = 1L;
                }.getType();
                
                json = productData[9];
                json = removeTrailingQuotes(json);
                json = removeMultipleQuotesWithSingle(json);
                product.setRelatedItems((Map<Long, String>) ("".equals(json) ? null : ("".equals(json) ? null : gson.fromJson(json, jsonType))));
                idToProductMap.put(product.getId(), product);
                }
                catch(Exception e)
                {
                    // ignore 
                    corruptedItems++;
                }
            }
        }
        catch (FileNotFoundException e) 
        {
            e.printStackTrace();
        } 
        catch (IOException e) 
        {
            e.printStackTrace();
        } 
        catch (Exception e) 
        {
            e.printStackTrace();
        }
        finally 
        {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        log.info("Total number of loaded items " + idToProductMap.size());
        log.info("Total number of invalid items " + corruptedItems);
    }
    
    public Product getProductForId(Long productId)
    {
        return idToProductMap.get(productId);
    }
    
    public Collection<Product> getProducts()
    {
        return idToProductMap.values();
    }
    
    private String removeTrailingQuotes(String str)
    {
        if(0 == str.length())
        {
            return str;
        }
        int start = str.startsWith("\"") ? 1: 0;
        int end = str.endsWith("\"") ? str.length() -1: str.length();
        return str.substring(start, end);
    }
    
    private String removeMultipleQuotesWithSingle(String str)
    {
        return str.replaceAll("\"+", "\"");
    }
    
    public static void main(String str[])
    {
        ProductCSVReader productCSVReader = new ProductCSVReader("/tmp/products.csv");
        System.out.println(productCSVReader.getProductForId(40004389l));
    }
}
