package com.xxx.WebCrawler;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;

import com.google.gson.Gson;
import com.xxx.util.DBConnectionUtil;
import com.xxx.util.PropReader;

@Slf4j
public class BigBasketCrawler 
{
    private final static int TOTAL_THREADS = 10;
    private final static int MAX_RETRY = 100;
    private final static Gson gson = new Gson();
    private static final ThreadPoolExecutor  bufferCleanerService    = new ThreadPoolExecutor(TOTAL_THREADS, TOTAL_THREADS, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    private ConcurrentLinkedQueue<String> processingInfoQueue ;
    private int errorCount;
    private boolean isShutDown ; 
    private static Connection connection;
    private ConcurrentHashMap<Long, Long> productIdMap;
    
    public static void main(String[] args) throws SQLException, IOException, InterruptedException 
    {  
        PropReader proReader = new PropReader("/home/abhishekshukla/Desktop/migration/bb_db_properties");
        DBConnectionUtil.init(proReader);
        connection = DBConnectionUtil.getBigBasketDbConnection();
        BigBasketCrawler basketCrawler = new BigBasketCrawler();
        basketCrawler.collectData();
        bufferCleanerService.awaitTermination(10, TimeUnit.HOURS);
    }
    
    public BigBasketCrawler() throws SQLException
    {
        errorCount = 0;
        isShutDown = false;
        processingInfoQueue  = new ConcurrentLinkedQueue<String>();
        productIdMap = new ConcurrentHashMap<Long, Long>();
    }
    
    public void shutDown()
    {
        isShutDown = true;
    }
    
    public void collectData()
    {
        processingInfoQueue.add("http://bigbasket.com/pd/20000535/bb-royal-figs-200-gm-pouch/");
        processingInfoQueue.add("http://bigbasket.com/pd/20000537/bb-royal-dry-dates-200-gm-pouch/");
        processingInfoQueue.add("http://bigbasket.com/pd/20004688/priya-badam-milk-mix-200-gm-carton/");
        processingInfoQueue.add("http://bigbasket.com/pd/253418/western-farm-fresh-green-peas-1-kg-pouch/");
        processingInfoQueue.add("http://bigbasket.com/pd/20004694/priya-instant-dosa-mix-200-gm-carton/");
        processingInfoQueue.add("http://bigbasket.com/pd/20004695/priya-instant-gulab-jamun-mix-200-gm-carton/");
        processingInfoQueue.add("http://bigbasket.com/pd/20004703/priya-instant-pesarattu-mix-200-gm-carton/");
        processingInfoQueue.add("http://bigbasket.com/pd/20005166/priya-mango-dal-300-gm-carton/");
        processingInfoQueue.add("http://bigbasket.com/pd/20004696/priya-mix-kheer-200-gm-carton/");
        processingInfoQueue.add("http://bigbasket.com/pd/20004699/priya-mix-rava-dosa-200-gm-carton/");
        processingInfoQueue.add("http://bigbasket.com/pd/20002083/dadus-mithai-vatika-badamees-500-gm-box/");
        processingInfoQueue.add("http://bigbasket.com/pd/307733/wonderful-pistachios-almonds-medium-320-gm-carton/");
        processingInfoQueue.add("http://bigbasket.com/pd/20003978/crane-betel-nut-pieces-sweet-40-gms/");
        processingInfoQueue.add("http://bigbasket.com/pd/20000480/bb-royal-betel-nut-black-100-gm-pouch/");
        processingInfoQueue.add("http://bigbasket.com/pd/282875/dizzle-mouth-freshener-shahi-mix-180-gm-bottle/");
        processingInfoQueue.add("http://bigbasket.com/pd/273875/spraymintt-instant-mouth-freshener-icy-mint-15-gm-carton/");
        processingInfoQueue.add("http://bigbasket.com/pd/100139558/dizzle-mouth-freshener-red-mix-210-gm-bottle/");
        
        processingInfoQueue.add("http://bigbasket.com/pd/20005088/fresho-banana-chakkarakeli-1-kg/");
        processingInfoQueue.add("http://bigbasket.com/pd/20003625/fresho-apple-fuji-premium-3-pcs/"); 
        processingInfoQueue.add("http://bigbasket.com/pd/20001345/fresho-apple-indian-500-gm/");
        processingInfoQueue.add("http://bigbasket.com/pd/20005420/fresho-musk-melon-local-1-pc/");
        processingInfoQueue.add("http://bigbasket.com/pd/30001778/fresho-cherry-imported-250-gm/");
        processingInfoQueue.add("http://bigbasket.com/pd/20000712/fresho-grapes-california-1-kg/");
        
        processingInfoQueue.add("http://bigbasket.com/pd/20000701/fresho-beans-french-500-gm/");
        processingInfoQueue.add("http://bigbasket.com/pd/20000722/fresho-brinjal-small-purple-1-kg/"); 
        processingInfoQueue.add("http://bigbasket.com/pd/30007324/fresho-pomelo-1-pc/");
        processingInfoQueue.add("http://bigbasket.com/pd/30001451/fresho-brussel-sprouts-250-gm/");
        processingInfoQueue.add("http://bigbasket.com/pd/10000172/fresho-capsicum-red-500-gm/");
        processingInfoQueue.add("http://bigbasket.com/pd/10000133/fresho-lettuce-iceberg-500-gm/");
        
        processingInfoQueue.add("http://bigbasket.com/pd/30004907/double-horse-matta-broken-1-kg-pouch/");
        processingInfoQueue.add("http://bigbasket.com/pd/30004908/double-horse-matta-broken-500-gm-pouch/"); 
        processingInfoQueue.add("http://bigbasket.com/pd/40015563/parragon-books-desserts-english-64-pages/");
        processingInfoQueue.add("http://bigbasket.com/pd/40009163/sanjeev-kapoors-wrap-n-roll-104-pages/");
        processingInfoQueue.add("http://bigbasket.com/pd/40015578/parragon-books-the-curry-cookbook-english-224-pages/");
        processingInfoQueue.add("http://bigbasket.com/pd/40008856/delhi-press-motoring-world-monthly/");
        
        processingInfoQueue.add("http://bigbasket.com/pd/20000755/fresho-basale-leaf-200-to-250-gm/");
        processingInfoQueue.add("http://bigbasket.com/pd/40019140/halo-nourishing-shampoo-natural-protein-egg-1000-ml-bottle/"); 
        processingInfoQueue.add("http://bigbasket.com/pd/40019138/nutri-bite-cookies-pista-badam-100-gm-carton/");   
        processingInfoQueue.add("http://bigbasket.com/pd/40019135/nutri-bite-nankhatai-100-gm-carton/");  
        processingInfoQueue.add("http://bigbasket.com/pd/40018718/pristine-induction-sandwich-base-dlx-saucepan-18-cm-225-ltr/");
        processingInfoQueue.add("http://bigbasket.com/pd/40018709/pristine-multi-purpose-kadai-induction-sandwich-base-27-cm/");  
        processingInfoQueue.add("http://bigbasket.com/pd/40013865/collins-my-first-book-of-the-human-body-72-pages/");
        processingInfoQueue.add("http://bigbasket.com/pd/40009755/euro-books-my-very-first-encyclopedia-of-animals-7-years/");
        processingInfoQueue.add("http://bigbasket.com/pd/40013867/collins-my-first-book-of-dinosaurs-72-pages/"); 
        processingInfoQueue.add("http://bigbasket.com/pd/40018256/megapolis-kitchen-knife-stainless-steel-3-pcs/"); 
        processingInfoQueue.add("http://bigbasket.com/pd/40004982/cafe-coffee-day-coffee-mug-black-coffee-times-1-pc/"); 
        
        processingInfoQueue.add("http://bigbasket.com/pd/40019333/jhilmil-royal-jhilmil-non-toxic-holi-colours-60-gm/");
        processingInfoQueue.add("http://bigbasket.com/pd/40019346/pichkari-pump-tank-3-ltr/"); 
        processingInfoQueue.add("http://bigbasket.com/pd/40018311/nayasa-strong-stool-517-cream-33x33x2009/");   
        processingInfoQueue.add("http://bigbasket.com/pd/40013746/royal-canin-junior-golden-retriever-3-kg/");  
        processingInfoQueue.add("http://bigbasket.com/pd/100006661/5010-detergent-bar-250-gm/");
        processingInfoQueue.add("http://bigbasket.com/pd/262837/henko-detergent-bar-stain-champion-250-gm/");  
        processingInfoQueue.add("http://bigbasket.com/pd/40018232/nayasa-round-dinner-set-deluxe-24-pcs/");
        
        processingInfoQueue.add("http://bigbasket.com/pd/272456/horlicks-womens-health-drink-chocolate-flavor-400-gm-jar/");  
        processingInfoQueue.add("http://bigbasket.com/pd/263574/tata-tea-life-with-tulsi-brahmi-cardamom-ginger-250-gm-pouch/"); 
        processingInfoQueue.add("http://bigbasket.com/pd/40008176/philips-essential-cfl-lamp-14-watt/");  
        processingInfoQueue.add("http://bigbasket.com/pd/240100/duracell-alkaline-battery-aa-4-pcs/");  
        processingInfoQueue.add("http://bigbasket.com/pd/101725/premier-kitchen-towel-4-pcs/");  
        processingInfoQueue.add("http://bigbasket.com/pd/40008829/prestige-ceramic-coated-curry-pan-with-lid-240-mm-diameter/"); 
        processingInfoQueue.add("http://bigbasket.com/pd/228623/surf-excel-detergent-powder-matic-front-load-2-kg/");  
        processingInfoQueue.add("http://bigbasket.com/pd/263975/harpic-toilet-cleaner-power-plus-rose-500-ml-bottle/"); 
        processingInfoQueue.add("http://bigbasket.com/pd/40019097/surf-excel-quick-wash-detergent-powder-12-gm-pouch/");
        processingInfoQueue.add("http://bigbasket.com/pd/40011745/gustora-spaghetti-durum-wheat-semolina-500-gm-pouch/"); 
        processingInfoQueue.add("http://bigbasket.com/pd/40002147/american-garden-spaghetti-pasta-sauce-mushroom-397-gm-bottle/"); 
        processingInfoQueue.add("http://bigbasket.com/pd/40018531/nestle-kit-kat-crisp-wafer-fingers-covered-with-chocolate-373-gm-pouch/"); 
        processingInfoQueue.add("http://bigbasket.com/pd/266170/maggi-nutri-licious-pazzta-cheese-macaroni-70-gm-pouch/");
        
        processingInfoQueue.add("http://bigbasket.com/pd/263921/colgate-dental-cream-200-gm/");  
        processingInfoQueue.add("http://bigbasket.com/pd/40019380/fresho-diced-papaya-200-gm/");  
        processingInfoQueue.add("http://bigbasket.com/pd/40005762/fresho-fruit-basket-medium-1-pc/");  
        processingInfoQueue.add("http://bigbasket.com/pd/10000031/fresho-banana-yelakki-semi-ripe-grade-a-1-kg/");  
        processingInfoQueue.add("http://bigbasket.com/pd/40015089/ambrosia-chilly-bajji-250-gm/");  
        processingInfoQueue.add("http://bigbasket.com/pd/40008406/tvs-organics-lemon-4-pcs/");  
        processingInfoQueue.add("http://bigbasket.com/pd/10000148/fresho-onion-medium-1-kg/");  
        processingInfoQueue.add("http://bigbasket.com/pd/40004389/first-agro-chilly-color-exotic-100-gm/");  
        processingInfoQueue.add("http://bigbasket.com/pd/10000148/fresho-onion-medium-1-kg/");   
        
        
        processingInfoQueue.add("http://bigbasket.com/pd/40009511/patanjali-cow-ghee-500-ml-carton/");  
        processingInfoQueue.add("http://bigbasket.com/pd/10000425/bb-royal-toor-dal-1-kg-pouch/");  
        processingInfoQueue.add("http://bigbasket.com/pd/20004530/wonderful-almonds-roasted-and-salted-200-gm-pouch/");  
        processingInfoQueue.add("http://bigbasket.com/pd/213273/nandini-pure-ghee-1-lt-pouch/");  
        processingInfoQueue.add("http://bigbasket.com/pd/126906/aashirvaad-atta-whole-wheat-10-kg-pouch/"); 
        processingInfoQueue.add("http://bigbasket.com/pd/40019401/melam-powder-coriander-500-gm-pouch/");  
        processingInfoQueue.add("http://bigbasket.com/pd/301974/24-mantra-organic-dal-tur-1-kg-pouch/");  
        processingInfoQueue.add("http://bigbasket.com/pd/20004911/best-special-rice-1-kg-pouch/");  
        processingInfoQueue.add("http://bigbasket.com/pd/241600/tata-salt-iodized-1-kg-pouch/");  
        processingInfoQueue.add("http://bigbasket.com/pd/40019280/parle-cake-vanilla-50-gm-pouch/");  
        processingInfoQueue.add("http://bigbasket.com/pd/242673/nandini-good-life-skimmed-milk-500-ml-pouch/");  
        processingInfoQueue.add("http://bigbasket.com/pd/150502/local-eggs-table-tray-30-pcs-pouch/");  
        processingInfoQueue.add("http://bigbasket.com/pd/40016286/soul-bakers-frozen-veggie-blast-pizza-11-inch/");  
        
        
        processingInfoQueue.add("http://bigbasket.com/pd/20003089/yeturu-aloe-vera-slim-fit-750-ml-bottle/");  
        processingInfoQueue.add("http://bigbasket.com/pd/40018759/similac-advance-stage-1-400-gm-jar/");  
        processingInfoQueue.add("http://bigbasket.com/pd/40018766/kdd-harvest-100-true-orange-juice-1-ltr-carton/");  
        processingInfoQueue.add("http://bigbasket.com/pd/40019291/bindu-packaged-drinking-water-1-ltr-bottle/");  
        processingInfoQueue.add("http://bigbasket.com/pd/20001251/24-mantra-organic-juice-apple-1-lt-carton/");  
        processingInfoQueue.add("http://bigbasket.com/pd/40019288/bindu-fizz-jeera-masala-1-ltr-bottle/");  
        processingInfoQueue.add("http://bigbasket.com/pd/102871/brooke-bond-red-label-tea-1-kg-pouch/");  
        processingInfoQueue.add("http://bigbasket.com/pd/40009525/patanjali-honey-500-gm-bottle/");  
        processingInfoQueue.add("http://bigbasket.com/pd/240155/nestle-nan-pro-stage-2-400-gm-carton/");  
        processingInfoQueue.add("http://bigbasket.com/pd/257034/hersheys-syrup-chocolate-623-gm-bottle/"); 
        processingInfoQueue.add("http://bigbasket.com/pd/40019268/parle-rajbhog-2-in-1-2455-gm-pouch/"); 
        processingInfoQueue.add("http://bigbasket.com/pd/40017895/kelloggs-corn-flakes-25-gm/");  
        processingInfoQueue.add("http://bigbasket.com/pd/168950/morton-baked-beans-in-tomato-sauce-450-gm-tin/"); 
        processingInfoQueue.add("http://bigbasket.com/pd/40018537/polo-the-mint-with-hole-312-gm-pouch/"); 
        processingInfoQueue.add("http://bigbasket.com/pd/40006374/pristine-millet-organica-500-gm-carton/");
        processingInfoQueue.add("http://bigbasket.com/pd/30006935/haldirams-rasmalai-500-gm/");  
        processingInfoQueue.add("http://bigbasket.com/pd/222010/nutella-cocoa-spread-hazelnut-350-gm-jar/");
        processingInfoQueue.add("http://bigbasket.com/pd/266109/maggi-noodles-masala-560-gm-pouch/");  
        processingInfoQueue.add("http://bigbasket.com/pd/40014260/phalada-pure-sure-organic-honey-250-gm-bottle/"); 
        processingInfoQueue.add("http://bigbasket.com/pd/298685/mtr-vermicelli-roasted-440-gm-pouch/"); 
        processingInfoQueue.add("http://bigbasket.com/pd/260427/priya-pickle-cut-mango-with-garlic-300-gm-bottle/");
        processingInfoQueue.add("http://bigbasket.com/pd/40018577/hi-tech-greens-green-peas-1-kg-pouch/");  
        processingInfoQueue.add("http://bigbasket.com/pd/152450/kissan-ketchup-fresh-tomato-1-kg-bottle/");
        processingInfoQueue.add("http://bigbasket.com/pd/40012930/bingo-yumitos-original-style-salt-sprinkled-35-gm-pouch/"); 


        processingInfoQueue.add("http://bigbasket.com/pd/100425760/huggies-pants-diapers-large-8-14-kg-38-pcs/");
        processingInfoQueue.add("http://bigbasket.com/pd/266657/axe-deodorant-body-spray-pulse-150-ml-bottle/"); 
        processingInfoQueue.add("http://bigbasket.com/pd/40010566/hanes-boxer-shorts-s-70-75cm/");  
        processingInfoQueue.add("http://bigbasket.com/pd/40019021/clinic-plus-hair-oil-daily-care-nourishing-100-ml-bottle/");  
        processingInfoQueue.add("http://bigbasket.com/pd/228623/surf-excel-detergent-powder-matic-front-load-2-kg/");
        processingInfoQueue.add("http://bigbasket.com/pd/40008836/prestige-veggi-cutter-1-pc/");  
        processingInfoQueue.add("http://bigbasket.com/pd/40019381/gourmet-delicacy-natural-tahini-350-gm-pet-jar/");  
        processingInfoQueue.add("http://bigbasket.com/pd/10000936/fresho-meats-mutton-leg-pieces-500-gm/");  
        processingInfoQueue.add("http://bigbasket.com/pd/10000900/fresho-meats-chicken-breast-boneless-500-gm/");  
        processingInfoQueue.add("http://bigbasket.com/pd/40019383/gruhashobe-mop-dry-1-pc/"); 
        processingInfoQueue.add("http://bigbasket.com/pd/40017938/kmb-waste-segregation-kit-2-bins-1-bag-1-pamphlet/"); 
        
        
        for(int i = 0; i < TOTAL_THREADS; i++)
        {
            bufferCleanerService.execute(new InfoCollector());
        }
    }
    
    private class InfoCollector implements Runnable
    {    
        public void run()
        {
           int tryCount = 0;
           while(!isShutDown)
           {
            try 
            {
                String url = processingInfoQueue.poll();
                if(null == url)
                {
                    tryCount++;
                    log.info("Nothing in queue, thread " + Thread.currentThread().getId() + " exiting");
                    if(tryCount > MAX_RETRY)
                        return;
                }
                log.info("queue size is " + processingInfoQueue.size());
                Product product =  PageInfoExtractor.getProduct(url);
                if(!productExists(product.getId()))
                {
                    insertData(product);
                }
                for(Entry<Long, String>  entry : product.getRelatedItems().entrySet())
                {
                    if(null == productIdMap.get(entry.getKey()))
                    {
                        productIdMap.put(entry.getKey(), entry.getKey());
                        processingInfoQueue.add(entry.getValue());
                    }
                }
            } 
            catch(Exception e)
            {
                errorCount++;
                log.error("error occured while processing product data errorcount " + errorCount , e);
            }
          }
        }
        /**
         * create table product
         * @param product
         * @return
         * @throws SQLException
         */
        public boolean insertData(Product product) throws SQLException
        {
            PreparedStatement insertStatement = connection.prepareStatement("insert into product ("
                    + "id, url, brand, title, image_url, about, saving, price, size_price_list, related_item) values (?,?,?,?,?,?,?,?,?,?);");
            insertStatement.setLong(1, product.getId());
            insertStatement.setString(2, product.getUrl());
            insertStatement.setString(3, product.getBrand());
            insertStatement.setString(4, product.getTitle());
            insertStatement.setString(5, product.getImageUrl());
            insertStatement.setString(6, product.getAbout());
            insertStatement.setString(7, product.getSaving());
            insertStatement.setString(8, product.getPrice());
            insertStatement.setString(9, gson.toJson(product.getSizePriceList()));
            insertStatement.setString(10, gson.toJson(product.getRelatedItems()));
            insertStatement.executeUpdate();
            return true;
        }
        
        public boolean productExists(long productId) throws SQLException
        {
            String query = "select id from product where id = " + productId;
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query);
            return resultSet.next();
        }
    }
}
