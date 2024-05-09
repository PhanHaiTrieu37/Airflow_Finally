import scrapy
from ..items import CrawlstvlItem
from scrapy.crawler import CrawlerProcess
from scrapy.utils.reactor import install_reactor
from multiprocessing.context import Process
install_reactor("twisted.internet.asyncioreactor.AsyncioSelectorReactor")
install_reactor("twisted.internet.selectreactor.SelectReactor")

class StvlSpider(scrapy.Spider):
    name = "STVL"
    allowed_domains = ["sieuthivieclam.vn"]
    start_urls = ["https://sieuthivieclam.vn"]

    def start_requests(self):
        url = "https://sieuthivieclam.vn/tim-viec-lam.html"
        yield scrapy.Request(url, self.parse)

    def parse(self, response):
        URLs = response.xpath('//div[@class="job-list"]/descendant::div/h3/a/@href').getall()
        for rawURL in URLs:
            item = CrawlstvlItem()
            domainURL = "https://sieuthivieclam.vn/"
            URLCrawl = domainURL + rawURL
            item["link"] = URLCrawl   
            request = scrapy.Request(URLCrawl,self.parse_question)
            request.meta["item"] = item
            yield request

    def parse_question(self, response):
        item = response.meta["item"]
        JobName = response.xpath("normalize-space(string(//body/div[@id='wrapper']/main[@id='main']/div[1]/div[1]/div[1]/div[1]/div[1]/h1[1]))").get() 
        CompanyName = response.xpath("normalize-space(string(//body/div[@id='wrapper']/main[@id='main']/div[1]/div[1]/div[1]/div[1]/div[1]/h3))").get()  
        Salary = response.xpath("normalize-space(string(//body/div[@id='wrapper']/main[@id='main']/div[1]/div[1]/div[1]/div[1]/div[1]/div[2]/ul[1]/li[4]/span[@class='t-data']))").get()   
        Quantity = response.xpath("normalize-space(string(//body/div[@id='wrapper']/main[@id='main']/div[1]/div[1]/div[1]/div[1]/div[1]/div[2]/ul[1]/li[5]/span[@class='t-data']))").get() 
        Deadline = response.xpath("normalize-space(string(//body/div[@id='wrapper']/main[@id='main']/div[1]/div[1]/div[1]/div[1]/div[1]/div[2]/ul[1]/li[10]/span[@class='t-data']))").get()
        
        item["JobName"] = JobName
        item["CompanyName"] = CompanyName   
        item["Salary"] = Salary
        item["Quantity"] = Quantity
        item["Deadline"] = Deadline
        yield item 
        
#  Run the spiders
def crawl():
    crawler = CrawlerProcess(settings={
    "CONCURRENT_REQUESTS": 3,
    "DOWNLOAD_TIMEOUT": 60,
    "RETRY_TIMES": 5,
    "ROBOTSTXT_OBEY": False,
    "USER_AGENT": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    })
    crawler.crawl(StvlSpider)
    crawler.start()
    
process = Process(target=crawl)
process.start()
process.join()

# process = CrawlerProcess(settings={
#     "CONCURRENT_REQUESTS": 3,
#     "DOWNLOAD_TIMEOUT": 60,
#     "RETRY_TIMES": 5,
#     "ROBOTSTXT_OBEY": False,
#     "USER_AGENT": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
# })

# process.crawl(StvlSpider)
# process.start()
