# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import os
import json
import csv
from scrapy.exceptions import DropItem

class CrawlstvlPipeline:
    def process_item(self, item, spider):
        return item
    
# class JsonPipeline:
  
    def open_spider(self, spider):
        # directory = '/Workspace/study/BigData/OnCK/PhanHaiTrieu20089981/data/crawlData'
        directory = '/opt/airflow/data/crawlData'
        
        json_file_path = os.path.join(directory, 'phanhaitrieu_dataSTVL.json')
        if not os.path.exists(directory):
            os.makedirs(directory)
        self.file = open(json_file_path, "w", encoding="utf-8")
        self.items = []
        # Mở file khi spider bắt đầu
        # self.file = open("phanhaitrieu_data.json", "w", encoding="utf-8")
        # self.items = []  # Khởi tạo một list để lưu trữ các item

    def process_item(self, item, spider):
        # Thêm item vào list thay vì ghi ngay vào file
        self.items.append(dict(item))
        return item

    def close_spider(self, spider):
        # Khi spider kết thúc, ghi toàn bộ items vào file JSON một lần
        json.dump(self.items, self.file, ensure_ascii=False, indent=4)
        self.file.close()
        
class CsvPipeline:
  
    def open_spider(self, spider):
        # directory = '/Workspace/study/BigData/OnCK/PhanHaiTrieu20089981/data/crawlData'
        directory = '/opt/airflow/data/crawlData'
        
        csv_file_path = os.path.join(directory, 'phanhaitrieu_dataSTVL.csv')
        if not os.path.exists(directory):
            os.makedirs(directory)
        self.file = open(csv_file_path, "w", encoding="utf-8", newline='')
        self.writer = csv.DictWriter(self.file, fieldnames=[], extrasaction='ignore')
        self.writer.writeheader()
        # Mở file khi spider bắt đầu

    def process_item(self, item, spider):
        # Lấy danh sách tên cột từ item đầu tiên nếu chưa có
        if not self.writer.fieldnames:
            self.writer.fieldnames = item.keys()
            self.writer.writeheader()
        # Ghi item vào file CSV
        self.writer.writerow(item)
        return item

    def close_spider(self, spider):
        # Đóng tệp CSV khi spider kết thúc
        self.file.close()
