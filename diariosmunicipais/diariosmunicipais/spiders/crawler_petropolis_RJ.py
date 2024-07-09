import scrapy
from pymongo import MongoClient
from datetime import datetime
import locale
import os
import re
import pdb

class PetropolisSpider(scrapy.Spider):
    name = "RJ_petropolis"
    base_url = "https://www.petropolis.rj.gov.br"
    delta = '2024-04-04'
    start_urls = [
        "https://www.petropolis.rj.gov.br/pmp/index.php/servicos-cidadao/diario-oficial/category/298"
    ]

    def start_requests(self):
        url = f'https://www.petropolis.rj.gov.br/pmp/index.php/servicos-cidadao/diario-oficial/category/298'
        yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        locale.setlocale(locale.LC_TIME, 'pt_BR.utf8')
        diaries_selectors = response.xpath('//div[@class="table-responsive"]/table[@class="table table-striped table-hover tabela-do"]/tbody/tr')

        for diary in diaries_selectors:
            date_initial = diary.xpath('./th/a/text()').get()
            day_month_year = date_initial.split(', ')
            date_month = day_month_year[1]
            date = datetime.strptime(date_month, "%d de %B de %Y").strftime('%Y-%m-%d')
            if date >= self.delta:
                edition_init = diary.xpath('./th/a/text()').get()
                if ' – ' in edition_init:
                    edition = edition_init.split(' – ')[0].strip()
                else:
                    edition = edition_init.strip()
                source_id = edition
                search_url = diary.xpath(".//a/@href").extract_first()
                meta = {
                    'date': date,
                    'edition': edition,
                    'source_id': source_id
                }
                url = self.base_url + search_url
                yield scrapy.Request(url=url, meta=meta, callback=self.save_document)

        yield from self.next_month(response)

    def save_pdf(self, response):
        pdf_content = response.body
        source_id = response.meta.get('source_id')
        name = f'{self.name}_{source_id}.pdf'
        documents_folder = os.path.join(os.path.dirname(__file__), '..', '..', 'Documents')
        folder_path = os.path.join(documents_folder, self.name)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        file_path = os.path.join(folder_path, name)
        with open(file_path, 'wb') as f:
            f.write(pdf_content)
        return file_path
        # salvar e retornar o file_path
        # quando houver páginas viradas, é importante deixar observação na planilha

    def save_document(self, response):
        client = MongoClient('localhost',27017)
        bd = client['executiveorder']
        collections = bd['petropolis_RJ']
        file_path = self.save_pdf(response)
        document = {
            'source': self.name,
            'date': response.meta.get('date'),
            'edition': response.meta.get('edition'),
            'source_id': response.meta.get('source_id'), # REGEX colocar no formato EX2024...
            'url': response.url,     #
            'file_path': file_path,
        }
        collections.insert_one(document)
        return document

    def next_month(self, response):
        current_page = response.url
        current_month = int(current_page.split('/category/')[-1])
        month_construction = 1
        month = current_month + month_construction
        url = f'https://www.petropolis.rj.gov.br/pmp/index.php/servicos-cidadao/diario-oficial/category/{month}'
        if url:
            yield scrapy.Request(url=url, callback=self.parse)
