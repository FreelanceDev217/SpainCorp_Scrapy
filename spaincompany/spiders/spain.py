# -*- coding: utf-8 -*-
import scrapy
from lxml import html
from bs4 import BeautifulSoup
import requests, json
from lxml.html import fromstring

class SpainSpider(scrapy.Spider):
    name = 'spain'
    start_urls = [
        'https://www.einforma.com/informes-empresas/PROVINCIA.html',
    ]

    MAX_AREA_PER_PROVINCE = 0


    def get_proxies(self):
        url = 'https://free-proxy-list.net/'
        response = requests.get(url)

        parser = fromstring(response.text)
        proxies = set()
        for i in parser.xpath('//tbody/tr')[:100]:
            if i.xpath('.//td[7][contains(text(),"yes")]'):
                #Grabbing IP and corresponding PORT
                proxy = ":".join([i.xpath('.//td[1]/text()')[0], i.xpath('.//td[2]/text()')[0]])
                proxies.add(proxy)
        return proxies

    def parse(self, response):

        #proxies = self.get_proxies()
        
        ii = 0
        for province in response.xpath('//div[2]/div/ul/li/a/@href'):
            ii = ii + 1
            #if ii < 5:
            #    continue
            #if ii > 5:
            #    return
            province_url = province.get()
            #print(province_url)
            yield response.follow(province_url, self.parse_province)
            
        

    
    def parse_province(self, response):
        ii = 0

        for area in response.css('div.col50 ul li a::attr(href)'):
            ii = ii + 1
            if self.MAX_AREA_PER_PROVINCE > 0 and ii > self.MAX_AREA_PER_PROVINCE:
                return

            area_url = area.get()
            #print(area_url)
            yield response.follow(area_url, self.parse_area)
            

        main_nav_div = response.css('div.col02')[0]

        next_page = main_nav_div.css('ul li:nth-child(2) a[href]::attr(href)').get()
        if next_page is not None:
            #print(next_page)
            yield response.follow(next_page, self.parse_province)





    def parse_area(self, response):
        for company in response.xpath('//h3/a/@href'):
            company_url = company.get()
            #print(company_url)
            yield response.follow(company_url, self.parse_company)
            # return

        main_nav_div = response.css('div.col02')[1]

        next_page = main_nav_div.css('ul li:nth-child(2) a[href]::attr(href)').get()
        if next_page is not None:
            #print(next_page)
            # return
            yield response.follow(next_page, self.parse_area)



    def parse_company(self, response):
        #print(response.url)
        ret_dict = { 'url': response.url }

        
        current_url = response.url
        
        arr_th = response.css('th.cnae')    
        #if (len(arr_th) == 0):
        #    return    
        for th in arr_th:
            th_key = th.xpath("text()").get()
            td_val = th.xpath('../td[1]/node()').extract()
            soup = BeautifulSoup(' '.join(td_val))
            for script in soup(["script", "style"]):
                script.decompose()

            # td_val = html.fromstring(html_str).text_content().strip()
            td_val = soup.get_text().strip()

            #print(th_key)
            #print(td_val)
            ret_dict[th_key] =  td_val
            
        yield ret_dict

        #... temporary stop

        #next_url = current_url.replace("informacion-empresa", "informacion-comercial")
        #yield response.follow(next_url, self.parse_company_cominfo)

        #next_url = current_url.replace("informacion-empresa", "borme")
        #yield response.follow(next_url, self.parse_company_bormeinfo)

        

    def parse_company_cominfo(self, response):
        #print(response.url)
        ret_dict1 = { 'url': response.url }

        arr_th = response.css('th.cnae')        
        for th in arr_th:
            th_key = th.xpath("text()").get()
            td_val = th.xpath('../td[1]/node()').extract()
            soup = BeautifulSoup(' '.join(td_val))
            for script in soup(["script", "style"]):
                script.decompose()
            td_val = soup.get_text().strip()

            ret_dict1[th_key] =  td_val

        yield ret_dict1

    def parse_company_bormeinfo(self, response):
        #print(response.url)
        ret_dict2 = { 'url': response.url }
        
        arr_li = response.css('div.mod-content02-50 div ul li')

        #if (len(arr_li) == 0):
        #    return

        for li in arr_li:
            th_key = li.xpath("strong/text()").get().strip(":")
            td_vals = li.xpath("node()").extract()
            td_val = ""
            if len(td_vals) == 2:
                td_val = td_vals[1].strip()
            
            ret_dict2[th_key] =  td_val
        yield ret_dict2