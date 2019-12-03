#!/usr/bin/python
# -*- coding: utf-8 -*-

import requests, json
from lxml.html import fromstring
def get_proxies():
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

def get_proxyrotator_proxies():
    url = 'http://falcon.proxyrotator.com:51337/proxy-list/'

    params = dict(
        apiKey='X8MvSphLxYdEFmPKob3u52WyTZJQwnkf'
    )

    resp = requests.get(url=url, params=params)
    # data = json.loads(resp.text)
    data = resp.text.splitlines()
    print (data)
    return data

def get_proxyrotator_proxy(count):
    proxies = set()

    for i in range(count):
        url = 'http://falcon.proxyrotator.com:51337/'
        params = dict(
            apiKey='X8MvSphLxYdEFmPKob3u52WyTZJQwnkf'
        )
        resp = requests.get(url=url, params=params)
        data = json.loads(resp.text)
        proxies.add(data['proxy'])
    return proxies

def fetch_all(endpage=2, https=False):
    proxies = []
    proxies += get_proxies()
    #proxies += get_proxyrotator_proxies()
    #proxies += get_proxyrotator_proxy(count = 10)
    return proxies

if __name__ == '__main__':
    import sys
    proxies = fetch_all()
    #print check("202.29.238.242:3128")
    for p in proxies:
        print (p)