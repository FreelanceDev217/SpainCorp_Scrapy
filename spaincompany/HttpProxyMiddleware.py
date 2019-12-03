#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
import logging
from datetime import datetime, timedelta
from twisted.web._newclient import ResponseNeverReceived
from twisted.internet.error import TimeoutError, ConnectionRefusedError, ConnectError
from spaincompany import fetch_free_proxies
from random import randint

logger = logging.getLogger(__name__)

class HttpProxyMiddleware(object):
    DONT_RETRY_ERRORS = (TimeoutError, ConnectionRefusedError, ResponseNeverReceived, ConnectError, ValueError)

    def __init__(self, use_https):
        self.last_no_proxy_time = datetime.now()
        self.recover_interval = 20
        self.dump_count_threshold = 20
        self.proxy_file = "proxies.dat"
        self.invalid_proxy_flag = True
        self.extend_proxy_threshold = 10
        self.proxies = [{"proxy": None, "valid": True, "count": 0}]
        self.proxy_index = 0
        self.fixed_proxy = len(self.proxies)
        self.last_fetch_proxy_time = datetime.now()
        self.fetch_proxy_interval = 120
        self.invalid_proxy_threshold = 200
        self.use_https = use_https

        # self.fetch_new_proxies()
        if os.path.exists(self.proxy_file):
            with open(self.proxy_file, "r") as fd:
                lines = fd.readlines()            
                for line in lines:
                    line = line.strip()
                    if not line or self.url_in_proxies(line):
                        continue
                    self.proxies.append({"proxy": line,
                                        "valid": True,
                                        "count": 0})

    @classmethod
    def from_crawler(cls, crawler):
        use_https = crawler.settings.getbool('HTTPS_PROXY')
        return cls(use_https)

    def url_in_proxies(self, url):
        """
        """
        for p in self.proxies:
            if url == p["proxy"]:
                return True
        return False

    def reset_proxies(self):
        """
        """
        logger.info("reset proxies to valid")
        for p in self.proxies:
            if p["count"] >= self.dump_count_threshold:
                p["valid"] = True

    def fetch_new_proxies(self):
        """
        """
        logger.info("extending proxies using fetch_free_proxies.py")
        new_proxies = fetch_free_proxies.fetch_all(https=self.use_https)
        logger.info("new proxies: %s" % new_proxies)
        self.last_fetch_proxy_time = datetime.now()

        for np in new_proxies:
            if self.url_in_proxies(np):
                continue
            else:
                self.proxies.append({"proxy": np,
                                     "valid": True,
                                     "count": 0})
        if self.len_valid_proxy() < self.extend_proxy_threshold: 
            self.extend_proxy_threshold -= 1

    def len_valid_proxy(self):
        """
        """
        count = 0
        for p in self.proxies:
            if p["valid"]:
                count += 1
        return count

    def inc_proxy_index(self, current=-1):
        """
        """
        assert self.proxies[0]["valid"]
        if current != -1 and self.proxy_index != current: 
            return
        while True:
            #self.proxy_index = (self.proxy_index + 1) % len(self.proxies)
            #self.proxy_index = 0
            # if randint(0, 30) >= 29:
            self.proxy_index = randint(0, len(self.proxies) - 1)
            if self.proxies[self.proxy_index]["valid"]:
                break

        if self.proxy_index == 0 and datetime.now() > self.last_fetch_proxy_time + timedelta(minutes=10):
            logger.info("captcha thrashing")
            self.fetch_new_proxies()

        if self.len_valid_proxy() <= self.fixed_proxy or self.len_valid_proxy() < self.extend_proxy_threshold: 
            self.reset_proxies()

        if self.len_valid_proxy() < self.extend_proxy_threshold: # 
            logger.info("valid proxy < threshold: %d/%d" % (self.len_valid_proxy(), self.extend_proxy_threshold))
            self.fetch_new_proxies()

        logger.info("now using new proxy: %s" % self.proxies[self.proxy_index]["proxy"])

        #if datetime.now() > self.last_fetch_proxy_time + timedelta(minutes=self.fetch_proxy_interval):
        #    logger.info("%d munites since last fetch" % self.fetch_proxy_interval)
        #    self.fetch_new_proxies()

    def set_proxy(self, request):
        """
        """
        # proxy = self.proxies[self.proxy_index]
        
        # if not proxy["valid"]:
        #     self.inc_proxy_index()
        #     proxy = self.proxies[self.proxy_index]

        if "no_proxy" in request.meta.keys():
            # del request.meta["proxy"]
            request.meta["proxy_index"] = self.proxy_index
            return

        self.inc_proxy_index()
        proxy = self.proxies[self.proxy_index]

        if self.proxy_index == 0: # 
            self.last_no_proxy_time = datetime.now()

        if proxy["proxy"]:
            request.meta["proxy"] = "http://" + proxy["proxy"]
        elif "proxy" in request.meta.keys():
            del request.meta["proxy"]
        request.meta["proxy_index"] = self.proxy_index
        proxy["count"] += 1

    def invalid_proxy(self, index):
        """
        """
        if index < self.fixed_proxy: 
            logger.info("fixed proxy will not be invalid: %s" % self.proxies[index])
            self.inc_proxy_index(index)
            return

        if self.proxies[index]["valid"]:
            logger.info("invalidate %s" % self.proxies[index])
            self.proxies[index]["valid"] = False
            if index == self.proxy_index:
                self.inc_proxy_index()

            if self.proxies[index]["count"] < self.dump_count_threshold:
                self.dump_valid_proxy()

    def dump_valid_proxy(self):
        """
        """
        if self.dump_count_threshold <= 0:
            return
        logger.info("dumping proxies to file")
        with open(self.proxy_file, "w") as fd:
            for i in range(self.fixed_proxy, len(self.proxies)):
                p = self.proxies[i]
                if p["valid"] or p["count"] >= self.dump_count_threshold:
                    fd.write(p["proxy"]+"\n") 

    def process_request(self, request, spider):
        """
        """
        if self.proxy_index > 0  and datetime.now() > (self.last_no_proxy_time + timedelta(minutes=self.recover_interval)):
            logger.info("After %d minutes later, recover from using proxy" % self.recover_interval)
            self.last_no_proxy_time = datetime.now()
            self.proxy_index = 0
        request.meta["dont_redirect"] = True  

        if "change_proxy" in request.meta.keys() and request.meta["change_proxy"]:
            logger.info("change proxy request get by spider: %s" % request)
            self.invalid_proxy(request.meta["proxy_index"])
            request.meta["change_proxy"] = False

        self.set_proxy(request)

    def process_response(self, request, response, spider):
        """
        """
        if "proxy" in request.meta.keys():
            logger.debug("%s %s %s" % (request.meta["proxy"], response.status, request.url))
        else:
            logger.debug("None %s %s" % (response.status, request.url))

        if response.status != 200 \
                and (not hasattr(spider, "website_possible_httpstatus_list") \
                             or response.status not in spider.website_possible_httpstatus_list):
            logger.info("response status[%d] not in spider.website_possible_httpstatus_list" % response.status)
            self.invalid_proxy(request.meta["proxy_index"])
            new_request = request.copy()
            new_request.dont_filter = True
            return new_request
        else:
            return response

    def process_exception(self, request, exception, spider):
        """
        """
        logger.debug("%s exception: %s" % (self.proxies[request.meta["proxy_index"]]["proxy"], exception))
        request_proxy_index = request.meta["proxy_index"]

        if isinstance(exception, self.DONT_RETRY_ERRORS):
            if request_proxy_index > self.fixed_proxy - 1 and self.invalid_proxy_flag: 
                if self.proxies[request_proxy_index]["count"] < self.invalid_proxy_threshold:
                    self.invalid_proxy(request_proxy_index)
                elif request_proxy_index == self.proxy_index:  
                    self.inc_proxy_index()
            else:              
                if request.meta["proxy_index"] == self.proxy_index:
                    self.inc_proxy_index()
            new_request = request.copy()
            new_request.dont_filter = True
            return new_request