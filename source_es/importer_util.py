import sys
import json
import os
import re
import csv
import logging
logger = logging.getLogger()

import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
from bs4 import BeautifulSoup
from retry import retry

def setSNSLinksToData(url, data):
    if url.find("facebook.com/") > 0 or url.find("instagram.com/") > 0 or url.find("twitter.com/") > 0:
        return
    
    soup = getBeautifulSoup(url)
    aList = soup.find_all("a")
    for aItem in aList:
        try:
            link = aItem["href"]
            setLinkToData(link, data)
        except:
            continue
        
@retry(tries=3, delay=3)
def getBeautifulSoup(url):
    response = requests.get(url, verify=False)
    response.encoding = response.apparent_encoding
    soup = BeautifulSoup(response.text, "html.parser")
    logger.info("soup link  : {0}".format(url))
    logger.info("soup title : {0}".format(soup.title))
    return soup

def setLinkToData(url, data):
    if url.find("facebook.com/") > 0 and url.find(".php?") < 0:
        if isPage(url, "facebook.com"):
            data["facebook"] = trimParam(url)
    elif url.find("instagram.com/") > 0:
        if isPage(url, "instagram.com"):
            data["instagram"] = trimParam(url)
    elif url.find("twitter.com/") > 0 and url.find("twitter.com/share") < 0:
        if isPage(url, "twitter.com"):
            data["twitter"] = trimParam(url)

def trimParam(url):
    if url.find("?") > 0:
        url = url[:url.find("?")]
    return url

def isPage(url, domain):
    tmp = url.replace("https://", "").replace("www", "").replace(domain, "").replace(".", "").replace("/", "")
    return len(tmp) > 1
