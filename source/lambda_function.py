import sys
import json
import os
import re
import csv
import time
from datetime import datetime
import random, string
import googlemaps

import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

import ssl
ssl._create_default_https_context = ssl._create_unverified_context

import importer_util

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
_consoleHandler = logging.StreamHandler(sys.stdout)
_consoleHandler.setLevel(logging.INFO)
_simpleFormatter = logging.Formatter(
    fmt='%(levelname)-5s %(funcName)-20s %(lineno)4s: %(message)s'
)
_consoleHandler.setFormatter(_simpleFormatter)
logger.addHandler(_consoleHandler)

from retry import retry
import boto3
from boto3.dynamodb.conditions import Key

S3_BUCKET_NAME              = ""    if("S3_BUCKET_NAME" not in os.environ)          else os.environ["S3_BUCKET_NAME"]
DYNAMODB_NAME               = ""    if("DYNAMODB_NAME" not in os.environ)           else os.environ["DYNAMODB_NAME"]
APIKEY_GOOGLE_MAP           = ""    if("APIKEY_GOOGLE_MAP" not in os.environ)       else os.environ["APIKEY_GOOGLE_MAP"]


S3_BUCKET_NAME = "near-near-map"
DYNAMODB_NAME = "near-near-map-2"
APIKEY_GOOGLE_MAP = "AIzaSyAU57bLSlVhZYnJ8Tb-Ud3qFtiDthWQIM8"
ENDPOINT_ES = "search-near-near-map-2axzwunoy5zuzjjct3qojkaoqu.ap-northeast-1.es.amazonaws.com"

DYNAMO_TABLE                = boto3.resource("dynamodb").Table(DYNAMODB_NAME)
S3_SOURCE_BUCKET            = boto3.resource('s3').Bucket(S3_BUCKET_NAME)
S3_CLIENT                   = boto3.client("s3")
GMAPS                       = googlemaps.Client(key=APIKEY_GOOGLE_MAP)

from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

ELS = Elasticsearch(
    hosts=[{
        'host': ENDPOINT_ES,
        'port': 443
    }],
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
    timeout=1500
)

def lambda_handler(event, context):
    try:
        logger.info("start")
        files = getFilesFromS3("crawler/")
        index = 0
        for file in files:
            index += 1
            importProc(S3_BUCKET_NAME, file)

    except Exception as e:
        logger.exception(e)
        return {
            "statusCode": 500,
            "body": "error"
        }

@retry(tries=3, delay=1)
def getFilesFromS3(prefix):
    files = []
    objs = S3_SOURCE_BUCKET.meta.client.list_objects_v2(Bucket=S3_SOURCE_BUCKET.name, Prefix=prefix, Delimiter='/')
    for o in objs.get('Contents'):
        key = o.get('Key')
        if(key == prefix):
            continue
        files.append(key)
    return files

def importProc(bucketName, key):
    localPath = getFromS3(bucketName, key)
    fileName = os.path.basename(localPath)
    file = open(localPath, "r")
    reader = csv.reader(file)
    lineno = 0
    for csvLine in reader:
        lineno = lineno + 1
        logger.info(key + " : lineno = " + str(lineno))
        if(lineno <= 1):
            continue
        importLine(csvLine, fileName)

@retry(tries=3, delay=1)
def getFromS3(bucketName, key):
    logger.info(bucketName)
    logger.info(key)
    name = os.path.basename(key)
    localPath = os.path.join("/tmp", name)
    S3_CLIENT.download_file(Bucket = bucketName, Key = key, Filename = localPath)
    return localPath

def importLine(csvLine, fileName):
    try:
        data = convertCsv2Json(csvLine)
        if data == None:
            return
        
        # telに数字以外が入ってたり、11文字より長かったらクリアする
        if "tel" in data and data["tel"]:
            if data["tel"].isdecimal() == False:
                data["tel"] = ""
            elif len(data["tel"]) > 11:
                data["tel"] = ""

        # fire_hanabi_jpa_jp_members.csv は名前(title)と地域(address)から住所と緯度経度を取得して設定する
        if fileName == "fire_hanabi_jpa_jp_members.csv":
            setInfoForFireHanabiJP(data)
            if "latlon" not in data or not data["latlon"]:
                logger.warn("can't get info... {0}".format(data))
                return

        # 電話番号がない場合は住所から取得してみる
        if "tel" not in data or not data["tel"]:
            setTelAndLatLonToData(data)

        # 緯度経度をタイトルと住所から取得する
        if "latlon" not in data or not data["latlon"]:
            setLatLonToData(data)

        record = None
        if "tel" not in data or not data["tel"]:
            record = None
            logger.info("xxxxxxxxxxx")
        else:
            record = selectItem(data)
        # record = None
        if record == None or len(record) == 0:
            insertItem(data)
        else:
            updateItem(record[0]["_source"], data)

    except Exception as e:
        logger.error(data)
        logger.error(e)

@retry(tries=3, delay=1)
def setSiteToData(data):
    try:
        result = GMAPS.places(query="{0} {1}".format(data["title"], data["address"]))
        placeId = ""
        try:
            lat = result["results"][0]["geometry"]["location"]["lat"]
            lng = result["results"][0]["geometry"]["location"]["lng"]
            if "latlon" not in data or not data["latlon"]:
                data["latlon"] = "{0},{1}".format(lat, lng)
            placeId = result["results"][0]["place_id"]
        except Exception as e:
            logger.warn("can't get places : {0}".format(data))
            return

        detail  = GMAPS.place(place_id = placeId , fields = ["website"])
        resultDetail = detail["result"]
        if "website" in resultDetail and resultDetail["website"]:
            data["homepage"] = resultDetail["website"]
            importer_util.setSNSLinksToData(data["homepage"], data)

    except Exception as e:
        logger.error(data)
        logger.error(e)

@retry(tries=3, delay=1)
def setInfoForFireHanabiJP(data):
    try:
        result = GMAPS.places(query="{0} {1}".format(data["title"], data["address"]))
        placeId = ""
        try:
            lat = result["results"][0]["geometry"]["location"]["lat"]
            lng = result["results"][0]["geometry"]["location"]["lng"]
            data["latlon"] = "{0},{1}".format(lat, lng)
            placeId = result["results"][0]["place_id"]
        except Exception as e:
            logger.warn("can't get places : {0}".format(data))
            return
        
        detail  = GMAPS.place(place_id = placeId , fields = ["formatted_address", "website"])
        resultDetail = detail["result"]
        if "formatted_address" in resultDetail and resultDetail["formatted_address"]:
            data["address"] = resultDetail["formatted_address"]
        if "website" in resultDetail and resultDetail["website"]:
            data["homepage"] = resultDetail["website"]
            importer_util.setSNSLinksToData(data["homepage"], data)

    except Exception as e:
        logger.error(data)
        logger.error(e)

        
@retry(tries=3, delay=1)
def setTelAndLatLonToData(data):
    try:
        result = GMAPS.places(query="{0} {1}".format(data["address"], data["title"]))
        placeId = ""
        try:
            lat = result["results"][0]["geometry"]["location"]["lat"]
            lng = result["results"][0]["geometry"]["location"]["lng"]
            data["latlon"] = "{0},{1}".format(lat, lng)
            placeId = result["results"][0]["place_id"]
        except Exception as e:
            logger.warn("can't get places : {0}".format(data))
            return

        detail  = GMAPS.place(place_id = placeId , fields = ["formatted_phone_number"])
        try:
            data["tel"] = re.sub("[- ]", "", detail["result"]["formatted_phone_number"])
        except Exception as e:
            logger.warn("can't get phone_number : {0}".format(data))
            return

    except Exception as e:
        logger.error(data)
        logger.error(e)

@retry(tries=3, delay=1)
def selectItem(data):
    # return DYNAMO_TABLE.query(
    #     KeyConditionExpression=Key("type").eq(type) & Key("tel").eq(tel)
    # )

    body = None
    must = []
    must.append({"match": {"type": data["type"]}})
    if "tel" in data and data["tel"]:
        must.append({"match": {"tel": data["tel"]}})
        body = {
            "query": {
                "bool": {
                    "must": must
                }
            }
        }
    else:
        should = []
        if "locoguide_id" in data and data["locoguide_id"]:
            must.append({"match": {"locoguide_id": data["locoguide_id"]}})
        else:
            should.append({"match": {"title": {"query": data["title"], "fuzziness": "AUTO"}}})
            should.append({"match": {"address": {"query": data["address"], "fuzziness": "AUTO"}}})
        body = {
            "query": {
                "bool": {
                    "must": must,
                    "should": should
                }
            },
            "size": 1
        }

    result = None
    result = ELS.search(index="articles", body=body)

    if result == None:
        return None

    return result["hits"]["hits"]

def insertItem(data):
    # homepageがなければgoogleから探す
    if "homepage" not in data or not data["homepage"]:
        setSiteToData(data)

    # setLatLonToData(data)

    checkIFrameEnableItem(data)

    dtnow = datetime.now()
    rand = randomname(4)
    data["guid"] = dtnow.strftime("%Y%m%d-%H%M-00%S-") + dtnow.strftime("%f")[0:4] + rand
    
    insertItemD(data)

@retry(tries=3, delay=1)
def insertItemD(data):
    DYNAMO_TABLE.put_item(
      Item = data
    )
    logger.info("insert : " + data["title"])

@retry(tries=3, delay=1)
def updateItem(orgData, newData):
    # if "latlon" not in orgData:
    #    setLatLonToData(newData)
        
    checkIFrameEnableItem(newData)

    data = margeData(orgData, newData)
    if data == None:
        return
    
    # data["tokubai_id"] = newData["tokubai_id"]

    # homepageがなければgoogleから探す
    if "homepage" not in data or not data["homepage"]:
        setSiteToData(data)

    updateItemD(data)
    
@retry(tries=3, delay=1)
def updateItemD(data):
    logger.info(data)
    DYNAMO_TABLE.update_item(
        Key={
            "type": data["type"],
            "guid": data["guid"]
        },
        UpdateExpression="set #title = :title, #tel = :tel, #address = :address, #latlon = :latlon, #homepage = :homepage, #facebook = :facebook, #instagram = :instagram, #twitter = :twitter, #media1 = :media1, #media2 = :media2, #media3 = :media3, #media4 = :media4, #media5 = :media5, #has_xframe_options = :has_xframe_options, #image = :image, #locoguide_id = :locoguide_id, #star = :star",
        ExpressionAttributeNames={
            "#title": "title", 
            "#tel": "tel", 
            "#address": "address", 
            "#latlon": "latlon", 
            "#homepage": "homepage", 
            "#facebook": "facebook", 
            "#instagram": "instagram", 
            "#twitter": "twitter", 
            "#media1": "media1", 
            "#media2": "media2", 
            "#media3": "media3", 
            "#media4": "media4", 
            "#media5": "media5",
            "#has_xframe_options": "has_xframe_options",
            "#image": "image",
            "#locoguide_id": "locoguide_id",
            "#star": "star"
        },
        ExpressionAttributeValues={
            ":title": data["title"], 
            ":tel": data["tel"], 
            ":address": data["address"], 
            ":latlon": data["latlon"], 
            ":homepage": data["homepage"], 
            ":facebook": data["facebook"], 
            ":instagram": data["instagram"], 
            ":twitter": data["twitter"],
            ":media1": data["media1"], 
            ":media2": data["media2"], 
            ":media3": data["media3"], 
            ":media4": data["media4"], 
            ":media5": data["media5"],
            ":has_xframe_options": data["has_xframe_options"],
            ":image": data["image"],
            ":locoguide_id": data["locoguide_id"],
            ":star": data["star"]
        }
    )
    logger.info("update : " + data["title"] + " : " + data["guid"])
    
def setLatLonToData(data):
    if "latlon" in data:
        return

    try:
        result = GMAPS.geocode("{0} {1}".format(data["address"], data["title"]))
        if len(result) == 0:
            result = GMAPS.geocode("{0}".format(data["address"]))
        lat = result[0]["geometry"]["location"]["lat"]
        lng = result[0]["geometry"]["location"]["lng"]
        
        data["latlon"] = "{0},{1}".format(lat, lng)
    except Exception as e:
        logger.error(data)
        logger.error(e)
        raise

def margeData(org, new):
    try:
        data = {
            "type"      : org["type"], 
            "guid"      : org["guid"],
            "tel"       : org["tel"], 
            "title"     : org["title"], 
            "address"   : org["address"]
        }
        isUpdated = False
        isUpdated |= mergeItem(data, org, new, "locoguide_id")
        isUpdated |= mergeItem(data, org, new, "latlon")
        isUpdated |= mergeItem(data, org, new, "homepage")
        isUpdated |= mergeItem(data, org, new, "facebook")
        isUpdated |= mergeItem(data, org, new, "instagram")
        isUpdated |= mergeItem(data, org, new, "twitter")
        isUpdated |= mergeItem(data, org, new, "image")
        isUpdated |= mergeItem(data, org, new, "locoguide_id")
        isUpdated |= mergeItem(data, org, new, "star")
        
        if org["has_xframe_options"] != new["has_xframe_options"]:
            isUpdated = True
            data["has_xframe_options"] = new["has_xframe_options"]
        else:
            data["has_xframe_options"] = org["has_xframe_options"]

        data["media1"] = org["media1"]
        data["media2"] = org["media2"]
        data["media3"] = org["media3"]
        data["media4"] = org["media4"]
        data["media5"] = org["media5"]

        emptyKey = getEmptyMediaKey(data)
        if emptyKey and "media1" in new and new["media1"] and isNewMedia(data, new["media1"]):
            data[emptyKey] = new["media1"]
            isUpdated = True
        emptyKey = getEmptyMediaKey(data)
        if emptyKey and "media2" in new and new["media2"] and isNewMedia(data, new["media2"]):
            data[emptyKey] = new["media2"]
            isUpdated = True
        emptyKey = getEmptyMediaKey(data)
        if emptyKey and "media3" in new and new["media3"] and isNewMedia(data, new["media3"]):
            data[emptyKey] = new["media3"]
            isUpdated = True
        emptyKey = getEmptyMediaKey(data)
        if emptyKey and "media4" in new and new["media4"] and isNewMedia(data, new["media4"]):
            data[emptyKey] = new["media4"]
            isUpdated = True
        emptyKey = getEmptyMediaKey(data)
        if emptyKey and "media5" in new and new["media5"] and isNewMedia(data, new["media5"]):
            data[emptyKey] = new["media5"]
            isUpdated = True
            
        isUpdated = True
            
        if isUpdated == False:
            return None

        return data
        
    except Exception as e:
        logger.error(data)
        logger.error(e)
        return None

def getEmptyMediaKey(data):
    if "media1" in data and not data["media1"]:
        return "media1"
    elif "media2" in data and not data["media2"]:
        return "media2"
    elif "media3" in data and not data["media3"]:
        return "media3"
    elif "media4" in data and not data["media4"]:
        return "media4"
    elif "media5" in data and not data["media5"]:
        return "media5"
    return None

def isNewMedia(data, media):
    if data["media1"] == media:
        return False
    elif data["media2"] == media:
        return False
    elif data["media3"] == media:
        return False
    elif data["media4"] == media:
        return False
    elif data["media5"] == media:
        return False
    return True

def mergeItem(data, org, new, key):
    isUpdated = False
    if key in org and org[key] != "":
        data[key] = org[key]
    elif key in new and new[key] != "":
        data[key] = new[key]
        isUpdated = True
    else:
        data[key] = ""
    
    return isUpdated

def convertCsv2Json(csvLine):
    try:
        data = {
            "type"      : csvLine[0], 
            "tel"       : re.sub("[- ]", "", csvLine[1]), 
            "title"     : csvLine[2].replace("\u3000", " "), 
            "address"   : csvLine[3], 
            "homepage"  : csvLine[4], 
            "facebook"  : csvLine[5], 
            "instagram" : csvLine[6], 
            "twitter"   : csvLine[7],
            "media1"     : csvLine[8], 
            "media2"     : csvLine[9], 
            "media3"     : csvLine[10], 
            "media4"     : csvLine[11], 
            "media5"     : csvLine[12],
            "locoguide_id" : csvLine[13],
            "star"       : int(csvLine[14])
        }

        return data
        
    except Exception as e:
        logger.error(data)
        logger.error(e)
        return None

def randomname(n):
   randlst = [random.choice(string.ascii_letters + string.digits) for i in range(n)]
   return ''.join(randlst)
 
@retry(tries=3, delay=1)
def requestWithRetry(url):
    return requests.get(url, verify=False)
    
def checkIFrameEnable(type, item, flgs, index):
    optionFlg = 0
    if type in item and item[type].find("https") == 0:
        try:
            response = requestWithRetry(item[type])
            if "X-Frame-Options" in response.headers:
                optionFlg = 1
        except Exception as e:
            logger.error(e)
                
    flgs[index] = optionFlg
    
def checkIFrameEnableItem(item):
    has_xframe_options = [0, 0, 0, 0, 0, 0]
    
    if "has_xframe_options" in item:
        return

    try:
        index = 0
        checkIFrameEnable("homepage", item, has_xframe_options, index)
        index += 1
        checkIFrameEnable("media1", item, has_xframe_options, index)
        index += 1
        checkIFrameEnable("media2", item, has_xframe_options, index)
        index += 1
        checkIFrameEnable("media3", item, has_xframe_options, index)
        index += 1
        checkIFrameEnable("media4", item, has_xframe_options, index)
        index += 1
        checkIFrameEnable("media5", item, has_xframe_options, index)
        
        item["has_xframe_options"] = ",".join(map(str, has_xframe_options))
        
        # if item["title"].find("杏林堂") >= 0:
        #     item["image"] = "images/life/kyorindo.jpg"

    except Exception as e:
        logger.error(e)


def main():
    lambda_handler(None, None)
    
main()
