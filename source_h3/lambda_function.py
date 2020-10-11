import sys
import json
import os
import re
import csv
import time
from datetime import datetime
import random, string
import googlemaps
import slackweb
import copy
import h3
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

import ssl
ssl._create_default_https_context = ssl._create_unverified_context

import importer_util

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

from retry import retry
import boto3
from boto3.dynamodb.conditions import Key

S3_BUCKET_NAME          = ""    if("S3_BUCKET_NAME" not in os.environ)              else os.environ["S3_BUCKET_NAME"]
S3_PREFIX_IN            = ""    if("S3_PREFIX_IN" not in os.environ)                else os.environ["S3_PREFIX_IN"]
S3_PREFIX_IN_BACK       = ""    if("S3_PREFIX_IN_BACK" not in os.environ)           else os.environ["S3_PREFIX_IN_BACK"]
S3_PREFIX_OUT           = ""    if("S3_PREFIX_OUT" not in os.environ)               else os.environ["S3_PREFIX_OUT"]
DYNAMODB_NAME           = ""    if("DYNAMODB_NAME" not in os.environ)               else os.environ["DYNAMODB_NAME"]
APIKEY_GOOGLE_MAP       = ""    if("APIKEY_GOOGLE_MAP" not in os.environ)           else os.environ["APIKEY_GOOGLE_MAP"]
SLACK_WEBHOOK_HAMAMATSU = ""    if("SLACK_WEBHOOK_HAMAMATSU" not in os.environ)     else os.environ["SLACK_WEBHOOK_HAMAMATSU"]
ATHENA_DB_NAME          = ""    if("ATHENA_DB_NAME" not in os.environ)              else os.environ["ATHENA_DB_NAME"]
ATHENA_TABLE_NAME       = ""    if("ATHENA_TABLE_NAME" not in os.environ)           else os.environ["ATHENA_TABLE_NAME"]
ATHENA_OUTPUT_BUCKET    = ""    if("ATHENA_OUTPUT_BUCKET" not in os.environ)        else os.environ["ATHENA_OUTPUT_BUCKET"]
ATHENA_OUTPUT_PREFIX    = ""    if("ATHENA_OUTPUT_PREFIX" not in os.environ)        else os.environ["ATHENA_OUTPUT_PREFIX"]

DYNAMO_TABLE            = boto3.resource("dynamodb").Table(DYNAMODB_NAME)
S3_SOURCE_BUCKET        = boto3.resource('s3').Bucket(S3_BUCKET_NAME)
S3_CLIENT               = boto3.client("s3")
S3_RESOURCE             = boto3.resource("s3")
ATHENA                  = boto3.client("athena")
GMAPS                   = googlemaps.Client(key=APIKEY_GOOGLE_MAP)

from requests_aws4auth import AWS4Auth

def lambda_handler(event, context):
    try:
        logger.info("start")

        files = getFilesFromS3(S3_PREFIX_IN)
        if files is None or len(files) == 0:
            return {
                "statusCode": 304,
                "body": "Not Modified"
            }

        index = 0
        for file in files:
            index += 1
            success_count, faile_count = importProc(S3_BUCKET_NAME, file)
            backupProc(S3_BUCKET_NAME, file, S3_PREFIX_IN, S3_PREFIX_IN_BACK)
            message = "CSV IMPORTER : {0}\n success = {1}, failed = {2}".format(file, success_count, faile_count)
            notifyToSlack(SLACK_WEBHOOK_HAMAMATSU, message)

        return {
            "statusCode": 200,
            "body": ",".join(files)
        }

    except Exception as e:
        logger.exception(e)
        return {
            "statusCode": 500,
            "body": "error"
        }

@retry(tries=3, delay=1)
def notifyToSlack(webhook_url, text):
    slack = slackweb.Slack(url = webhook_url)
    slack.notify(text = text)

@retry(tries=3, delay=1)
def getFilesFromS3(prefix):
    if prefix[-1:] is not "/":
        prefix = prefix + "/"
    
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
    success_count = 0
    faile_count = 0
    for csvLine in reader:
        lineno = lineno + 1
        logger.info(key + " : lineno = " + str(lineno))
        if(lineno <= 1):
            continue
        if importLine(csvLine, fileName):
            success_count = success_count + 1
        else:
            faile_count = faile_count + 1
    
    return success_count, faile_count

@retry(tries=3, delay=1)
def backupProc(bucketName, key_org, prefix_org, prefix_back):
    key_back = key_org.replace(prefix_org, prefix_back, 1)
    S3_CLIENT.copy_object(Bucket=bucketName, Key=key_back, CopySource={'Bucket': bucketName, 'Key': key_org})
    S3_CLIENT.delete_object(Bucket=bucketName, Key=key_org)

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

        # 電話番号がない場合は住所から取得してみる
        if "tel" not in data or not data["tel"]:
            setTelAndLatLonToData(data)

        # 緯度経度をタイトルと住所から取得する
        if "latlon" not in data or not data["latlon"]:
            setLatLonToData(data)
            
        # 既に登録済みか確認する
        latlon_ary = data["latlon"].split(",")
        h3index9 = h3.geo_to_h3(float(latlon_ary[0]), float(latlon_ary[1]), 9)
        records = selectItem(data, h3index9)
        
        if records is None or len(records) == 0:
            insertItem(data, h3index9)

        elif len(records) == 1:
            updateItem(records[0], data)

        elif not data["tel"]:
            message = "CSV IMPORTER : {0}\n Alert : MULTIPLE NO TELEPHONE\n {1}".format(fileName, data["title"])
            notifyToSlack(SLACK_WEBHOOK_HAMAMATSU, message)
            
        else:
            message = "CSV IMPORTER : {0}\n Alert : MULTIPLE RECORDS({1}) {2}\n {3}\n {4}".format(fileName, len(records), data["title"], records[0], records[1])
            notifyToSlack(SLACK_WEBHOOK_HAMAMATSU, message)

        return True
    except Exception as e:
        logger.exception(e)
        message = "CSV IMPORTER : {0}\n Exception : {1}".format(fileName, e.__class__.__name__)
        notifyToSlack(SLACK_WEBHOOK_HAMAMATSU, message)
        return False

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
        logger.exception(e)

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
        logger.exception(e)

@retry(tries=3, delay=1)
def selectItem(data, h3index9):
    
    try:
        query = "SELECT * FROM \"{0}\"".format(ATHENA_TABLE_NAME) + \
                " where \"p_type\" = '{0}' and \"p_h3_9\" like '{1}' and \"tel\" = '{2}' limit 10;".format(data["type"], h3index9, data["tel"])
        athenaLocation = os.path.join("s3://", ATHENA_OUTPUT_BUCKET, ATHENA_OUTPUT_PREFIX)
        logger.info(query)
        response = ATHENA.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                "Database": ATHENA_DB_NAME
            },
            ResultConfiguration={
                "OutputLocation": athenaLocation
            }
        )

        
        QueryExecutionId = response['QueryExecutionId']
        time.sleep(3)
        result = poll_status(QueryExecutionId)
        
        if result['QueryExecution']['Status']['State'] == 'SUCCEEDED':
    
            s3_key = os.path.join(ATHENA_OUTPUT_PREFIX, QueryExecutionId + '.csv')
            local_filename = "/tmp/" + QueryExecutionId + '.csv'
    
            # download result file
            S3_RESOURCE.Bucket(ATHENA_OUTPUT_BUCKET).download_file(s3_key, local_filename)

            # read file to array
            rows = []
            with open(local_filename) as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    rows.append(row)
    
            # delete result file
            if os.path.isfile(local_filename):
                os.remove(local_filename)

        return rows

    except Exception as e:
        logger.error(data)
        logger.exception(e)

    return None

@retry(tries=5, delay=2)
def poll_status(_id):
    result = ATHENA.get_query_execution( QueryExecutionId = _id )
    state  = result['QueryExecution']['Status']['State']
    if state == 'SUCCEEDED':
        return result
    elif state == 'FAILED':
        return result
    else:
        raise Exception
        
def insertItem(data, h3index9):
    latlon_ary = data["latlon"].split(",")
    h3index8 = h3.geo_to_h3(float(latlon_ary[0]), float(latlon_ary[1]), 8)
    h3index7 = h3.geo_to_h3(float(latlon_ary[0]), float(latlon_ary[1]), 7)
    h3index6 = h3.geo_to_h3(float(latlon_ary[0]), float(latlon_ary[1]), 6)
    dtnow = datetime.now()
    time = dtnow.strftime("%Y%m%d-%H%M%S") + dtnow.strftime("%f")[0:3]
    data["h3-9"] = h3index9 + "_" + time
    data["h3-8"] = h3index8
    data["h3-7"] = h3index7
    data["h3-6"] = h3index6

    # homepageがなければgoogleから探す
    if "homepage" not in data or not data["homepage"]:
        setSiteToData(data)

    checkIFrameEnableItem(data)
    insertItemD(data)

@retry(tries=3, delay=1)
def insertItemD(data):
    DYNAMO_TABLE.put_item(
      Item = data
    )
    logger.info("insert : " + data["title"])

def updateItem(orgData, newData):
    data = margeData(orgData, newData)
    if data == None:
        return

    # homepageがなければgoogleから探す
    if "homepage" not in data or not data["homepage"]:
        setSiteToData(data)

    checkIFrameEnableItem(data)
    updateItemD(data)
    
    return data
    
@retry(tries=3, delay=1)
def updateItemD(data):
    DYNAMO_TABLE.update_item(
        Key={
            "type": data["type"],
            "h3-9": data["h3-9"]
        },
        UpdateExpression="set #h3_8 = :h3_8, #h3_7 = :h3_7, #h3_6 = :h3_6, #title = :title, #tel = :tel, #address = :address, #latlon = :latlon, #homepage = :homepage, #facebook = :facebook, #instagram = :instagram, #twitter = :twitter, #media1 = :media1, #media2 = :media2, #media3 = :media3, #media4 = :media4, #media5 = :media5, #has_xframe_options = :has_xframe_options, #image = :image, #locoguide_id = :locoguide_id, #star = :star",
        ExpressionAttributeNames={
            "#h3_8": "h3-8", 
            "#h3_7": "h3-7", 
            "#h3_6": "h3-6", 
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
            ":h3_8": data["h3-8"], 
            ":h3_7": data["h3-7"], 
            ":h3_6": data["h3-6"], 
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
    logger.info("update : " + data["title"] + " : " + data["h3-9"])
    
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
        logger.exception(e)
        raise

def margeData(org, new):
    try:
        data = {
            "type"      : org["type"], 
            "h3-9"      : org["h3-9"],
            "h3-8"      : org["h3-8"],
            "h3-7"      : org["h3-7"],
            "h3-6"      : org["h3-6"],
            "title"     : org["title"], 
            "address"   : org["address"],
            "latlon"    : org["latlon"]
        }
        isUpdated = False
        isUpdated |= mergeItem(data, org, new, "tel")
        isUpdated |= mergeItem(data, org, new, "homepage")
        isUpdated |= mergeItem(data, org, new, "facebook")
        isUpdated |= mergeItem(data, org, new, "instagram")
        isUpdated |= mergeItem(data, org, new, "twitter")
        isUpdated |= mergeItem(data, org, new, "image")
        isUpdated |= mergeItem(data, org, new, "locoguide_id")

        starOrg = 0
        starNew = 0
        if "star" in org:
            starOrg = int(org["star"])
        if "star" in new:
            starNew = int(new["star"])
        
        if starOrg == 1 or starNew == 1:
            data["star"] = 1
        else:
            data["star"] = 0
        
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
        logger.exception(e)
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
            "locoguide_id" : csvLine[13]
        }
        
        if str.isdecimal(csvLine[14]):
            data["star"] = int(csvLine[14])
        else:
            data["star"] = 0

        return data
        
    except Exception as e:
        logger.exception(e)
        return None

def randomname(n):
   randlst = [random.choice(string.ascii_letters + string.digits) for i in range(n)]
   return ''.join(randlst)
 
@retry(tries=2, delay=1)
def requestWithRetry(url):
    return requests.get(url, verify=False, timeout=(5.0, 5.0))
    
def checkIFrameEnable(type, item, flgs, index):
    optionFlg = 0
    if type in item and item[type].find("https") == 0:
        try:
            response = requestWithRetry(item[type])
            if "X-Frame-Options" in response.headers:
                optionFlg = 1
        except Exception as e:
            logger.exception(e)
                
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
        logger.exception(e)

