import time
import pandas as pd

from main import Connect
from main.RecordLog import record, recordOnlyOne
from bson.objectid import ObjectId
from main.analysis_tool import AnalysisTool

queryNum = 0


def join_table_query(config_content: dict):
    global queryNum
    dbname = config_content["dbName"]
    print("username:{},dbname:{}".format(config_content["userName"], dbname))
    number, unit, skip = inputParameter(config_content)
    connectMongoDB = Connect.MongoDBConnect(config_content, dbname)
    queryCondition1 = config_content["mianQueryCondition1"]
    queryCondition1['_id']['$gte'] = ObjectId(queryCondition1['_id']['$gte'])
    queryCondition2 = config_content["mianQueryCondition2"]
    queryCondition2['_id']['$gt'] = ObjectId(queryCondition2['_id']['$gt'])
    jion_table = config_content["jionDataSet"]
    for data_index in range(len(jion_table)):
        recordOnlyOne(config_content["mainDataSet"] + " jion table " + "【" + jion_table[data_index] + "】")
        for num in range(number):
            if num == 0:
                content = connectMongoDB.query(config_content["mainDataSet"], queryCondition1, unit, 0)
            else:
                content = connectMongoDB.query(config_content["mainDataSet"], queryCondition2, unit, 0)
            # last_id = contentQuery(config_content, content, jion_table[data_index], connectMongoDB)
            last_id = contentQueryNoAnalysisHost(config_content, content, jion_table[data_index], connectMongoDB)
            record('query', num * unit + len(content) + skip, content[-1]['_id'])
            record('insert', num * unit + len(content) + skip, last_id)
            queryCondition2['_id']['$gt'] = content[-1]['_id']
            print("查询进度：" + str(int(num * unit + len(content) + skip)) + ": 连接量--" + str(queryNum))

    return number * unit

def contentQuery(config_content: dict, content: list, jion_table, connectMongoDB):
    global queryNum
    queryCondition = config_content["jionQueryCondition"]
    last_id = "None"
    # listing_id = []
    # for index in range(len(content)):
    #     listing_id.append(content[index]["HOUSE_ID"])
    # queryCondition['listing_id']['$in'] = listing_id
    # query_content = connectMongoDB.query(jion_table, queryCondition, 0, 0, log=False)
    # queryNum = queryNum + len(query_content)
    # if len(query_content) == 0: return last_id
    # # insert_content = calendar_analysis(query_content, content, index, jion_table)
    # # insert_content = review_analysis(query_content, content, index, jion_table)
    # last_id = connectMongoDB.insert(config_content["outDataSet"], query_content, log=False)
    for index in range(len(content)):
        queryCondition['listing_id'] = content[index]["HOUSE_ID"]
        query_content = connectMongoDB.query(jion_table, queryCondition, 0, 0, log=False)
        queryNum = queryNum + len(query_content)
        if len(query_content) == 0: continue
        insert_content = calendar_analysis(query_content, content, index, jion_table)
        # insert_content = review_analysis(query_content, content, index, jion_table)
        last_id = connectMongoDB.insert(config_content["outDataSet"], insert_content.to_dict(orient="records"),
                                        log=False)
    return last_id

def contentQueryNoAnalysisHost(config_content: dict, content: list, jion_table, connectMongoDB):
    global queryNum
    queryCondition = config_content["jionQueryCondition"]
    last_id = "None"
    listing_id = []
    for index in range(len(content)):
        listing_id.append(content[index]["HOST_ID"])
    queryCondition['user_id']['$in'] = listing_id
    query_content = connectMongoDB.query(jion_table, queryCondition, 0, 0, log=False)
    queryNum = queryNum + len(query_content)
    if len(query_content) == 0: return last_id
    last_id = connectMongoDB.insert(config_content["outDataSet"], query_content, log=False)
    return last_id

def contentQueryNoAnalysisHouse(config_content: dict, content: list, jion_table, connectMongoDB):
    global queryNum
    queryCondition = config_content["jionQueryCondition"]
    last_id = "None"
    listing_id = []
    for index in range(len(content)):
        listing_id.append(content[index]["HOUSE_ID"])
    queryCondition['listing_id']['$in'] = listing_id
    query_content = connectMongoDB.query(jion_table, queryCondition, 0, 0, log=False)
    queryNum = queryNum + len(query_content)
    if len(query_content) == 0: return last_id
    # insert_content = calendar_analysis(query_content, content, index, jion_table)
    # insert_content = review_analysis(query_content, content, index, jion_table)
    last_id = connectMongoDB.insert(config_content["outDataSet"], query_content, log=False)
    return last_id

def inputParameter(config_content: dict):
    num = config_content["RecordNum"]
    unit = config_content["queryUnit"]
    skip = config_content["skip"]
    num = int(num / unit)
    print("这次处理的数量为{}（单位为：{}）,需要跳过的数据量为{}".format(num, unit, skip))
    input("请确认无误！按任意键继续--->>>")
    return num + 1, unit, skip


def calendar_analysis(query_content, content, content_index, from_table):
    path = '../resources/dk_can_all_calendars.json'
    allCalendarTable = pd.read_json(path, encoding="utf-8", orient='records')
    for index in range(len(query_content)):
        dict_one = {}
        dict_one["HOUSE_ID"] = key_exist("HOUSE_ID", content, content_index)
        dict_one["HOST_ID"] = key_exist("HOST_ID", content, content_index)
        dict_one["HOUSE_COUNTRY"] = key_exist("HOUSE_COUNTRY", content, content_index)
        dict_one["HOUSE_CITY"] = key_exist("HOUSE_CITY", content, content_index)

        dict_one["LISTING_ID"] = key_exist("listing_id", query_content, index)
        dict_one["DATE"] = key_exist("date", query_content, index, "string_change_date")
        dict_one["AVAILABLE"] = key_exist("available", query_content, index, "string_change_boolean")
        dict_one["PRICE"] = key_exist("price", query_content, index, "money_string_float")
        dict_one["ADJUSTED_PRICE"] = key_exist("adjusted_price", query_content, index, "money_string_float")
        dict_one["MINIMUM_NIGHTS"] = key_exist("minimum_nights", query_content, index, "string_change_int")
        dict_one["MAXIMUM_NIGHTS"] = key_exist("maximum_nights", query_content, index, "string_change_int")
        dict_one["FROM_TABLE"] = from_table
        dict_one["RELEASE_DATE"] = key_exist("release_date", query_content, index, "string_change_date")

        allCalendarTable.loc[index] = dict_one
    return allCalendarTable


def review_analysis(query_content, content, content_index, from_table):
    path = '../resources/dk_can_all_house_reviews.json'
    allCalendarTable = pd.read_json(path, encoding="utf-8", orient='records')
    for index in range(len(query_content)):
        dict_one = {}
        dict_one["HOUSE_ID"] = key_exist("HOUSE_ID", content, content_index)
        dict_one["HOST_ID"] = key_exist("HOST_ID", content, content_index)
        dict_one["HOUSE_COUNTRY"] = key_exist("HOUSE_COUNTRY", content, content_index)
        # dict_one["HOUSE_STATE"] = key_exist("HOUSE_STATE", content, content_index)
        dict_one["HOUSE_CITY"] = key_exist("HOUSE_CITY", content, content_index)
        # dict_one["HOUST_NEIGHBOURHOOD"] = key_exist("HOUST_NEIGHBOURHOOD", content, content_index)
        # dict_one["HOUSE_MARKET"] = key_exist("HOUSE_MARKET", content, content_index)

        dict_one["LISTING_ID"] = key_exist("listing_id", query_content, index)
        dict_one["COMMENT_ID"] = key_exist("id", query_content, index)
        dict_one["REVIEWER_ID"] = key_exist("reviewer_id", query_content, index)
        dict_one["REVIEWER_NAME"] = key_exist("reviewer_name", query_content, index)
        dict_one["COMMENTS"] = key_exist("comments", query_content, index)
        dict_one["DATE"] = key_exist("date", query_content, index)
        dict_one["FROM_TABLE"] = from_table
        dict_one["RELEASE_DATE"] = key_exist("release_date", query_content, index)

        allCalendarTable.loc[index] = dict_one
    return allCalendarTable


def key_exist(key: str, content: dict, index: int, type=""):
    AT = AnalysisTool()
    if key in content[index]:
        if type == "string_change_date":
            return AT.string_change_date(content[index][key])
        elif type == "string_change_boolean":
            return AT.string_change_boolean(content[index][key])
        elif type == "money_string_float":
            return AT.money_string_float(content[index][key])
        elif type == "string_change_int":
            return AT.string_change_int(content[index][key])
        elif type == "string_change_float":
            return AT.string_change_float(content[index][key])
        elif type == "len":
            return len(content[index][key])
        else:
            return content[index][key]
    else:
        return ""
