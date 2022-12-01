import json
import time

from Analysis.OneHouseFeature import house_basic_analysis_91
from Analysis.join_table_query import join_table_query
from main import Connect
from main.RecordLog import record, recordOnlyOne
from bson.objectid import ObjectId


# client = MongoClient('mongodb://{}:{}@{}:{}/?authSource={}'.format("airbnb_admin","admin123456","数据库地址","端口号","身份认证所用的库"))

def inputParameter(config_content: dict):
    num = config_content["RecordNum"]
    unit = config_content["queryUnit"]
    skip = config_content["skip"]
    num = int(num / unit)
    print("这次处理的数量为{}（单位为：{}）,需要跳过的数据量为{}".format(num, unit, skip))
    input("请确认无误！按任意键继续--->>>")
    return num+1, unit, skip


def houseBasicAnalysis_91(config_content: dict):
    dbname = config_content["dbName"]
    print("username:{},dbname:{}".format(config_content["userName"], dbname))
    number, unit, skip = inputParameter(config_content)
    connectMongoDB = Connect.MongoDBConnect(config_content, dbname)
    queryCondition = config_content["queryCondition"]
    queryCondition['_id']['$gt'] = ObjectId(queryCondition['_id']['$gt'])
    for num in range(number):
        # content = connectMongoDB.query(target_dataSet, None, unit, num * unit + skip)
        content = connectMongoDB.query(config_content["queryDataSet"], queryCondition, unit, 0)
        insert_content = house_basic_analysis_91(content)
        last_id = connectMongoDB.insert( config_content["outDataSet"], insert_content.to_dict(orient="records"))
        record('query', (num + 1) * unit + skip, content[-1]['_id'])
        record('insert', (num + 1) * unit + skip, last_id)
        queryCondition['_id']['$gt'] = content[-1]['_id']
    return number * unit

def analysisOneHouseFeature():
    config_path = input("请配置好文件的参数，输入配置文件路径:" + str())
    try:
        with open(config_path, 'r',
                  encoding='utf-8') as f:  # D:\TT-knight\Master\My Programming\python_2021\MongoDB\ImportantFile\config.json
            config_content = json.load(f)
        number = houseBasicAnalysis_91(config_content[0])
        input("{}程序成功处理了{}条数据，请按任意键退出".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), str(number)))
    except Exception as e:
        recordOnlyOne(e)
        input("{}程序运行错误，请按任意键退出".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

def jionWithID():
    config_path = input("请配置好文件的参数，输入配置文件路径:" + str())
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config_content = json.load(f)
        number = join_table_query(config_content[1])
        input("{}程序成功处理了{}条数据，请按任意键退出".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), str(number)))
    except Exception as e:
        recordOnlyOne(e)
        input("{}程序运行错误，请按任意键退出".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

if __name__ == '__main__':
    # analysisOneHouseFeature()
    jionWithID()


    # try:
    #     with open(config_path, 'r', encoding='utf-8') as f:    #D:\mongoDB_airbnbywh\MongoDB\ImportantFile\config.json
    #         config_content = json.load(f)
    #     indata = config_content[0]["outDataSet"]
    #     number = houseBasicAnalysis_91(config_content[0]["queryDataSet"], indata,config_content[0])
    #     input("{}程序成功处理了{}条数据，请按任意键退出".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), str(number)))
    # except Exception as e:
    #     recordOnlyOne(e)
    #     input("{}程序运行错误，请按任意键退出".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

