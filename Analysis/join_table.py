import time
import pandas as pd

from main import Connect
from main.RecordLog import record, recordOnlyOne
from bson.objectid import ObjectId
from main.analysis_tool import AnalysisTool



class joinTable:
    def __init__(self, config_content: dict, dbname):
        # 用户认证
        self.dbname = config_content["dbName"]
        self.config_content=config_content
        print("username:{},dbname:{}".format(config_content["userName"], dbname))
        self.number, self.unit, self.skip = self.inputParameter(config_content)
        self.connectMongoDB = Connect.MongoDBConnect(config_content, dbname)
        self.queryCondition1 = config_content["mianQueryCondition1"]
        self.queryCondition1['_id']['$gte'] = ObjectId(self.queryCondition1['_id']['$gte'])
        self.queryCondition2 = config_content["mianQueryCondition2"]
        self.queryCondition2['_id']['$gt'] = ObjectId(self.queryCondition2['_id']['$gt'])
        self.jion_table = config_content["jionDataSet"]
        self.queryNum = 0

    def join_table_query(self):
        for data_index in range(len(self.jion_table)):
            recordOnlyOne(self.config_content["mainDataSet"] + " jion table " + "【" + self.jion_table[data_index] + "】")
            for num in range(self.number):
                if num == 0:
                    content = self.connectMongoDB.query(self.config_content["mainDataSet"], self.queryCondition1, self.unit, 0)
                else:
                    content = self.connectMongoDB.query(self.config_content["mainDataSet"], self.queryCondition2, self.unit, 0)
                # last_id = self.contentQueryNoAnalysisHost(self.config_content, content, self.jion_table[data_index],
                #                                          self.connectMongoDB)
                last_id = self.contentQuery(self.config_content, content, self.jion_table[data_index], self.connectMongoDB)
                record('query', num * self.unit + len(content) + self.skip, content[-1]['_id'])
                record('insert', num * self.unit + len(content) + self.skip, last_id)
                self.queryCondition2['_id']['$gt'] = content[-1]['_id']
                print("查询进度：" + str(int(num * self.unit + len(content) + self.skip)) + ": 连接量--" + str(queryNum))

    def inputParameter(self,config_content: dict):
        num = config_content["RecordNum"]
        unit = config_content["queryUnit"]
        skip = config_content["skip"]
        num = int(num / unit)
        print("这次处理的数量为{}（单位为：{}）,需要跳过的数据量为{}".format(num, unit, skip))
        input("请确认无误！按任意键继续--->>>")
        return num + 1, unit, skip

    def contentQuery(self,config_content: dict, content: list, jion_table, connectMongoDB):
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
        # for index in range(len(content)):
        #     queryCondition['listing_id'] = content[index]["HOUSE_ID"]
        #     query_content = connectMongoDB.query(jion_table, queryCondition, 0, 0, log=False)
        #     queryNum = queryNum + len(query_content)
        #     if len(query_content) == 0: continue
        #     # insert_content = calendar_analysis(query_content, content, index, jion_table)
        #     insert_content = review_analysis(query_content, content, index, jion_table)
        #     last_id = connectMongoDB.insert(config_content["outDataSet"], insert_content.to_dict(orient="records"),
        #                                     log=False)
        return last_id

    def contentQueryNoAnalysisHost(self,config_content: dict, content: list, jion_table, connectMongoDB):
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

    def contentQueryNoAnalysisHouse(self,config_content: dict, content: list, jion_table, connectMongoDB):
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

    def downloadHostPicture(self):
        pass


