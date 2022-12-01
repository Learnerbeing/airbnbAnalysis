import re

from pymongo import MongoClient
from bson.objectid import ObjectId

import time
import pandas as pd


class MongoDBConnect:
    def __init__(self, config_content: dict, dbname):
        # 用户认证
        client = self.auth('localhost', 27017, config_content["userName"], config_content["userCode"])
        self.mydb = client[dbname]

    def auth(self, host, port, username, user_code):
        client = MongoClient(host, port)
        db = client.admin  # 必须得连接admin数据库才可以（管权限）
        if (user_code == None):
            print("{}：不存在此用户".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        else:
            db.authenticate(username, user_code, mechanism='SCRAM-SHA-1')
            print("{}-> 认证成功".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
            return client

    def readusercode(self, username: str):
        try:
            with open("../ImportantFile/mongodbCode.txt", "r") as f:
                content = f.read()
                user_code = re.findall(username + ",password:(.*)", content)
                return user_code[0]
        except IOError:
            print("Error: 没有找到文件或读取文件失败")

    # 查询
    def query(self, collectionName: str, queryCondition=None, limitNum=0, skipNum=0, del_id=True,log=True):
        """
        :param collectionName: 查询的集合名字
        :param queryCondition: 查询语句的条件（mongodb）
        :param limitNum: 限制数量
        :param del_id: 是否删除mongoDB生成的"_id"
        :return:查询结果（Dataframe格式）
        """
        try:
            st = time.time()
            collection = self.mydb[collectionName]
            query_result = collection.find(queryCondition).limit(limitNum).skip(skipNum)
            # print(list(query_result))
            # query_resultDataFrame = pd.DataFrame(list(query_result))
            # df.to_csv("abc.csv", encoding="utf_8_sig")      # 转化为csv文件
            # if del_id:
            #     del query_resultDataFrame['_id']  # 是否删除mongoDB第一列生成的id
            if log:
                print(
                    "{}->The query is complete and takes time:{}'s".format(
                        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                        round(time.time() - st, 3)))

            return list(query_result)
        except IOError:
            print("Error: 查询失败")

    def query_page(self, collectionName: str, last_id: str, limitNum=0 ):
        try:
            st = time.time()
            queryCondition = {"_id": {"$gt": ObjectId(last_id)}}        #ObjectId():包装成Mongo主键；"$gt"：表示大于；$gte:表示大于等于
            collection = self.mydb[collectionName]
            query_result = collection.find(queryCondition).limit(limitNum)
            print(
                "{}->The query is complete and takes time:{}'s".format(
                    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                    round(time.time() - st, 3)))
            return list(query_result)
        except IOError:
            print("Error: 查询失败")

    # 数据排序
    def dataSort(self, collectionName: str, sortField):
        pass

    # 插入
    def insert(self, collectionName: str, insertList: list,log=True):
        """
        :param collectionName: 集合名字
        :param insertList: 插入的数据
        """
        try:
            st = time.time()
            insertContent = self.mydb[collectionName].insert_many(insertList)
            # 输出插入的所有文档对应的 _id 值
            if log:
                print(
                    "{}->The insertion succeeded. Procedure:{}'s".format(
                        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                        round(time.time() - st, 3)))

            return insertContent.inserted_ids[-1]
        except IOError:
            print("Error: 插入失败")

    # 更新
    def update(self, collectionName: str, queryCondition, newValues):
        """

        :param collectionName: 集合名字
        :param queryCondition: 查询条件语句（mongodb）
        :param newValues: 查询后需要更新的数据
        """
        updateContent = self.mydb[collectionName].update_many(queryCondition, newValues)
        # 更新所有文档完成的数量
        print("{}文档已修改".format(updateContent.modified_count))
