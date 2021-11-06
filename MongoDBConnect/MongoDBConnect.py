import re

from pymongo import MongoClient
import time
import pandas as pd


class MongoDBConnect():
    def __init__(self, username, dbname):
        # 用户认证
        client = self.auth('localhost', 27017, username)
        self.mydb = client[dbname]

    def auth(self, host, port, username):
        client = MongoClient(host, port)
        db = client.admin                # 必须得连接admin数据库才可以（管权限）
        user_code=self.readusercode(username)
        if (user_code == None):
            print("{}：不存在此用户".format(time.strftime("%Y-%m-%d:%H %M %S", time.localtime())))
        else:
            db.authenticate(username, user_code,mechanism='SCRAM-SHA-1')
            print("{}-> 认证成功".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
            return client

    def readusercode(self,username:str):
        try:
            with open("../ImportantFile/mongodbCode.txt","r") as f:
                content=f.read()
                user_code=re.findall(username+",password:(.*)",content)
                return user_code[0]
        except IOError:
            print("Error: 没有找到文件或读取文件失败")

    # 查询
    def query(self, collectionName:str, queryCondition=None, limitNum=0, skipNum=0, del_id=True):
        """

        :param collectionName: 查询的集合名字
        :param queryCondition: 查询语句的条件（mongodb）
        :param limitNum: 限制数量
        :param del_id: 是否删除mongoDB生成的"_id"
        :return:查询结果（Dataframe格式）
        """
        collection=self.mydb[collectionName]
        query_result = collection.find(queryCondition).limit(limitNum).skip(skipNum)
        query_resultDataFrame = pd.DataFrame(list(query_result))
        #df.to_csv("abc.csv", encoding="utf_8_sig")      # 转化为csv文件

        if del_id:
            del query_resultDataFrame['_id']                        #是否删除mongoDB第一列生成的id
        return query_resultDataFrame

    #数据排序
    def dataSort(self,collectionName:str,sortField):
        pass

    #插入
    def insert(self, collectionName:str, insertList:list):
        """

        :param collectionName: 集合名字
        :param insertList: 插入的数据
        """
        insertContent = self.mydb[collectionName].insert_many(insertList)
        # 输出插入的所有文档对应的 _id 值
        print("插入成功：{}",format(insertContent.inserted_ids))

    #更新
    def update(self,collectionName:str,queryCondition,newValues):
        """

        :param collectionName: 集合名字
        :param queryCondition: 查询条件语句（mongodb）
        :param newValues: 查询后需要更新的数据
        """
        updateContent = self.mydb[collectionName].update_many(queryCondition,newValues)
        # 更新所有文档完成的数量
        print("{}文档已修改".format(updateContent.modified_count))

