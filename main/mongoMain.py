from pymongo import MongoClient
from MongoDBConnect import MongoDBConnect
# client = MongoClient('mongodb://{}:{}@{}:{}/?authSource={}'.format("airbnb_admin","admin123456","数据库地址","端口号","身份认证所用的库"))


if __name__ == '__main__':
    connectMongoDB = MongoDBConnect.MongoDBConnect("airbnb_admin","airbnb_analysis")
    content=connectMongoDB.query("raw_house",None,100)
    print(content)
