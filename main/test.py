# import time
#
# from Analysis.OneHouseFeature import house_basic_analysis_91
# from main import Connect
# from main.RecordLog import record, recordOnlyOne
#
#
# # client = MongoClient('mongodb://{}:{}@{}:{}/?authSource={}'.format("airbnb_admin","admin123456","数据库地址","端口号","身份认证所用的库"))
#
# def inputParameter():
#     # num = int(input("请输入这次要处理数据的数量:"))
#     # unit = int(input("请输入每次处理的数量单位（当unit==0时，则一直处理到结束）："))
#     # skip = int(input("请输入已处理数据的数量："))
#     num=100
#     unit=100
#     skip=19500
#     num = int(num / unit)
#     print("这次处理的数量为{}（单位为：{}）,需要跳过的数据量为{}".format(num, unit, skip))
#     return num, unit, skip
#
#
# def houseBasicAnalysis(username, dbname):
#     number, unit, skip = inputParameter()
#     connectMongoDB = Connect.MongoDBConnect(username, dbname)
#     for num in range(number):
#         content = connectMongoDB.query("raw_house", None, unit, num * unit + skip)
#         insert_content = house_basic_analysis_91(content)
#         # print(insert_content[["HOUSE_BED_TYPE"]])
#         last_id = connectMongoDB.insert("feature_num_house_basic", insert_content.to_dict(orient="records"))
#         record('insert', (num + 1) * unit + skip, last_id)
#     return number*unit
#
#
# if __name__ == '__main__':
#     try:
#         number = houseBasicAnalysis("airbnb_admin", "airbnb_analysis")
#     except Exception as e:
#         recordOnlyOne(e)
#         input("{}程序运行错误，请按任意键退出".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))

list=[]
list.append(1)
list.append(2)
print(list)