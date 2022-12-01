from pymongo import MongoClient

import datetime
import re
import logging
import pandas as pd
import time



def record(operate, record_num, id):
    '''
    :param operate: 当前操作
    :param record_num: 处理数据条数
    :param id: 最后一个记录自动生成的唯一id
    :return:
    '''

    logging.basicConfig(level=logging.DEBUG,
                        format=' %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename='../OutputFile/log_' + str(datetime.date.today()) + '.txt',
                        filemode='a')
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.getLogger().addHandler(console)
    logging.info((time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())) + '-> ' + str(
        record_num) + 'pieces of data are processed successfully(' + operate + '), the id of the last record is' + str(id))
    logging.getLogger().removeHandler(console)

def recordOnlyOne(message):
    '''
    :param operate: 当前操作
    :param record_num: 处理数据条数
    :param id: 最后一个记录自动生成的唯一id
    :return:
    '''

    logging.basicConfig(level=logging.DEBUG,
                        format=' %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename='../OutputFile/log_' + str(datetime.date.today()) + '.txt',
                        filemode='a')
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.getLogger().addHandler(console)
    logging.info((time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())) + '-> ' + str(message))
    logging.getLogger().removeHandler(console)

class MongoDBConnect:
    def __init__(self, username, dbname):
        # 用户认证
        client = self.auth('localhost', 27017, username)
        self.mydb = client[dbname]

    def auth(self, host, port, username):
        # client = MongoClient(host, port)
        user_code = self.readusercode(username)
        # user_code = getpass.getpass("password: ")
        if (user_code == None):
            print("{}：不存在此用户".format(time.strftime("%Y-%m-%d:%H %M %S", time.localtime())))
        else:
            # db.authenticate(username, user_code, mechanism='SCRAM-SHA-1')
            client = MongoClient(host=host, port=port, username=username, password="12345678")
            db = client.admin  # 必须得连接admin数据库才可以（管权限）
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
    def query(self, collectionName: str, queryCondition=None, limitNum=0, skipNum=0, del_id=True):
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
            query_resultDataFrame = pd.DataFrame(list(query_result))
            # df.to_csv("abc.csv", encoding="utf_8_sig")      # 转化为csv文件

            if del_id:
                del query_resultDataFrame['_id']  # 是否删除mongoDB第一列生成的id
            print(
                "{}->The query is complete and takes time:{}'s".format(
                    time.strftime("%Y-%m-%d:%H %M %S", time.localtime()),
                    round(time.time() - st, 3)))
            return query_resultDataFrame
        except IOError:
            print("Error: 查询失败")

    # 数据排序
    def dataSort(self, collectionName: str, sortField):
        pass

    # 插入
    def insert(self, collectionName: str, insertList: list):
        """
        :param collectionName: 集合名字
        :param insertList: 插入的数据
        """
        try:
            st = time.time()
            insertContent = self.mydb[collectionName].insert_many(insertList)
            # 输出插入的所有文档对应的 _id 值
            print(
                "{}->The insertion succeeded. Procedure:{}'s".format(
                    time.strftime("%Y-%m-%d:%H %M %S", time.localtime()),
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


class AnalysisTool:
    def __init__(self):
        pass

    def string_change_date(self, date_string):
        if date_string == "":
            return ""
        else:
            return datetime.datetime.strptime(date_string, "%Y-%m-%d")

    def string_change_boolean(self, t_f_string):
        if t_f_string == "t":
            return True
        elif t_f_string == "f":
            return False
        else:
            return ""

    def money_string_float(self, money_string):
        if money_string == "":
            return ""
        else:
            return float(money_string[1:].replace(',', ''))  # 去除$

    def string_change_int(self, string_int):
        if string_int == "" or string_int == "NaN":
            return ""
        else:
            return int(string_int)

    def string_change_float(self, string_float):
        if string_float == "":
            return ""
        else:
            return float(string_float)
    @classmethod
    def readuserdbname(self, username: str):
        try:
            with open("../ImportantFile/mongodbCode.txt", "r") as f:
                content = f.read()
                user_code = re.findall(username + ",dbname:(.*)", content)
                return user_code[0]
        except IOError:
            print("Error: 没有找到文件或读取文件失败")

def analysis_amenities(dict_one, amenities_content: str):
    dict_one["HOUSE_HAS_AMENITIES_WIFI"] = (amenities_content.find("wifi") != -1)
    dict_one["HOUSE_HAS_AMENITIES_TV"] = (amenities_content.find("TV") != -1)
    dict_one["HOUSE_HAS_AMENITIES_CABLE_TV"] = (amenities_content.find("Cable TV") != -1)
    dict_one["HOUSE_HAS_AMENITIES_BUZZER"] = amenities_content.find("Buzzer/wireless intercom") != -1
    dict_one["HOUSE_HAS_AMENITIES_COOKING"] = amenities_content.find("Cooking basics") != -1
    dict_one["HOUSE_HAS_AMENITIES_ETHERNET"] = amenities_content.find("Ethernet connection") != -1
    dict_one["HOUSE_HAS_AMENITIES_KITCHEN"] = (amenities_content.find("Kitchen") != -1)
    dict_one["HOUSE_HAS_AMENITIES_HAIR_DRYER"] = (amenities_content.find("Hair dryer") != -1)
    dict_one["HOUSE_HAS_AMENITIES_ESSENTIALS"] = (amenities_content.find("Essentials") != -1)
    dict_one["HOUSE_HAS_AMENITIES_HEATING"] = (amenities_content.find("Heating") != -1)
    dict_one["HOUSE_HAS_AMENITIES_AIR_CONDITIONER"] = (amenities_content.find("Air conditioner") != -1)
    dict_one["HOUSE_HAS_AMENITIES_WASHER"] = (amenities_content.find("Washer") != -1)
    dict_one["HOUSE_HAS_AMENITIES_SHAMPOO"] = (amenities_content.find("Shampoo") != -1)
    dict_one["HOUSE_HAS_AMENITIES_IRON"] = (amenities_content.find("iron") != -1)
    dict_one["HOUSE_HAS_AMENITIES_HANGERS"] = (amenities_content.find("Hangers") != -1)
    dict_one["HOUSE_HAS_AMENITIES_BATHTUB"] = (amenities_content.find("Bathtub") != -1)
    dict_one["HOUSE_HAS_AMENITIES_DRYER"] = (amenities_content.find("Dryer") != -1)
    dict_one["HOUSE_HAS_AMENITIES_COFFEE_MAKER"] = (amenities_content.find("Coffee maker") != -1)
    dict_one["HOUSE_HAS_AMENITIES_REFRIGERATOR"] = (amenities_content.find("Refrigerator") != -1)
    dict_one["HOUSE_HAS_AMENITIES_DISH_SILVERWARE"] = (amenities_content.find("Dishes and silverware") != -1)
    dict_one["HOUSE_HAS_AMENITIES_HOT_WATER"] = (amenities_content.find("Hot water") != -1)
    dict_one["HOUSE_HAS_AMENITIES_STOVE"] = (amenities_content.find("Stove") != -1)
    dict_one["HOUSE_HAS_AMENITIES_OVEN"] = (amenities_content.find("Oven") != -1)
    dict_one["HOUSE_HAS_AMENITIES_WORKSPACE"] = (amenities_content.find("Laptop friendly workspace") != -1)
    dict_one["HOUSE_HAS_AMENITIES_CRIB"] = (amenities_content.find("Crib") != -1)
    dict_one["HOUSE_HAS_AMENITIES_KID_FRIENDLY"] = (amenities_content.find("Family/kid friendly") != -1)
    dict_one["HOUSE_HAS_AMENITIES_LOCK_BEDROOM_DOOR"] = (amenities_content.find("Lock on bedroom door") != -1)
    dict_one["HOUSE_HAS_AMENITIES_OUTLET_COVERS"] = (amenities_content.find("Outlet covers") != -1)
    dict_one["HOUSE_HAS_AMENITIES_CHILD_DINNERWARE"] = (amenities_content.find("Children's dinnerware") != -1)
    dict_one["HOUSE_HAS_AMENITIES_BABYSITTER_RECOMMEND"] = (amenities_content.find(
        "Babysitter recommendations") != -1)
    dict_one["HOUSE_HAS_AMENITIES_PLAY_TRAVEL_CRIB"] = (amenities_content.find(
        "Pack'n Play/travel crib") != -1)
    dict_one["HOUSE_HAS_AMENITIES_CHILD_BOOKS_TOYS"] = (amenities_content.find(
        "Children's books and toys") != -1)
    dict_one["HOUSE_HAS_AMENITIES_FREE_PARKING"] = (amenities_content.find("Free street parking") != -1)
    dict_one["HOUSE_HAS_AMENITIES_PAID_PARKING"] = (amenities_content.find("Paid parking off premises") != -1)
    dict_one["HOUSE_HAS_AMENITIES_SHADES"] = (amenities_content.find("Room-darkening shades") != -1)
    dict_one["HOUSE_HAS_AMENITIES_BED_LINENS"] = (amenities_content.find("Bed linens") != -1)
    dict_one["HOUSE_HAS_AMENITIES_EXTRA_PILLOW_BLANKET"] = (amenities_content.find(
        "Extra pillows and blankets") != -1)
    dict_one["HOUSE_HAS_AMENITIES_BALCONY"] = (amenities_content.find("Patio or balcony") != -1)
    dict_one["HOUSE_HAS_AMENITIES_CO_DETECTOR"] = (amenities_content.find("Carbon monoxide detector") != -1)
    dict_one["HOUSE_HAS_AMENITIES_FIRE_EXTINGUISHER"] = (amenities_content.find("Fire extinguisher") != -1)
    dict_one["HOUSE_HAS_AMENITIES_SOMKE_DETECTOR"] = (amenities_content.find("Smoke detector") != -1)
    dict_one["HOUSE_ALLOW_LUGGAGE_DROPOFF"] = (amenities_content.find("Luggage dropoff allowed") != -1)
    dict_one["HOUSE_ALLOW_LONG_TERM_STAYS"] = (amenities_content.find("Long term stays allowed") != -1)
    dict_one["HOUSE_HAS_AMENITIES_HOST_GREETS"] = (amenities_content.find("Host greets you") != -1)
    return dict_one



def house_basic_analysis_91(content):
    st = time.time()
    path = '../resources/feature_num_house_basic.json'
    # houseBasicTable = pd.read_json(path, encoding="utf-8", orient='records')
    houseBasicTable = []
    st1 = time.time()
    AT = AnalysisTool()
    for index in range(len(content)):
        dict_one = {}
        dict_one["HOUSE_ID"] = content["id"][index]
        dict_one["HOST_ID"] = content["host_id"][index]
        dict_one["HOUSE_COUNTRY"] = content["country"][index]
        dict_one["HOUSE_STATE"] = content["state"][index]
        dict_one["HOUSE_CITY"] = content["city"][index]
        dict_one["HOUST_NEIGHBOURHOOD"] = content["neighbourhood"][index]
        dict_one["HOUSE_MARKET"] = content["market"][index]
        dict_one["HOUSE_IS_LOCATION_EXACT"] = AT.string_change_boolean(content["is_location_exact"][index])
        dict_one["HOUSE_NAME_LEN"] = len(content["name"][index])  # 获取名字的长度
        dict_one["HOUSE_BEDROOM_NUM"] = AT.string_change_int(content["bedrooms"][index])
        dict_one["HOUSE_BED_TYPE"] = content["bed_type"][index]
        dict_one["HOUSE_BED_NUM"] = AT.string_change_int(content["beds"][index])
        dict_one["HOUSE_BATHROOM_NUM"] = AT.string_change_float(content["bathrooms"][index])
        dict_one["HOUSE_SQUARE"] = AT.string_change_int(content["square_feet"][index])
        dict_one["HOUSE_PROPERTY_TYPE"] = content["property_type"][index]
        dict_one["HOUSE_ROOM_TYPE"] = content["room_type"][index]
        dict_one["HOUSE_GUESTS_INCLUDED"] = AT.string_change_int(content["guests_included"][index])
        dict_one["HOUSE_PRICE_DAY"] = AT.money_string_float(content["price"][index])
        dict_one["HOUSE_PRICE_WEEKLY"] = AT.money_string_float(content["weekly_price"][index])
        dict_one["HOUSE_PIRCE_MONTHLY"] = AT.money_string_float(content["monthly_price"][index])
        dict_one["HOUSE_SECURITY_DEPOSIT"] = AT.money_string_float(content["security_deposit"][index])
        dict_one["HOUSE_CLEANNING_FEE"] = AT.money_string_float(content["cleaning_fee"][index])
        dict_one["HOUSE_EXTRA_PEOPLE_FEE"] = AT.money_string_float(content["extra_people"][index])
        dict_one["HOUSE_MAXIMUN_DAY"] = AT.string_change_int(content["minimum_nights"][index])
        dict_one["HOUSE_MINIMUN_DAY"] = AT.string_change_int(content["maximum_nights"][index])
        dict_one["HOUSE_IS_AVAILABILITY"] = AT.string_change_boolean(content["has_availability"][index])
        dict_one["HOUSE_AVAILABLE_BOOK_DAY_WITHIN_30"] = AT.string_change_int(content["availability_30"][index])
        dict_one["HOUSE_AVAILABLE_BOOK_DAY_WITHIN_60"] = AT.string_change_int(content["availability_60"][index])
        dict_one["HOUSE_AVAILABLE_BOOK_DAY_WITHIN_90"] = AT.string_change_int(content["availability_90"][index])
        dict_one["HOUSE_AVAILABLE_BOOK_DAY_WITHIN_365"] = AT.string_change_int(content["availability_365"][index])
        # 计算长度
        dict_one["HOUSE_STREET_LEN"] = len(content["street"][index])
        dict_one["HOUSE_TRANSIT_LEN"] = len(content["transit"][index])
        dict_one["HOUSE_NOTES_LEN"] = len(content["notes"][index])
        dict_one["HOUSE_DESCRIPTION_LEN"] = len(content["description"][index])
        dict_one["HOUSE_SUMMARY_LEN"] = len(content["summary"][index])
        dict_one["HOUSE_SPACE_LEN"] = len(content["space"][index])
        dict_one["HOUSE_NEIGHBOUR_OVERVIEW_LEN"] = len(content["neighborhood_overview"][index])
        dict_one["HOUSE_INTERACTION_LEN"] = len(content["interaction"][index])
        dict_one["HOUSE_ACCESS_LEN"] = len(content["access"][index])
        dict_one["HOUSE_RULE_LEN"] = len(content["house_rules"][index])
        # 处理"amenities"
        dict_one = analysis_amenities(dict_one, content["amenities"][index])

        dict_one["HOUSE_IS_REQUIRE_GUEST_IMAGE"] = AT.string_change_boolean(
            content["require_guest_profile_picture"][index])
        dict_one["HOUSE_IS_REQUIRE_GUEST_PHONE_VERIFICATION"] = AT.string_change_boolean(
            content["require_guest_phone_verification"][index])
        dict_one["HOUSE_IS_INSTATN_BOOKABLE"] = AT.string_change_boolean(content["instant_bookable"][index])
        dict_one["HOUSE_IS_BUSINESS_TRAVEL_READY"] = AT.string_change_boolean(
            content["is_business_travel_ready"][index])
        dict_one["HOUSE_IS_REQUIRE_LICENSE"] = AT.string_change_boolean(content["requires_license"][index])
        dict_one["HOUSE_REVIEW_SCORE_RATE"] = AT.string_change_int(content["review_scores_rating"][index])
        dict_one["HOUSE_REVIEW_SCORE_ACCURACY"] = AT.string_change_int(content["review_scores_accuracy"][index])
        dict_one["HOUSE_REVIEW_SCORE_LOCATION"] = AT.string_change_int(content["review_scores_cleanliness"][index])
        dict_one["HOUSE_REVIEW_SCORE_COMMUNICATION"] = AT.string_change_int(content["review_scores_checkin"][index])
        dict_one["HOUSE_REVIEW_SCORE_CHECKIN"] = AT.string_change_int(content["review_scores_communication"][index])
        dict_one["HOUSE_REIVEW_SCORE_CLEAN"] = AT.string_change_int(content["review_scores_location"][index])
        dict_one["HOUSE_REVIEW_SCORE_VALUE"] = AT.string_change_int(content["review_scores_value"][index])
        dict_one["HOUSE_REVIEW_PER_MONTH"] = AT.string_change_float(content["reviews_per_month"][index])
        dict_one["HOUSE_REVIEW_NUM"] = AT.string_change_int(content["number_of_reviews"][index])
        dict_one["HOUSE_REVIEW_NUM_FIRST_PAGE"] = AT.string_change_int(content["number_of_reviews_ltm"][index])
        dict_one["HOUSE_REVIEW_DATE_FIRST_TIME"] = AT.string_change_date(content["first_review"][index])
        dict_one["HOUSE_REVIEW_DATE_LAST_TIME"] = AT.string_change_date(content["last_review"][index])
        dict_one["HOUSE_RELEASE_DATE"] = AT.string_change_date(content["release_date"][index])
        # houseBasicTable.append(dict_one,ignore_index=True)
        houseBasicTable.append(dict_one)
        # houseBasicTable.loc[index] = dict_one
        if (index + 1) % 1000 == 0:
            print("第{}条数据分析完成.==>> 耗时:{}'s".format(str(index + 1), round(time.time() - st1, 3)))
            st1 = time.time()
    print("数据分析完成.==>> 耗时:{}'s".format(round(time.time() - st, 3)))
    return houseBasicTable

def house_basic_analysis_59(content):
    st = time.time()
    path = '../resources/feature_num_house_basic.json'
    # houseBasicTable = pd.read_json(path, encoding="utf-8", orient='records')
    houseBasicTable = []
    st1 = time.time()
    AT = AnalysisTool()
    for index in range(len(content)):
        dict_one = {}
        dict_one["HOUSE_ID"] = content["id"][index]
        dict_one["HOST_ID"] = content["host_id"][index]
        dict_one["HOUST_NEIGHBOURHOOD"] = content["neighbourhood"][index]
        dict_one["HOUSE_NAME_LEN"] = len(content["name"][index])  # 获取名字的长度
        dict_one["HOUSE_BEDROOM_NUM"] = AT.string_change_int(content["bedrooms"][index])
        dict_one["HOUSE_BED_NUM"] = AT.string_change_int(content["beds"][index])
        dict_one["HOUSE_BATHROOM_NUM"] = AT.string_change_float(content["bathrooms"][index])
        dict_one["HOUSE_PROPERTY_TYPE"] = content["property_type"][index]
        dict_one["HOUSE_ROOM_TYPE"] = content["room_type"][index]
        dict_one["HOUSE_ACCOMMODATES"] = AT.string_change_int(content["accommodates"][index])
        dict_one["HOUSE_PRICE_DAY"] = AT.money_string_float(content["price"][index])
        dict_one["HOUSE_MAXIMUN_DAY"] = AT.string_change_int(content["minimum_nights"][index])
        dict_one["HOUSE_MINIMUN_DAY"] = AT.string_change_int(content["maximum_nights"][index])
        dict_one["HOUSE_IS_AVAILABILITY"] = AT.string_change_boolean(content["has_availability"][index])
        dict_one["HOUSE_AVAILABLE_BOOK_DAY_WITHIN_30"] = AT.string_change_int(content["availability_30"][index])
        dict_one["HOUSE_AVAILABLE_BOOK_DAY_WITHIN_60"] = AT.string_change_int(content["availability_60"][index])
        dict_one["HOUSE_AVAILABLE_BOOK_DAY_WITHIN_90"] = AT.string_change_int(content["availability_90"][index])
        dict_one["HOUSE_AVAILABLE_BOOK_DAY_WITHIN_365"] = AT.string_change_int(content["availability_365"][index])
        # 计算长度
        dict_one["HOUSE_DESCRIPTION_LEN"] = len(content["description"][index])
        dict_one["HOUSE_NEIGHBOUR_OVERVIEW_LEN"] = len(content["neighborhood_overview"][index])
        # 处理"amenities"
        dict_one = analysis_amenities(dict_one, content["amenities"][index])

        dict_one["HOUSE_IS_INSTATN_BOOKABLE"] = AT.string_change_boolean(content["instant_bookable"][index])
        dict_one["HOUSE_REVIEW_SCORE_RATE"] = AT.string_change_int(content["review_scores_rating"][index])
        dict_one["HOUSE_REVIEW_SCORE_ACCURACY"] = AT.string_change_int(content["review_scores_accuracy"][index])
        dict_one["HOUSE_REVIEW_SCORE_LOCATION"] = AT.string_change_int(content["review_scores_cleanliness"][index])
        dict_one["HOUSE_REVIEW_SCORE_COMMUNICATION"] = AT.string_change_int(content["review_scores_checkin"][index])
        dict_one["HOUSE_REVIEW_SCORE_CHECKIN"] = AT.string_change_int(content["review_scores_communication"][index])
        dict_one["HOUSE_REIVEW_SCORE_CLEAN"] = AT.string_change_int(content["review_scores_location"][index])
        dict_one["HOUSE_REVIEW_SCORE_VALUE"] = AT.string_change_int(content["review_scores_value"][index])
        dict_one["HOUSE_REVIEW_PER_MONTH"] = AT.string_change_float(content["reviews_per_month"][index])
        dict_one["HOUSE_REVIEW_NUM"] = AT.string_change_int(content["number_of_reviews"][index])
        dict_one["HOUSE_REVIEW_NUM_FIRST_PAGE"] = AT.string_change_int(content["number_of_reviews_ltm"][index])
        dict_one["HOUSE_REVIEW_DATE_FIRST_TIME"] = AT.string_change_date(content["first_review"][index])
        dict_one["HOUSE_REVIEW_DATE_LAST_TIME"] = AT.string_change_date(content["last_review"][index])
        dict_one["HOUSE_RELEASE_DATE"] = AT.string_change_date(content["release_date"][index])
        # houseBasicTable.append(dict_one,ignore_index=True)
        houseBasicTable.append(dict_one)
        if (index + 1) % 1000 == 0:
            print("第{}条数据分析完成.==>> 耗时:{}'s".format(str(index + 1), round(time.time() - st1, 3)))
            st1 = time.time()
    print("数据分析完成.==>> 耗时:{}'s".format(round(time.time() - st, 3)))
    return houseBasicTable

def inputParameter():
    num = int(input("请输入这次要处理数据的数量:"))
    unit = int(input("请输入每次处理的数量单位（当unit==0时，则一直处理到结束）："))
    skip = int(input("请输入已处理数据的数量："))
    num = int(num / unit)
    print("这次处理的数量为{}（单位为：{}）,需要跳过的数据量为{}".format(num, unit, skip))
    return num, unit, skip


def houseBasicAnalysis_91():
    username = "airbnb_admin"
    AT = AnalysisTool
    dbname = AT.readuserdbname(username)
    print("username:{},dbname:{}".format(username, dbname))
    number, unit, skip = inputParameter()
    connectMongoDB = MongoDBConnect(username, dbname)
    for num in range(number):
        content = connectMongoDB.query("raw_house", None, unit, num * unit + skip)
        insert_content = house_basic_analysis_91(content)
        # print(insert_content[["HOUSE_BED_TYPE"]])
        # last_id = connectMongoDB.insert("feature_num_house_basic", insert_content.to_dict(orient="records"))
        last_id = connectMongoDB.insert("feature_num_house_basic", insert_content)
        record('insert', (num + 1) * unit + skip, last_id)
    return number * unit


def houseBasicAnalysis_59():
    username = 'airbnb_admin'
    AT = AnalysisTool
    dbname = AT.readuserdbname(username)
    print("username:{}, dbname:{}".format(username, dbname))
    number, unit, skip = inputParameter()
    connectMongoDB = MongoDBConnect(username, dbname)
    for num in range(number):
        content = connectMongoDB.query("raw_house", None, unit, num * unit + skip)
        insert_content = house_basic_analysis_59(content)
        # print(insert_content[["HOUSE_BED_TYPE"]])
        # last_id = connectMongoDB.insert("feature_num_house_basic", insert_content.to_dict(orient="records"))
        last_id = connectMongoDB.insert("feature_num_house_basic", insert_content)
        record('insert', (num + 1) * unit + skip, last_id)
    return number * unit

if __name__ == '__main__':
    try:
        switch = int(input("请输入选择的模式: \n"
                           "house_basic_analysis(feature_91):1 \n"
                           "house_basic_analysis(feature_59):2 \n"))

        if switch == 1:
            number = houseBasicAnalysis_91()
        if switch == 2:
            number = houseBasicAnalysis_59()
        else:
            print("{}暂未开发其他功能".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        input("{}程序成功处理了{}条数据，请按任意键退出".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), str(number)))
    except Exception as e:
        recordOnlyOne(e)
        input("{}程序运行错误，请按任意键退出".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))