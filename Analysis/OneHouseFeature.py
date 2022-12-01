import time
import pandas as pd

from main import Connect
from main.analysis_tool import AnalysisTool


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
    houseBasicTable = pd.read_json(path, encoding="utf-8", orient='records')
    st1 = time.time()
    for index in range(len(content)):
        # print(content[index])
        dict_one = {}
        dict_one["HOUSE_ID"] = key_exist("id", content, index)
        dict_one["HOST_ID"] = key_exist("host_id", content, index)
        dict_one["HOUSE_COUNTRY"] = key_exist("country", content, index)
        dict_one["HOUSE_STATE"] = key_exist("state", content, index)
        dict_one["HOUSE_CITY"] = key_exist("city", content, index)
        dict_one["HOUST_NEIGHBOURHOOD"] = key_exist("neighbourhood", content, index)
        dict_one["HOUSE_MARKET"] = key_exist("market", content, index)
        dict_one["HOUSE_IS_LOCATION_EXACT"] = key_exist("is_location_exact", content, index, "string_change_boolean")
        dict_one["HOUSE_NAME_LEN"] = key_exist("name", content, index, "len")  # 获取名字的长度
        dict_one["HOUSE_BEDROOM_NUM"] = key_exist("bedrooms", content, index, "string_change_int")
        dict_one["HOUSE_BED_TYPE"] = key_exist("bed_type", content, index)
        dict_one["HOUSE_BED_NUM"] = key_exist("beds", content, index, "string_change_int")
        dict_one["HOUSE_BATHROOM_NUM"] = key_exist("bathrooms", content, index, "string_change_float")
        dict_one["HOUSE_SQUARE"] = key_exist("square_feet", content, index, "string_change_float")
        dict_one["HOUSE_PROPERTY_TYPE"] = key_exist("property_type", content, index)
        dict_one["HOUSE_ROOM_TYPE"] = key_exist("room_type", content, index)
        dict_one["HOUSE_GUESTS_INCLUDED"] = key_exist("guests_included", content, index, "string_change_int")
        dict_one["HOUSE_PRICE_DAY"] = key_exist("price", content, index, "money_string_float")
        dict_one["HOUSE_PRICE_WEEKLY"] = key_exist("weekly_price", content, index, "money_string_float")
        dict_one["HOUSE_PIRCE_MONTHLY"] = key_exist("monthly_price", content, index, "money_string_float")
        dict_one["HOUSE_SECURITY_DEPOSIT"] = key_exist("security_deposit", content, index, "money_string_float")
        dict_one["HOUSE_CLEANNING_FEE"] = key_exist("cleaning_fee", content, index, "money_string_float")
        dict_one["HOUSE_EXTRA_PEOPLE_FEE"] = key_exist("extra_people", content, index, "money_string_float")
        dict_one["HOUSE_MAXIMUN_DAY"] = key_exist("minimum_nights", content, index, "string_change_int")
        dict_one["HOUSE_MINIMUN_DAY"] = key_exist("maximum_nights", content, index, "string_change_int")
        dict_one["HOUSE_IS_AVAILABILITY"] = key_exist("has_availability", content, index, "string_change_boolean")
        dict_one["HOUSE_AVAILABLE_BOOK_DAY_WITHIN_30"] = key_exist("availability_30", content, index,
                                                                   "string_change_int")
        dict_one["HOUSE_AVAILABLE_BOOK_DAY_WITHIN_60"] = key_exist("availability_60", content, index,
                                                                   "string_change_int")
        dict_one["HOUSE_AVAILABLE_BOOK_DAY_WITHIN_90"] = key_exist("availability_90", content, index,
                                                                   "string_change_int")
        dict_one["HOUSE_AVAILABLE_BOOK_DAY_WITHIN_365"] = key_exist("availability_365", content, index,
                                                                    "string_change_int")
        # 计算长度=
        dict_one["HOUSE_STREET_LEN"] = key_exist("street", content, index, "len")
        dict_one["HOUSE_TRANSIT_LEN"] = key_exist("transit", content, index, "len")
        dict_one["HOUSE_NOTES_LEN"] = key_exist("notes", content, index, "len")
        dict_one["HOUSE_DESCRIPTION_LEN"] = key_exist("description", content, index, "len")
        dict_one["HOUSE_SUMMARY_LEN"] = key_exist("summary", content, index, "len")
        dict_one["HOUSE_SPACE_LEN"] = key_exist("space", content, index, "len")
        dict_one["HOUSE_NEIGHBOUR_OVERVIEW_LEN"] = key_exist("neighborhood_overview", content, index, "len")
        dict_one["HOUSE_INTERACTION_LEN"] = key_exist("interaction", content, index, "len")
        dict_one["HOUSE_ACCESS_LEN"] = key_exist("access", content, index, "len")
        dict_one["HOUSE_RULE_LEN"] = key_exist("house_rules", content, index, "len")
        # 处理"amenities"=
        dict_one = analysis_amenities(dict_one, key_exist("amenities", content, index))
        dict_one["HOUSE_IS_REQUIRE_GUEST_IMAGE"] = key_exist("require_guest_profile_picture", content, index,
                                                             "string_change_boolean")
        dict_one["HOUSE_IS_REQUIRE_GUEST_PHONE_VERIFICATION"] = key_exist("require_guest_phone_verification", content,
                                                                          index, "string_change_boolean")
        dict_one["HOUSE_IS_INSTATN_BOOKABLE"] = key_exist("instant_bookable", content, index, "string_change_boolean")
        dict_one["HOUSE_IS_BUSINESS_TRAVEL_READY"] = key_exist("is_business_travel_ready", content, index,
                                                               "string_change_boolean")
        dict_one["HOUSE_IS_REQUIRE_LICENSE"] = key_exist("requires_license", content, index, "string_change_boolean")
        dict_one["HOUSE_REVIEW_SCORE_RATE"] = key_exist("review_scores_rating", content, index, "string_change_int")
        dict_one["HOUSE_REVIEW_SCORE_ACCURACY"] = key_exist("review_scores_accuracy", content, index,
                                                            "string_change_int")
        dict_one["HOUSE_REVIEW_SCORE_LOCATION"] = key_exist("review_scores_cleanliness", content, index,
                                                            "string_change_int")
        dict_one["HOUSE_REVIEW_SCORE_COMMUNICATION"] = key_exist("review_scores_checkin", content, index,
                                                                 "string_change_int")
        dict_one["HOUSE_REVIEW_SCORE_CHECKIN"] = key_exist("review_scores_communication", content, index,
                                                           "string_change_int")
        dict_one["HOUSE_REIVEW_SCORE_CLEAN"] = key_exist("review_scores_location", content, index, "string_change_int")
        dict_one["HOUSE_REVIEW_SCORE_VALUE"] = key_exist("review_scores_value", content, index, "string_change_int")
        dict_one["HOUSE_REVIEW_PER_MONTH"] = key_exist("reviews_per_month", content, index, "string_change_float")
        dict_one["HOUSE_REVIEW_NUM"] = key_exist("number_of_reviews", content, index, "string_change_int")
        dict_one["HOUSE_REVIEW_NUM_FIRST_PAGE"] = key_exist("number_of_reviews_ltm", content, index,
                                                            "string_change_int")
        dict_one["HOUSE_REVIEW_DATE_FIRST_TIME"] = key_exist("first_review", content, index, "string_change_date")
        dict_one["HOUSE_REVIEW_DATE_LAST_TIME"] = key_exist("last_review", content, index, "string_change_date")
        dict_one["HOUSE_IS_LICENSE"] = key_exist("release_date", content, index)
        dict_one["HOUSE_RELEASE_DATE"] = key_exist("license", content, index, "string_change_date")

        houseBasicTable.loc[index] = dict_one
    #     if (index + 1) % 1000 == 0:
    #         print("第{}条数据分析完成.==>> 耗时:{}'s".format(str(index + 1), round(time.time() - st1, 3)))
    #         st1 = time.time()
    # print("数据分析完成.==>> 耗时:{}'s".format(round(time.time() - st, 3)))
    return houseBasicTable


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


def house_basic_analysis_59(content):
    st = time.time()
    path = '../resources/feature_num_house_basic.json'
    houseBasicTable = pd.read_json(path, encoding="utf-8", orient='records')
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

        houseBasicTable.loc[index] = dict_one
        if (index + 1) % 1000 == 0:
            print("第{}条数据分析完成.==>> 耗时:{}'s".format(str(index + 1), round(time.time() - st1, 3)))
            st1 = time.time()
    print("数据分析完成.==>> 耗时:{}'s".format(round(time.time() - st, 3)))
    return houseBasicTable


if __name__ == '__main__':
    connectMongoDB = Connect.MongoDBConnect("airbnb_admin", "airbnb_analysis")
    content = connectMongoDB.query("raw_house", None, 5, 0)
    insert_content = house_basic_analysis_59(content)
    # print(insert_content[["HOUSE_BED_TYPE"]])
    connectMongoDB.insert("feature_num_house_basic", insert_content.to_dict(orient="records"))
