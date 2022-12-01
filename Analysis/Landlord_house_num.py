from main import Connect


def landlord_house_analysis():
    pass


if __name__ == '__main__':
    connectMongoDB = Connect.MongoDBConnect("airbnb_admin", "airbnb_analysis")
    content = connectMongoDB.query("raw_house", None, 0, 10)