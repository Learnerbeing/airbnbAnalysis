from datetime import datetime
import re

import numpy as np


class AnalysisTool:
    def __init__(self):
        pass

    def string_change_date(self, date_string):
        try:
            if date_string == "" or "-" not in date_string:
                # if date_string is None:
                return date_string
            else:
                return datetime.strptime(date_string, "%Y-%m-%d")
        except Exception:
            return date_string



    def string_change_boolean(self, t_f_string):
        if t_f_string == "t":
            return True
        elif t_f_string == "f":
            return False
        else:
            return ""

    def money_string_float(self, money_string):
        if money_string == "" or money_string is np.nan:
            return ""
        else:
            return float(money_string[1:].replace(',', ''))  # 去除$

    def string_change_int(self, string_int):
        if string_int == "":
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
