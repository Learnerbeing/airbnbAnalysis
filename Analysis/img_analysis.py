import requests
from json import JSONDecoder


def face_analysis(filepath):
    http_url = "https://api-us.faceplusplus.com/facepp/v3/detect"     # 你要调用API的URL
    key = "mbMzBl4vwI-tWGs9IAaR1PIDKxxnBVLF"
    secret = "uweaTydBd3k8FMoQTvqF7ArNwYT18rio"        # face++提供的一对**
    filepath = "D:\TT-knight\Master\My Programming\python_2021\MongoDB\Analysis\img.png"     # 图片文件的绝对路径
    imgurl="https://a0.muscache.com/im/pictures/user/02e58f1e-4ec5-488d-a117-c39690a462c3.jpg?aki_policy=profile_x_medium"
    # 必需的参数，注意key、secret、"gender,age,smiling,beauty"均为字符串，与官网要求一致
    data = {"api_key": key, "api_secret": secret, "image_url":imgurl,
            "return_attributes": "gender,age,smiling,beauty"}
    files = {"image_file": open(filepath, "rb")}
    '''以二进制读入图像，这个字典中open(filepath, "rb")返回的是二进制的图像文件，所以"image_file"是二进制文件，符合官网要求'''
    # response = requests.post(http_url, data=data, files=files)    # POTS上传
    response = requests.post(http_url, data=data)    # POTS上传
    req_con = response.content.decode('utf-8')    # response的内容是JSON格式
    req_dict = JSONDecoder().decode(req_con)    # 对其解码成字典格式
    print(req_dict)
    return req_dict

def download_img(img_url:str, headers,down_file_pyth:str):
    # # 图片链接
    # img_url = "https://a0.muscache.com/defaults/user_pic-225x225.png?v=3"
    # headers = {
    #     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36'
    # }
    r = requests.get(img_url, headers=headers)
    # 下载图片
    with open(down_file_pyth, mode="wb") as f:
        f.write(r.content)  # 图片内容写入文件

# download_img()
face_analysis("")