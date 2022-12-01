import logging
import datetime
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
        record_num) + 'pieces of data are processed successfully(' + operate + '), the id of the last record is ' + str(id))
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
