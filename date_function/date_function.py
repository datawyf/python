import datetime

def getyesterday():
    '''
    获取昨日日期
    :return:
    '''
    today = datetime.date.today()
    oneday = datetime.timedelta(days=1)                              #改日期
    yesterday = today - oneday
    return yesterday.strftime("%Y-%m-%d")