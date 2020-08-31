import pandas as pd
import requests
import mysql.connector
from sqlalchemy import create_engine
from lxml import etree
from dcrawl import xpath, requests_get, findall, xstrip
from fake_useragent import UserAgent
import datetime as dt
import numpy as np

engine = create_engine('mysql://root:dds123456@localhost/lianjia?charset=utf8', encoding='utf-8')
connection = engine.connect()
ua = UserAgent()
# connection = mysql.connector.connect(
#     host="localhost",
#     user="root",
#     passwd="dds123456",
#     database="lianjia",
#     auth_plugin="mysql_native_password")


root_url = 'https://sh.lianjia.com'

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,"
              "*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en,zh-CN;q=0.9,zh;q=0.8,en-US;q=0.7",
    "Connection": "keep-alive",
    "DNT": "1",
    "Host": "sh.lianjia.com",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36",
}


# districts
def get_region(city_url=None):
    if not city_url:
        city_url = 'https://sh.lianjia.com/xiaoqu/'
    response = requests_get(city_url, headers=HEADERS)
    doc = response.text
    html = etree.HTML(response.text)
    urls = [root_url + item for item in xpath('//div[@data-role="ershoufang"]/div/a/@href', html, first=False)]
    names = xpath('//div[@data-role="ershoufang"]/div/a/text()', html, first=False)
    regions = pd.DataFrame(list(zip(names, urls)),
                           columns=['district', 'district_url'])
    return regions


# area
def get_sub_region(region_url=None):
    if not region_url:
        region_url = 'https://sh.lianjia.com/xiaoqu/pudong/'
    response = requests_get(region_url, headers=HEADERS)
    doc = response.text
    html = etree.HTML(response.text)
    urls = [root_url + item for item in xpath('//div[@data-role="ershoufang"]/div[2]/a/@href', html, first=False)]
    names = xpath('//div[@data-role="ershoufang"]/div[2]/a/text()', html, first=False)
    sub_regions = pd.DataFrame(list(zip(names, urls)),
                               columns=['area', 'area_url'])
    return sub_regions

area_url = 'https://sh.lianjia.com/xiaoqu/beicai/'


def get_community(sub_region_url, page=1):
    page_url = sub_region_url + 'pg%s/' % str(page)
    response = requests_get(page_url, headers=HEADERS)
    # doc = response.text
    html = etree.HTML(response.text)
    # total_page = findall('"totalPage":(.*?),', doc)
    community_id = xpath('//li[@class="clear xiaoquListItem"]/@data-id', html, first=False)
    community_list = xpath('//li/div[@class="info"]/div[@class="title"]/a/@href', html, first=False)
    community_names = xpath('//li/div[@class="info"]/div[@class="title"]/a/text()', html, first=False)
    community_df = pd.DataFrame(list(zip(community_names, community_id, community_list)),
                                columns=['community', 'community_id', 'community_url'])

    return community_df


def get_house(community_url=None):
    community_url = 'https://sh.lianjia.com/chengjiao/c5011000014388/'
    response = requests_get(community_url, headers=HEADERS)
    doc = response.text
    html = etree.HTML(doc)
    total_page = findall('"totalPage":(.*?),', doc)
    house_urls = xpath('//div[@class="info"]/div[@class="title"]/a/@href', html, first=False)
    print(house_urls)
    print(len(house_urls))


def get_house_ids(community_id, proxies, page=1):
    murl = "https://m.lianjia.com/liverpool/api/chengjiao/getList?cityId=310000" \
           "&condition=%252Fc{}pg{}&curPage={}".format(str(community_id), str(page), str(page))
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en,zh-CN;q=0.9,zh;q=0.8,en-US;q=0.7",
        "Connection": "keep-alive",
        "DNT": "1",
        "Host": "m.lianjia.com",
        "ORIGINAL-PAGE-URL": "https://m.lianjia.com/sh/chengjiao/c{}".format(str(community_id)),
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) "
                      "AppleWebKit/605.1.15 (KHTML, like Gecko) "
                      "Version/13.0.3 Mobile/15E148 Safari/604.1",
    }

    response = requests_get(url=murl, headers=headers, proxies=proxies)
    doc = response.text
    html = etree.HTML(response.text)
    json_data = response.json()['data']['data']['getChengjiaoList']
    total_count = int(json_data['totalCount'])
    total_page = int(np.ceil(total_count / 30))

    if total_page == 0:
        house_codes = ['无']
        house_df = pd.DataFrame(house_codes, columns=['house_code'])
        house_df['community_id'] = community_id
        house_df.to_sql('house', con=connection, if_exists='append', index=False)
    elif page < total_page:
        house_codes = json_data['list']
        house_codes = [item['houseCode'] for item in house_codes]
        house_df = pd.DataFrame(house_codes, columns=['house_code'])
        house_df['community_id'] = community_id
        house_df.to_sql('house', con=connection, if_exists='append', index=False)
        page += 1
        get_house_ids(community_id=community_id, page=page)


community_collected = pd.read_sql("select community_id from house",
                                  connection)['community_id'].unique()
community_df = pd.read_sql("select community_id from community",
                           connection).query('community_id not in @community_collected')
print(len(community_collected))

for index, row in community_df.iterrows():
    community_id = row['community_id']
    proxies = requests_get('http://webapi.http.zhimacangku.com/getip?num=1&type=1&pro=&city=0&yys=0'
                           '&port=1&time=1&ts=0&ys=0&cs=0&lb=1&sb=0&pb=4&mr=1&regions=')
    print(proxies)
    try:
        get_house_ids(str(community_id), proxies, 1)
    except:
        proxies = requests_get(
            'http://webapi.http.zhimacangku.com/getip?num=1&type=1&pro=&city=0&yys=0'
            '&port=1&time=1&ts=0&ys=0&cs=0&lb=1&sb=0&pb=4&mr=1&regions=')
        get_house_ids(str(community_id), proxies, 1)

    print(community_id, '完成爬取..')

districts = get_region('https://sh.lianjia.com/xiaoqu/')
for index, row in districts.iterrows():
    district, district_url = row['district'], row['district_url']
    areas = get_sub_region(district_url)
    areas['district'] = district
    areas['district_url'] = district_url
    areas['province'] = '上海市'
    areas['city'] = '上海市'
    areas['city_url'] = 'https://sh.lianjia.com/'
    areas['create_date'] = dt.datetime.today().strftime('%Y-%m-%d')
    areas['update_date'] = dt.datetime.today().strftime('%Y-%m-%d')
    areas.to_sql('location', con=connection, if_exists='append', index=False)
    print(district, '数据输入完毕')

areas_collected = pd.read_sql("select area, area_url from community", connection)['area_url'].unique()
areas_df = pd.read_sql("select area, area_url from location", connection).query('area_url not in @areas_collected')

for index, row in areas_df.iterrows():
    area = row['area']
    area_url = row['area_url']
    print(area, area_url, "开始爬取小区信息...")

    response = requests_get(area_url, headers=HEADERS)
    doc = response.text
    html = etree.HTML(response.text)
    total_fl = int(xstrip(xpath('//h2[@class="total fl"]/span/text()', html)))
    total_page = int(np.ceil(total_fl / 30))
    result = pd.DataFrame()
    if total_fl == 0:
        print("该区域没有小区")
        continue
    else:
        for page in range(1, total_page + 1):
            page_df = get_community(area_url, page)
            result = pd.concat([result, page_df])

        result['area'] = area
        result['area_url'] = area_url
        result.to_sql('community', con=connection, if_exists='append', index=False)
        print(area, area_url, '小区信息入库完成.')

connection.close()
# full_areas.to_excel('full_area.xlsx')
