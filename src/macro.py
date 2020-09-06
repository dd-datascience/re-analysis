from dcrawl import requests_get, xpath, findall, xstrip
import pandas as pd
import requests
from lxml import etree
import time
import mysql.connector
from sqlalchemy import create_engine
from fake_useragent import UserAgent
import datetime as dt
import numpy as np

headers = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/84.0.4147.135 Safari/537.36"}
root_url = 'http://gdp.gotohui.com/'


def parse_gdp(url, location):
    data = pd.read_html(url)[0]
    cols = data.columns
    indic_name = [findall(r'(.*?)\(', item) for item in cols]
    indic_name = [item.replace('人均', '人均GDP').replace('产业', '产业增加值') for item in indic_name]
    indic_unit = [findall(r'\((.*?)\)', item) for item in cols]
    indic_unit = [item.replace('亿元', '亿').replace('亿', '亿元') for item in indic_unit]
    indic_source = [item.split(')')[1] for item in cols]
    data.columns = indic_name
    data['省市'] = location
    # index_table = pd.DataFrame(zip(indic_name, indic_unit, indic_source),
    #                            columns=['指标', '单位', '来源'])
    # index_table['省市'] = location
    # index_table = index_table.filter(['指标', '省市', '单位', '来源']).query('指标 !=  "时间"')
    data = data.filter(['时间', 'GDP', '人均GDP', '第一产业增加值', '第二产业增加值', '第三产业增加值', '省市'])
    return data


def province_info():
    url = 'http://gdp.gotohui.com/'
    response = requests.get(url=url, headers=headers)
    html = etree.HTML(response.text)
    names = html.xpath('//table[@class="ntable"]/tr/td[2]/a/text()')[0:30]
    urls = [url + item[1:] for item in html.xpath('//table[@class="ntable"]/tr/td[2]/a/@href')[0:30]]
    province_df = pd.DataFrame(zip(names, urls), columns=['省', '网址'])
    return province_df


def get_country():
    country_data = parse_gdp('http://gdp.gotohui.com/', '全国')
    # country_index.to_excel('全国GDP.xlsx', sheet_name='指标说明', index=False)
    country_data.to_excel('全国GDP.xlsx', index=False)
    print('全国 GDP 数据已经输出到本地，请等待三秒钟...')
    time.sleep(3)
    province_data = province_info()
    province_data.to_excel('省份.xlsx', index=False)
    print('省份信息已经输出到本地...')


def parse_province():
    province_urls = pd.read_excel('省份.xlsx', index=False)
    for index, row in province_urls.iterrows():
        time.sleep(3)
        province = row['省']
        province_url = row['网址']
        print(province, province_url)

        # 省份 GDP 数据
        province_df = parse_gdp(province_url, province)
        prov_response = requests_get(province_url, headers=headers)
        prov_html = etree.HTML(prov_response.text)
        city_names = prov_html.xpath('//table[@class="ntable table-striped"]/tr/td[2]/a/text()')
        city_urls = [root_url + item[1:] for item in
                     prov_html.xpath('//table[@class="ntable table-striped"]/tr/td[2]/a/@href')]
        city_dict = dict(zip(city_names, city_urls))

        for city in city_dict.keys():
            time.sleep(1)
            city_url = city_dict[city]
            city_df = parse_gdp(city_url, city)
            province_df = pd.concat([province_df, city_df])
            print(city, "完成爬取.")
    #
        province_df.to_excel(province + "GDP.xlsx", index=False)


def fix2019():
    gdp2019 = pd.read_excel('GDP2019.xlsx')
    gdp2019.columns = ['时间', 'GDP', '人均GDP', '第三产业增加值', '第二产业增加值', '第一产业增加值', '省市']
    province_list = gdp2019['省市'].to_list()

    for province in province_list:
        result = gdp2019.query('省市 == "%s"' % province)
        province_data = pd.read_excel('%sGDP.xlsx' % province, index=False)
        temp = province_data.query('GDP != "登录查看"')
        result = pd.concat([result, temp], ignore_index=True)
        result['GDP'] = pd.to_numeric(result['GDP'])
        result.to_excel('%sGDP(改).xlsx' % province, index=False)
        print('%s完成修正...' % province)


# get_country()
# parse_province()
fix2019()
