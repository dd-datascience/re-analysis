import pandas as pd
import requests
from lxml import etree
from pymongo import MongoClient
from dcrawl import xpath, xstrip
client = MongoClient()
db = client.housing
col_index = db.policy_index
col_content = db.policy_content

headers = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36",
    "x-requested-with": "XMLHttpRequest"}


def parse_page_index(page):
    url = 'http://house.china.com.cn/News/98_%s.htm' % page
    response = requests.get(url=url, headers=headers).content.decode('utf8', 'ignore')
    html = etree.HTML(response)
    title = [xstrip(x) for x in html.xpath("//div[@class='xwlist_a']/a/text()")]
    urls = [xstrip(x) for x in html.xpath("//div[@class='xwlist_a']/a/@href")]
    dates = [xstrip(x) for x in html.xpath("//div[@class='xwlist_b']/text()")]
    short_contents = [xstrip(x) for x in html.xpath("//div[@class='xwlist_c']/text()")]

    df = pd.DataFrame(list(zip(title, urls, dates, short_contents)),
                      columns=['title', 'url', 'date', 'abstract'])

    col_index.insert_many(df.to_dict('records'))
    print('page %s has completed!' % page)


# for page in range(375, 610):
#     parse_page_index(page)


def parse_news_content(news_url):
    new_url = news_url[:-4] + '-all.htm'
    response = requests.get(url=new_url, headers=headers).content.decode('utf8', 'ignore')
    html = etree.HTML(response)
    lxml_tree = html.xpath('//div[@class="xwzw"]/p')
    news_content = '\n'.join([xstrip(''.join(dd.itertext())) for dd in lxml_tree])

    time_tree = html.xpath('//span[@id="pubtime_baidu"]')
    pub_time = '\n'.join([xstrip(''.join(dd.itertext())) for dd in time_tree])

    source_tree = html.xpath('//span[@id="source_baidu"]')
    pub_source = '\n'.join([xstrip(''.join(dd.itertext())) for dd in source_tree])

    news_dict = {
        'url': new_url,
        'pub_time': pub_time,
        'source': pub_source,
        'news_content': news_content
    }
    col_content.insert_one(news_dict)
    print('The %s has completed!' % url)


urls = list(col_index.distinct('url'))
for url in urls:
    parse_news_content(url)
