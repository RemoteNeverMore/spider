import json
import sys
import time
import requests
import traceback
import db_config as db_config

from lxml import etree
import pandas as pd
import re
import pymysql
import proxy_ip as px
url = 'https://m.dianping.com/isoapi/module'
# shop_name_list = []
# shop_category_name_list = []
# shop_uuid_list = []
# shop_avg_price_list = []
# shop_star_score_list = []
# shop_address_list = []
# shop_phone_list = []
down_load = 1
c_name = ''

c_id = ''

# 将三个woff文件解析为三个list存入Map
shopNum_address_tagName_num_list = {}

shop_header = {
    'Cookie':  '_lxsdk_cuid=181937967a2c8-0524acb49c7ff2-26021b51-e1000-181937967a3c8; _lxsdk=181937967a2c8-0524acb49c7ff2-26021b51-e1000-181937967a3c8; _hc.v=f22ceec7-6f27-6ec8-bf64-39cea833694c.1656036616; ctu=85d7020c1ab00477ec4861a129db82cbb787d4765049b4c52f293d8a1cdfbdc0; s_ViewType=10; seouser_ab=ugcdetail:A:1; ua=dpuser_9094697459; fspop=test; default_ab=shopList:A:5; cityid=291; logan_custom_report=; Hm_lvt_220e3bf81326a8b21addc0f9c967d48d=1657078138,1657090761,1657157517; Hm_lvt_602b80cf8079ae6591966cc70a3940e7=1656916560,1657085125,1657096727,1657163283; _lx_utm=utm_source=Baidu&utm_medium=organic; cy=79; cye=haerbin; Hm_lpvt_602b80cf8079ae6591966cc70a3940e7=1657175676; msource=default; logan_session_token=uvj7bkfxlieb9n7y2qk5; Hm_lpvt_220e3bf81326a8b21addc0f9c967d48d=1657178035; _lxsdk_s=181d781f221-33-aba-d36||167',
    "User-Agent":  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36",
	'Referer': 'https://m.dianping.com/xian/ch10/',

}

shop_info_header = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36"
}

cookie_list = ['_lxsdk_cuid=181937967a2c8-0524acb49c7ff2-26021b51-e1000-181937967a3c8; _lxsdk=181937967a2c8-0524acb49c7ff2-26021b51-e1000-181937967a3c8; _hc.v=f22ceec7-6f27-6ec8-bf64-39cea833694c.1656036616; ctu=85d7020c1ab00477ec4861a129db82cbb787d4765049b4c52f293d8a1cdfbdc0; s_ViewType=10; seouser_ab=ugcdetail:A:1; default_ab=shopList:A:5; cityid=17; fspop=test; cy=2; cye=beijing; Hm_lvt_602b80cf8079ae6591966cc70a3940e7=1657677306,1657702219,1657709264,1657758800; _lx_utm=utm_source=Baidu&utm_medium=organic; msource=default; logan_custom_report=; Hm_lvt_220e3bf81326a8b21addc0f9c967d48d=1657250963,1657267760,1657679213,1657781265; m_flash2=1; pvhistory="6L+U5ZuePjo8L3N1Z2dlc3QvZ2V0SnNvbkRhdGE/Y2FsbGJhY2s9anNvbnBfMTY1Nzc4MTI4NjM0Nl84MTI0Mz46PDE2NTc3ODEyODY1ODNdX1s="; Hm_lpvt_602b80cf8079ae6591966cc70a3940e7=1657781782; logan_session_token=qlxdkbq0v410726dhljm; Hm_lpvt_220e3bf81326a8b21addc0f9c967d48d=1657782977; _lxsdk_s=181fb700184-c20-c9a-1f9||370']


def handle_shop_cookie(header, i):
    start = cookie_list[0].rindex('||')
    var2 = cookie_list[0][start + 2:]
    str_arr = cookie_list[0].split('=')
    target_star = str_arr[len(str_arr) - 2]
    index = target_star.find(';')
    var1 = target_star[0:index]
    new_var1 = int(var1) + i * 5
    new_var2 = int(var2) + i * 5
    new_cook = cookie_list[0].replace(str(var1) + '; _lxsdk_s=', str(new_var1) + '; _lxsdk_s=').replace(
        '||' + str(var2), '||' + str(new_var2))
    header['Cookie'] = new_cook
    return header


data = {
	"pageEnName": "shopList",
	"moduleInfoList": [
		{
			"moduleName": "mapiSearch",
			"query": {
				"search": {
					"start": 0,
					"categoryId": "10",
					"parentCategoryId": 10,
					"locateCityid": 0,
					"limit": 50,
					"sortId": "0",
					"cityId": 17,
					"range": "-1",
					"maptype": 0,
					"keyword": ""
				}
			}
		}
	]
}


db = pymysql.connect(host=db_config.DB_HOST, user=db_config.DB_USER, password=db_config.DB_PASSWORD, port=db_config.DB_PORT, db=db_config.DB_DADA_DB)
cur = db.cursor()

def close_db():
    cur.close()
    db.close()

def shop_data_save(data):
    uuid = csv_data['shop_uuid_list']
    name = csv_data['shop_name_list']
    category = csv_data['shop_category_name_list']
    avg_price = csv_data['shop_avg_price_list']
    star_score = csv_data['shop_star_score_list']
    comment = csv_data['shop_comment_list'] = shop_comment_list
    sql = "insert into t_dp_shop_info(shop_uuid, shop_name, shop_category, shop_avg_price, shop_star_score , shop_region, shop_total_comments, shop_parent_category ) values(%s, %s, %s, %s, %s, %s, %s, %s) " \
          "ON DUPLICATE KEY UPDATE shop_name = (%s) , shop_category = (%s), shop_avg_price = (%s),  shop_star_score = (%s),  shop_region = (%s),   shop_total_comments = (%s) ,shop_parent_category = (%s)"
    for k in range(0, len(uuid)):
        cur.execute(sql, (uuid[k], name[k], category[k], avg_price[k], star_score[k], c_name, comment[k], c_id, name[k], category[k], avg_price[k], star_score[k], c_name, comment[k], c_id))
    db.commit()

def shop_extra_data_save(data):
    uuid = csv_data['shop_uuid_list']
    shop_extra_data = csv_data['shop_list_data']
    sql = "insert into t_dp_shop_extra_info(shop_uuid, shop_data) values(%s,%s)" \
          "ON DUPLICATE KEY UPDATE shop_data = (%s) "
    for k in range(0, len(uuid)):
        cur.execute(sql, (uuid[k], shop_extra_data[k], shop_extra_data[k]))
    db.commit()



def write_data_to_csv(csv_data):
    shop = {'shop_name': csv_data['shop_name_list'], 'phone': csv_data['shop_phone_list'], 'address': csv_data['shop_address_list'],
            'categoryName': csv_data['shop_category_name_list'], 'shopuuid': csv_data['shop_uuid_list'],
            'avg_price': csv_data['shop_avg_price_list'], 'starScore': csv_data['shop_star_score_list']}
    shop = pd.DataFrame(shop,
                        columns=['shop_name', 'phone', 'address', 'categoryName', 'shopuuid', 'avg_price', 'starScore'])
    shop.to_csv("shop_bak.csv", encoding="utf_8_sig", index=False, mode='a', header=False)

def proxy(proxy):
    p = requests.get('http://icanhazip.com', proxies=proxy)
    print(p.text)


def get_city_id():
    global c_name
    shop_region_id = sys.argv[1]
    if len(shop_region_id) == '0' or shop_region_id.strip() == '':
        print('请输入区域id不能为空')
        raise Exception('Expetion 请输入区域id不能为空')
    sql = "SELECT city_name FROM `t_dp_city_info` where city_id  = '{}' limit 1 ".format(shop_region_id)
    cur.execute(sql)
    cityName = cur.fetchone()
    c_name = cityName[0]
    # del_sql = "DELETE FROM t_dp_shop_info WHERE   shop_region = '{}' ".format(c_name)
    # cur.execute(del_sql)
    # db.commit()
    return shop_region_id


def get_category_list():
    sql = "SELECT category_id, category_name FROM `t_dp_category_info` "
    cur.execute(sql)
    categorys = cur.fetchall()
    arr = []
    for category_obj in categorys:
        category = {}
        category['category_id'] = category_obj[0]
        category['category_name'] = category_obj[1]
        arr.append(category)
    return arr


def get_category_list_test():

    arr = [{'category_id': '207', 'category_name': '茶餐厅'}]

    return arr

if __name__ == '__main__':
    proxy_shop = px.get_proxy_ip(sys.argv[2])
    city_id = get_city_id()
    data['moduleInfoList'][0]['query']['search']['cityId'] = city_id
    category_list = get_category_list_test()
    for category_obj in category_list:
        i = 0
        c_id = category_obj['category_id']
        stop = False
        while True:
            time.sleep(1)
            csv_data = {}
            shop_name_list = []
            shop_category_name_list = []
            shop_uuid_list = []
            shop_avg_price_list = []
            shop_star_score_list = []
            shop_address_list = []
            shop_phone_list = []
            shop_list_data = []
            shop_comment_list = []
            data['moduleInfoList'][0]['query']['search']['categoryId'] = category_obj['category_id']
            data['moduleInfoList'][0]['query']['search']['start'] = i * 20
            shop_header = handle_shop_cookie(shop_header, i)
            # proxy_shop = proxy_list[i % 15]
            # proxy_shop =  {'http': 'http://117.28.134.240:8888'}
            try:
                response = requests.post(url, json=data, headers=shop_header, proxies=proxy_shop, timeout=20)
                # proxy(proxy_shop)
                if response.status_code == 200:
                    res = json.loads(response.text)
                    shop_list = res['data']['moduleInfoList'][0]['moduleData']['data']['listData']['list']
                    if len(shop_list) > 0:
                        for index in range(len(shop_list)):
                            shop_data = shop_list[index]
                            shop_list_data.append(str(shop_data))
                            shop_uuid_list.append(shop_data['shopuuid'])
                            shop_category_name_list.append(shop_data['categoryName'])
                            shop_name_list.append(shop_data['name'])
                            shop_avg_price_list.append(shop_data['priceText'])
                            shop_star_score_list.append(shop_data['starScore'])
                            shop_comment_list.append(shop_data['reviewCount'])
                    else:
                        print(str(i) + '页没有门店信息')
                        stop = True
                    csv_data['shop_uuid_list'] = shop_uuid_list
                    csv_data['shop_name_list'] = shop_name_list
                    csv_data['shop_category_name_list'] = shop_category_name_list
                    csv_data['shop_avg_price_list'] = shop_avg_price_list
                    csv_data['shop_star_score_list'] = shop_star_score_list
                    csv_data['shop_name_list'] = shop_name_list
                    csv_data['shop_list_data'] = shop_list_data
                    csv_data['shop_comment_list'] = shop_comment_list
                    # write_data_to_csv(csv_data)
                    shop_data_save(csv_data)
                    shop_extra_data_save(csv_data)
                else:
                    # 403 之后需重新获取ip
                    # ip 需要重置
                    print('爬取数据异常，403')
                    time.sleep(2)
                    proxy_shop = px.get_proxy_ip(sys.argv[2])
                    continue
                print(c_name + category_obj['category_name'] + '第' + str(i) + '页数据爬取完成')
                i += 1
                if stop:
                    print(c_name + category_obj['category_name'] + '第' + str(i) + '页数据爬取完成')
                    break
            except Exception as e:
                traceback.print_exc()
                proxy_shop = px.get_proxy_ip(sys.argv[2])
                print('获取数据异常，重新获取代理')
    close_db()
    print(c_name+'门店信息爬取完成')
