import random
import time
import requests
import traceback
from pyquery import PyQuery as pq
from lxml import etree
from fontTools.ttLib import TTFont
import re
import pymysql
import sys
import os
import db_config as db_config
import dp_font_list_config as dp_font

import proxy_ip as px

import dp_cookie

down_load = 1
# 将三个woff文件解析为三个list存入Map
shopNum_address_tagName_num_list = {}

shop_address_list = []
shop_phone_list = []
id_list = []
shop_flavor_score_list = []
shop_env_score_list = []
shop_service_score_list = []

cook_count = {}

test_header = {
    'Sec-Fetch-User':'?1',
    'Upgrade-Insecure-Requests':'1',
    'Referer': 'https://www.dianping.com/xian/ch10',
    'sec-ch-ua': '".Not/A)Brand";v="99", "Google Chrome";v="103", "Chromium";v="103"',
    'Cookie': '',
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36",
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'
}

shop_info_header = {
    "User_Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.114 Safari/537.36 Edg/103.0.1264.62"
}







def handle_shop_cookie(header, i, cookie):

    start = cookie.rindex('||')
    var2 = cookie[start + 2:]
    str_arr = cookie.split('=')
    target_star = str_arr[len(str_arr) - 2]
    index = target_star.find(';')
    var1 = target_star[0:index]
    new_var1 = int(var1) + i * 1
    new_var2 = int(var2) + i * 1
    new_cook = cookie.replace(str(var1) + '; _lxsdk_s=', str(new_var1) + '; _lxsdk_s=').replace('||' + str(var2),
                                                                                                '||' + str(new_var2))
    header['Cookie'] = new_cook
    return header


def handle_cookie(header, i, cookie):
    # 处理 || 后边的数字
    Hm_lpvt_source = ''
    Hm_lpvt_target = 99

    Hm_lpvt_arr = cookie.split('Hm_lpvt_')
    if len(Hm_lpvt_arr) > 0:
        Hm_arr = Hm_lpvt_arr[1].split(';')
        if len(Hm_arr) > 0:
            Hm_lpvt_str = Hm_arr[0]
            Hm_lpvt_eq_arr = Hm_lpvt_str.split('=')
            if len(Hm_lpvt_eq_arr) > 0:
                Hm_lpvt_source = Hm_lpvt_eq_arr[1]
                Hm_lpvt_target = int(Hm_lpvt_source) + i * 1

    _lxsdk_s_source = ''
    _lxsdk_s_target = 77
    _lxsdk_s_arr = cookie.split('_lxsdk_s=')
    if len(_lxsdk_s_arr) > 0:
        lxsdk_arr = _lxsdk_s_arr[1].split(';')
        if len(lxsdk_arr) > 0:
            lxsdk_str = lxsdk_arr[0]
            lxsdk_eq_arr = lxsdk_str.split('||')
            if len(lxsdk_eq_arr) > 0:
                _lxsdk_s_source = lxsdk_eq_arr[1]
                _lxsdk_s_target = int(_lxsdk_s_source) + i * 1
    new_cookie = cookie.replace(Hm_lpvt_source, str(Hm_lpvt_target)).replace(_lxsdk_s_source, str(_lxsdk_s_target))
    header['Cookie'] = new_cookie
    return header


# 获取详情id
def fetch_uuid_list(min_id):
    if len(str(min_id)) == 0:
        # sql = "SELECT id, shop_uuid FROM `t_dp_shop_info` where shop_address is not null and  CHAR_LENGTH(shop_address) < 4 AND CHAR_LENGTH(shop_tel) < 5 and shop_tel <> '404' order by id desc limit 50"
        sql = "SELECT id, shop_uuid FROM t_dp_shop_info WHERE LENGTH(TRIM(SHOP_TEL))= 0 and shop_region in ('广州市','深圳市','东莞市','佛山市','珠海市','厦门市','福州市','南宁市','昆明市','贵阳市','三亚市','海口市') order by id asc  limit 50"
    else:
        # sql = "select id,shop_uuid from t_dp_shop_info where id > '{}' and  shop_address = '体育东路13号(中国邮政隔壁) ' order by id asc limit 50".format(
        #     min_id)
        # sql = "SELECT id, shop_uuid FROM `t_dp_shop_info` where id < '{}' and shop_address is not null and  CHAR_LENGTH(shop_address) < 4 AND CHAR_LENGTH(shop_tel) < 5 and shop_tel <> '404' order by id desc limit 50".format(min_id)
        sql = "SELECT id, shop_uuid FROM t_dp_shop_info WHERE  id > '{}' LENGTH(TRIM(SHOP_TEL))= 0 and shop_region in ('广州市','深圳市','东莞市','佛山市','珠海市','厦门市','福州市','南宁市','昆明市','贵阳市','三亚市','海口市') order by id asc  limit 50".format(min_id)
    cur.execute(sql)
    uuid_list = cur.fetchall()
    return uuid_list


def save_shop_info(info_data):
    phone_list = info_data['shop_phone_list']
    address_list = info_data['shop_address_list']
    flavor_score_list = info_data['shop_flavor_score_list']
    env_score_list = info_data['shop_env_score_list']
    service_score_list = info_data['shop_service_score_list']
    # total_comment_list = info_data['shop_total_comment_list']  # 后期这个字段需要删除掉
    ids = info_data['id_list']

    for k in range(0, len(ids)):
        sql = "update t_dp_shop_info set shop_address = (%s) ,  shop_tel = (%s)  , shop_env_score= (%s) , " \
              "shop_service_score = (%s) , shop_flavor_score = (%s)  where id = " + str(ids[k])
        cur.execute(sql, (
        address_list[k], phone_list[k], env_score_list[k], service_score_list[k], flavor_score_list[k]))
    db.commit()


# 解析电话
def parse_phone(phone_html):
    try:
        # 提取出来的字符串含有转义字符 \u，所以这里需要做额外处理
        phone_htl = r'' + repr(phone_html)
        phone_htl = str(phone_htl)
        if(phone_htl.__contains__('无')):
            return '无'
        phone_htl = phone_htl.replace('<span class="info-name">电话：</span>', '').replace('\'', '') \
            .replace('<d class="num">', ';').replace('</d>', ';').replace("\\u", "")
        phone_arr = phone_htl.split(";")
        real_phone = ''
        for num in phone_arr:
            if len(num.strip()) <= 0:
                continue
            if num.__contains__('\\xa0'):
                num = num.replace('\\xa0', '——')
            if len(num) == 4:
                temp_num = ''
                for i in range(10):
                    if num == shopNum_address_tagName_num_list['num'][i]:
                        temp_num = num.replace(shopNum_address_tagName_num_list['num'][i], dp_font.FONT_LIST[i])
                        break
                real_phone += temp_num
            else:
                real_phone += num
    except:
        real_phone = ''
    return real_phone


db = pymysql.connect(host=db_config.DB_HOST, user=db_config.DB_USER, password=db_config.DB_PASSWORD,
                     port=db_config.DB_PORT, db=db_config.DB_DADA_DB)
cur = db.cursor()


def close_db():
    cur.close()
    db.close()


# 解析地址
def parse_address(address_html):
    try:
        # 提取出来的字符串含有转义字符 \u，所以这里需要做额外处理
        address_htl = r'' + repr(address_html)
        address_htl = str(address_htl)
        address_htl = address_htl.replace("</e>", ';') \
            .replace("</d>", ";") \
            .replace("<e ", ";") \
            .replace("<d ", ";") \
            .replace('class="address">', 'e||') \
            .replace('class="num">', 'd||') \
            .replace('\'', '') \
            .replace("\\u", "")
        address_arr = address_htl.split(";")
        # 解析数字和字符
        real_address = ''
        for num in address_arr:
            if len(num.strip()) <= 0:
                continue
            if len(num) == 7:
                temp_address = ''
                if num.startswith('e||'):
                    num = num[3:]
                    for i in range(10, len(shopNum_address_tagName_num_list['address'])):

                        if num == shopNum_address_tagName_num_list['address'][i]:
                            temp_address = num.replace(shopNum_address_tagName_num_list['address'][i],
                                                       dp_font.FONT_LIST[i])
                            break
                elif num.startswith('d||'):
                    num = num[3:]
                    for i in range(10):
                        if num == shopNum_address_tagName_num_list['num'][i]:
                            temp_address = num.replace(shopNum_address_tagName_num_list['num'][i], dp_font.FONT_LIST[i])
                            break
                real_address += temp_address
            else:
                real_address += num
            # print('address >>>:' + real_address)
    except:
        real_address = ''
    return real_address


def down_woff_parse(html, list, proxy_shop):
    global font
    # 此处需要处理
    text_css = re.findall('<link rel="stylesheet" type="text/css" href="(.*?)">', html)[1]
    css_url = 'http:' + text_css
    # print(css_url)
    # 获取字体文件链接的网页数据
    font_html = get_html(css_url, proxy_shop)
    # 正则表达式获取 字体信息列表
    font_woff_list = re.findall(r'@font-face{(.*?)}', font_html)
    # 获取使用到的字体及其链接
    font_dics = {}
    for font in font_woff_list:
        # 正则表达式获取字体文件名称
        font_name = re.findall(r'font-family: "PingFangSC-Regular-(.*?)"', font)[0]
        # 正则表达式获取字体文件对应链接
        font_dics[font_name] = 'http:' + re.findall(r',url\("(.*?)"\);', font)[0]
        # print(font_name)
        # print(font_dics[font_name])
    # 由于我们用到的只有shopNum、tagName和address，这里只下载这三类字体
    # font_use_list = ['shopNum', 'address', 'tagName', 'num']
    for key in list:
        woff = requests.get(font_dics[key], headers=shop_info_header).content
        with open(f'{key}.woff', 'wb') as f:
            f.write(woff)
    # 修改三类字体映射关系
    for key in list:
        # 打开本地字体文件
        font_data = TTFont(f'{key}.woff')
        # font_data.saveXML('shopNum.xml')
        # 获取全部编码，前2个非有用字符去掉
        uni_list = font_data.getGlyphOrder()[2:]
        # 请求数据中是 "&#xf8a1" 对应 编码中为"uniF8A1",我们进行替换，以请求数据为准
        # shopNum_address_tagName_num_list[key] = ['&#x' + uni[3:] for uni in uni_list]
        shopNum_address_tagName_num_list[key] = ['' + uni[3:] for uni in uni_list]


def proxy(proxy):
    p = requests.get('http://icanhazip.com', proxies=proxy)
    print(p.text)


def re_start():
    re_city_id = sys.argv[1]
    log_name = re_city_id + "_info.log"
    # start = re_city_id.rindex("_")
    # re_city_id = re_city_id[start + 1:]
    # print('sh ~/spider/restart.sh' + ' ' + re_city_id + ' ' + log_name)
    status = os.system('sh ~/spider/restart.bash' + ' ' + re_city_id + ' ' + log_name)
    print(status)


def increase_count(cookie):
    global cook_count
    num = cook_count.get(cookie['id'])
    if num is None or len(str(num)) == 0:
        num = 0
    num += 1
    cook_count[cookie['id']] = num

def clear_count(cookie):
    cook_count[cookie['id']] = 0

def acquire_count(cookie):
    num = cook_count[cookie['id']]
    if num is None or len(str(num)) == 0:
        num = 0
    return num

def delete_cookie(cookie):
    del cook_count[cookie['id']]


def get_shop_html(shop_id, proxy_shop, i):
    # 获取网页静态源代码
    cookie = dp_cookie.get_cookie()
    new_header = handle_cookie(test_header, i, cookie['cookie'])
    shop_info_url = 'https://www.dianping.com/shop/' + shop_id
    try:
        resp = requests.get(shop_info_url, headers=new_header, proxies=proxy_shop, timeout=3)
        time_sleep = random.randint(0, 15)
        time.sleep(time_sleep/10)
        if resp.status_code == 200:
            clear_count(cookie)
            return resp.text
        else:
            print(shop_info_url + '获取页面详情失败，请求状态' + str(resp.status_code))
            increase_count(cookie)
            if 404 == resp.status_code:
                id_list.append(str(i))
                shop_phone_list.append('404')
                shop_address_list.append('404')
                shop_flavor_score_list.append('404')
                shop_env_score_list.append('404')
                shop_service_score_list.append('404')
                return '404'
            i += 1
            if acquire_count(cookie) > 3:
                delete_cookie(cookie)
                dp_cookie.invalid_cookie(cookie)
            proxy_shop = px.get_proxy_ip(sys.argv[2])
            return get_shop_html(shop_id, proxy_shop, i)
    except Exception as e:
        traceback.print_exc()
        print('获取数据异常----------------->> reStart')
        info_obj = {}
        if len(id_list) > 0:
            info_obj['shop_phone_list'] = shop_phone_list
            info_obj['shop_address_list'] = shop_address_list
            info_obj['shop_flavor_score_list'] = shop_flavor_score_list
            info_obj['shop_env_score_list'] = shop_env_score_list
            info_obj['shop_service_score_list'] = shop_service_score_list
            info_obj['id_list'] = id_list
            save_shop_info(info_obj)
        # re_start()
        # i += 1
        # print('获取数据异常')
        # # 重新获取代理，重新调用
        time.sleep(0.4)
        new_proxy = px.get_proxy_ip(sys.argv[2])
        return get_shop_html(shop_id, new_proxy, i)


def get_html(url, proxy_shop):
    try:
        resp = requests.get(url, headers=shop_info_header, proxies=proxy_shop, timeout=5)
        if resp.status_code == 200:
            return resp.text
        else:
            print(url + '获取页面css 失败，请求状态' + str(resp.status_code))
            new_proxy = px.get_proxy_ip(sys.argv[2])
            return get_html(url, new_proxy)
    except Exception as e:
        traceback.print_exc()
        print('获取页面css 异常')
        new_proxy = px.get_proxy_ip(sys.argv[2])
        return get_html(url, new_proxy)



def handle_num(store_num):
    target_str = ''
    if (len(store_num) == 0):
        return target_str
    for store in store_num:
        var = repr(store).replace('\'', '').replace('\\u', '')
        for i in range(10):
            if var == shopNum_address_tagName_num_list['num'][i]:
                target_str += dp_font.FONT_LIST[i]
                break
            if i == 9:
                target_str += var
    return target_str


if __name__ == '__main__':
    proxy_shop = px.get_proxy_ip(sys.argv[2])
    next_min_id = ''
    while True:
        shop_uuid_list = fetch_uuid_list(next_min_id)
        info_data = {}
        shop_address_list = []
        shop_phone_list = []
        id_list = []
        shop_flavor_score_list = []
        shop_env_score_list = []
        shop_service_score_list = []
        shop_total_comment_list = []
        if len(shop_uuid_list) > 0:
            for uuid in shop_uuid_list:
                shop_info_html = get_shop_html(uuid[1], proxy_shop, uuid[0])
                if shop_info_html == '404':
                    continue
                if shop_info_html is None or shop_info_html.__contains__('验证中心'):
                    proxy_shop = px.get_proxy_ip(sys.argv[2])
                    print('获取页面数据不正确，需要验证')
                    shop_info_html = get_shop_html(uuid[1], proxy_shop, uuid[0])
                    if shop_info_html == '404':
                        continue
                id_list.append(uuid[0])
                num_address_list = ['address', 'num']
                if down_load == 1:
                    down_woff_parse(shop_info_html, num_address_list, proxy_shop)
                    down_load += 1
                try:
                    shop_doc = pq(shop_info_html)
                    phone_html = shop_doc('#basic-info > p.tel').html()
                    address_html = shop_doc('#basic-info > div.address > div.map_address > span').html()
                    phone = parse_phone(phone_html)
                    if len(phone.strip()) > 0 and len(phone) < 5 and phone != '无':
                        down_load = 1
                    address = parse_address(address_html)
                    shop_phone_list.append(phone)
                    shop_address_list.append(address)

                    tree = etree.HTML(shop_info_html)
                    shop_flavor_score = handle_num(tree.xpath('//*[@id="comment_score"]/span[1]//text()'))
                    shop_env_score = handle_num(tree.xpath('//*[@id="comment_score"]/span[2]//text()'))
                    shop_service_score = handle_num(tree.xpath('//*[@id="comment_score"]/span[3]//text()'))
                    shop_flavor_score_list.append(shop_flavor_score)
                    shop_env_score_list.append(shop_env_score)
                    shop_service_score_list.append(shop_service_score)
                    print(phone)
                except Exception as e:
                    traceback.print_exc()
                    shop_phone_list.append('error')
                    shop_address_list.append('error')
                    shop_flavor_score_list.append('error')
                    shop_env_score_list.append('error')
                    shop_service_score_list.append('error')
                    print(uuid[1] + '解析出错')
                next_min_id = uuid[0]
                print(next_min_id)
        else:
            print('详情抓取完成了')
            break
        info_data['shop_phone_list'] = shop_phone_list
        info_data['shop_address_list'] = shop_address_list
        info_data['shop_flavor_score_list'] = shop_flavor_score_list
        info_data['shop_env_score_list'] = shop_env_score_list
        info_data['shop_service_score_list'] = shop_service_score_list
        # info_data['shop_total_comment_list'] = shop_total_comment_list
        info_data['id_list'] = id_list
        save_shop_info(info_data)
    close_db()
