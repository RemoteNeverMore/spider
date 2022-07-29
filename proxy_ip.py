import json
import time
import requests
import traceback

app_key_secret = [
    {'appKey': '869165977861246976', 'appSecret': 'zyjJV8Eo'},
    {'appKey': '868672035663269888', 'appSecret': 'dXKJ0wFO'},
    {'appKey': '866919307442278400', 'appSecret': 'iBK2wE9y'},
    {'appKey': '864400019602952192', 'appSecret': 'SomMezpu'},
    {'appKey': '864323246500499456', 'appSecret': 'Dtx6J0jM'}]


def get_proxy_ip(proxy_index):
    proxy_index = int(proxy_index)
    proxy_url = 'https://api.xiaoxiangdaili.com/ip/get?appKey=' + app_key_secret[proxy_index]['appKey'] \
                + '&appSecret=' + app_key_secret[proxy_index]['appSecret'] + '&cnt=20&wt=json'
    try:
        proxy_resp = requests.get(proxy_url)
        proxy = {}
        if proxy_resp.status_code == 200:
            proxy_obj = json.loads(proxy_resp.text)
            if proxy_obj['code'] == 200:
                proxy_data = proxy_obj['data']
                if len(proxy_data) > 0:
                    proxy['http'] = 'http://'+proxy_data[0]['ip']+':'+str(proxy_data[0]['port'])
                    proxy['https'] = 'http://'+proxy_data[0]['ip']+':'+str(proxy_data[0]['port'])
                    print("获取代理成功", proxy)
                    return proxy
                else:
                    print('获取的代理ip数为空')
        else:
            print('获取的代理ip数为空')
        print(proxy_resp.text)
        time.sleep(1)
        return get_proxy_ip(proxy_index)
    except Exception as e:
        traceback.print_exc()
        time.sleep(3)
        return get_proxy_ip(proxy_index)


if __name__ == '__main__':
    get_proxy_ip()

