import re
import requests
import json
from urllib.parse import quote
import pandas as pd
import hashlib
import urllib
import time
import csv


# Get headers with Bilibili cookie
def get_Header():
    with open('bili_cookie.txt', 'r') as f:   # Replace with your own cookie file
        cookie = f.read()
    header = {
        "Cookie": cookie,
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0'
    }
    return header


# Get video oid and title from bv ID
def get_information(bv):
    resp = requests.get(f"https://www.bilibili.com/video/{bv}", headers=get_Header())
    # Extract oid
    obj = re.compile(f'"aid":(?P<id>.*?),"bvid":"{bv}"')
    oid = obj.search(resp.text).group('id')
    # Extract title
    obj = re.compile(r'<title data-vue-meta="true">(?P<title>.*?)</title>')
    title = obj.search(resp.text).group('title')
    return oid, title


# Crawl comments page by page
def start(bv, oid, pageID, count, csv_writer, is_second):
    # Request parameters
    mode = 2
    plat = 1
    type = 1
    seek_rpid = ''
    web_location = 1315875

    # Current timestamp
    wts = time.time()

    # Not the first page
    if pageID != '':
        pagination_str = '{"offset":"{\\\"type\\\":3,\\\"direction\\\":1,\\\"Data\\\":{\\\"cursor\\\":%d}}"}' % pageID
    # First page
    else:
        pagination_str = '{"offset":""}'

    # MD5 encryption
    md5_str = 'ea1db124af3c7062474693fa704f4ff8'
    code = f"mode={mode}&oid={oid}&pagination_str={urllib.parse.quote(pagination_str)}&plat={plat}&seek_rpid={seek_rpid}&type={type}&web_location={web_location}&wts={wts}" + md5_str
    MD5 = hashlib.md5()
    MD5.update(code.encode('utf-8'))
    w_rid = MD5.hexdigest()

    url = f"https://api.bilibili.com/x/v2/reply/wbi/main?oid={oid}&type={type}&mode={mode}&pagination_str={urllib.parse.quote(pagination_str, safe=':')}&plat=1&seek_rpid={seek_rpid}&web_location={web_location}&w_rid={w_rid}&wts={wts}"
    comment = requests.get(url=url, headers=get_Header()).content.decode('utf-8')
    comment = json.loads(comment)

    for reply in comment['data']['replies']:
        # Count += 1
        count += 1
        # Parent comment ID
        parent = reply["parent"]
        # Comment ID
        rpid = reply["rpid"]
        # User ID
        uid = reply["mid"]
        # Username
        name = reply["member"]["uname"]
        # User level
        level = reply["member"]["level_info"]["current_level"]
        # Gender
        sex = reply["member"]["sex"]
        # Avatar URL
        avatar = reply["member"]["avatar"]
        # VIP status
        if reply["member"]["vip"]["vipStatus"] == 0:
            vip = "否"
        else:
            vip = "是"
        # IP location
        try:
            IP = reply["reply_control"]['location'][5:]
        except:
            IP = "未知"
        # Comment text
        context = reply["content"]["message"]
        # Comment time
        reply_time = pd.to_datetime(reply["ctime"], unit='s')
        # Number of replies
        try:
            rereply = reply["reply_control"]["sub_reply_entry_text"]
            rereply = int(re.findall(r'\d+', rereply)[0])
        except:
            rereply = 0
        # Like count
        like = reply['like']
        # User signature
        try:
            sign = reply['member']['sign']
        except:
            sign = ''

        # Write top-level comment to CSV
        csv_writer.writerow(
            [count, parent, rpid, "一级评论", uid, name, level, sex, context, reply_time, rereply, like, sign, IP, vip,
             avatar])

        # Crawl sub-comments if enabled and the reply count is non-zero
        if is_second and rereply != 0:
            for page in range(1, rereply // 10 + 2):
                second_url = f"https://api.bilibili.com/x/v2/reply/reply?oid={oid}&type=1&root={rpid}&ps=10&pn={page}&web_location=333.788"
                second_comment = requests.get(url=second_url, headers=get_Header()).content.decode('utf-8')
                second_comment = json.loads(second_comment)
                for second in second_comment['data']['replies']:
                    count += 1
                    parent = second["parent"]
                    second_rpid = second["rpid"]
                    uid = second["mid"]
                    name = second["member"]["uname"]
                    level = second["member"]["level_info"]["current_level"]
                    sex = second["member"]["sex"]
                    avatar = second["member"]["avatar"]
                    if second["member"]["vip"]["vipStatus"] == 0:
                        vip = "否"
                    else:
                        vip = "是"
                    try:
                        IP = second["reply_control"]['location'][5:]
                    except:
                        IP = "未知"
                    context = second["content"]["message"]
                    reply_time = pd.to_datetime(second["ctime"], unit='s')
                    try:
                        rereply = second["reply_control"]["sub_reply_entry_text"]
                        rereply = re.findall(r'\d+', rereply)[0]
                    except:
                        rereply = 0
                    like = second['like']
                    try:
                        sign = second['member']['sign']
                    except:
                        sign = ''

                    # Write sub-comment to CSV
                    csv_writer.writerow(
                        [count, parent, second_rpid, "二级评论", uid, name, level, sex, context, reply_time, rereply,
                         like, sign, IP, vip, avatar])

    # Get the ID of the next page
    next_pageID = comment['data']['cursor']['next']
    # If next_pageID == 0, it means last page
    if next_pageID == 0:
        print(f"评论爬取完成！总共爬取{count}条。")
        return
    else:
        time.sleep(0.5)
        print(f"当前爬取{count}条。")
        start(bv, oid, next_pageID, count, csv_writer, is_second)


if __name__ == "__main__":
    # Input the BV ID of the video to be scraped
    bv = "BV1NkD3YrEwK"
    # Get oid and title
    oid, title = get_information(bv)
    # Start from first page
    next_pageID = ''
    # Initial comment count
    count = 0

    # Enable sub-comment crawling
    is_second = True

    # Create CSV and write headers
    with open(f'{title[:12]}_评论.csv', mode='w', newline='', encoding='utf-8-sig') as file:
        csv_writer = csv.writer(file)
        csv_writer.writerow(
            ['序号', '上级评论ID', '评论ID', "评论属性", '用户ID', '用户名', '用户等级', '性别', '评论内容', '评论时间',
             '回复数', '点赞数', '个性签名', 'IP属地', '是否是大会员', '头像'])

        # Start crawling
        start(bv, oid, next_pageID, count, csv_writer, is_second)
