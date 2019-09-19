# -*- coding:utf-8 -*-
# Parsing dividends data from DART

import urllib.request
import urllib.parse
import xlsxwriter
import os
import time
import sys
import getopt
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import re
from urllib.request import urlopen
import parser
import pandas as pd
from collections import defaultdict


def get_AADJ2(k, sample_dict):  # 자산총계
    kor_name = ['자산총계', '총자산', '銃자산']

    for kor in kor_name:
        if kor in sample_dict[k]:
            out = sample_dict[k].get(kor)
            break
        else:
            out = np.nan

    return out


def get_AADJ8(k, sample_dict):  # 유동자산
    kor_name = ['유동자산']

    for kor in kor_name:
        if kor in sample_dict[k]:
            out = sample_dict[k].get(kor)
            break
        else:
            out = np.nan

    return out


def get_AADJ4(k, sample_dict):
    kor_name = ['자본총계', '총자본', '銃자본']

    for kor in kor_name:
        if kor in sample_dict[k]:
            out = sample_dict[k].get(kor)
            break
        else:
            out = np.nan

    return out


def get_AADJ6(k, sample_dict):
    kor_name = ['유동부채']

    for kor in kor_name:
        if kor in sample_dict[k]:
            out = sample_dict[k].get(kor)
            break
        else:
            out = np.nan

    return out


def get_AADJ7(k, sample_dict):
    kor_name = ['비유동부채', '고정부채']

    for kor in kor_name:
        if kor in sample_dict[k]:
            out = sample_dict[k].get(kor)
            break
        else:
            out = np.nan

    return out


def cleansing(text):
    pattern = r'quot'  # 일본어,한자제거  quot
    text = re.sub(pattern=pattern, repl='', string=text)
    pattern = r'([ぁ-ゟ゠-ヿ一-鿋])'  # 일본어,한자제거  quot
    text = re.sub(pattern=pattern, repl='', string=text)
    pattern = r'\xa0'  # 줄바꿈제거
    text = re.sub(pattern=pattern, repl='', string=text)
    pattern = r'URL 복사 이웃추가 본문 기타 기능 번역보기'
    text = re.sub(pattern=pattern, repl='', string=text)
    pattern = r'([ㄱ-ㅎㅏ-ㅣ])'  # 한글자음 제거
    text = re.sub(pattern=pattern, repl='', string=text)
    pattern = r'[^\w\s]'  # 특수기호 제거
    text = re.sub(pattern=pattern, repl='', string=text)
    pattern = r'[\Ⅰ\Ⅱ\ㆍ]'  # 로마자 제거
    text = re.sub(pattern=pattern, repl='', string=text)
    pattern = r'[0-9]'  # 로마자 제거
    text = re.sub(pattern=pattern, repl='', string=text)
    pattern = r' '  # 띄어쓰기 제거
    text = re.sub(pattern=pattern, repl='', string=text)

    return text


def main():
    # 어쩌고 데이터프레임
    df_ASSET_TRANSFER = pd.read_csv('C:\\Users\\HS\\Desktop\\3,4차 기업이벤트 양수도 전체 목록(결정 삭제).csv', encoding='ANSI')
    df_ASSET_TRANSFER['url'] = df_ASSET_TRANSFER.rcp_no.apply(
        lambda x: "http://dart.fss.or.kr/dsaf001/main.do?rcpNo=" + str(x))
    df_ASSET_TRANSFER['기재정정'] = df_ASSET_TRANSFER['rpt_nm'].isin(
        ['[기재정정]자산양수ㆍ도종료보고서', '[기재정정]합병등종료보고서(자산양수도)', '[기재정정]합병등종료보고서(영업양수도)', '[기재정정]영업양수ㆍ도종료보고서']).apply(int)
    rpt_nm_list = [
        ['합병등종료보고서(자산양수도)', '[기재정정]합병등종료보고서(자산양수도)'],  # 8,9
        ['합병등종료보고서(영업양수도)', '[기재정정]합병등종료보고서(영업양수도)'],  # 8,9
        ['영업양수ㆍ도종료보고서', '[기재정정]영업양수ㆍ도종료보고서'],  # 4,5
        ['자산양수ㆍ도종료보고서', '[기재정정]자산양수ㆍ도종료보고서']  # 3,4
    ]

    df_sample = df_ASSET_TRANSFER.loc[df_ASSET_TRANSFER['삭제'].isnull()].loc[
        df_ASSET_TRANSFER.rpt_nm.isin(rpt_nm_list[0])]
    # df_sample = df_sample.loc[df_sample.index.isin([0,3,16,17,18])]
    df_sample = df_sample.iloc[:200, :]

    ###### 돌리기~~~~~
    dart_div_list = list()

    for i in range(0, 100):
        target = df_sample.iloc[i]
        # print(target)
        crpCd = target.crp_cd
        crpNm = target.crp_nm
        rcpNo = target.rcp_no
        rcpDt = target.rcp_dt
        link = target.url

        handle = urllib.request.urlopen(link)
        print('여기다!!!', handle)
        data = handle.read()
        soup2 = BeautifulSoup(data, 'html.parser', from_encoding='utf-8')  # http read
        # print(soup2)

        test = soup2.find('a', {'href': '#download'})['onclick']
        # print(test)

        words = test.split("'")
        # print(words)

        rcpNo = words[1]
        dcmNo = words[3]
        # print(rcpNo)
        # print(dcmNo)

        dart2 = soup2.find_all(string=re.compile('dart2.dtd'))
        # print(dart2)
        dart3 = soup2.find_all(string=re.compile('dart3.xsd'))
        # print(dart3)

        if target['기재정정'] == 0:
            eleId = '8'  # 재무제표 8
        else:
            eleId = '9'  # 재무제표 8

        ####### 뷰어 링크
        if len(dart3) != 0:
            link2 = "http://dart.fss.or.kr/report/viewer.do?rcpNo=" + rcpNo + "&dcmNo=" + dcmNo + "&eleId=" + eleId + "&offset=4916&length=3668&dtd=dart3.xsd"
        print(link2)

        ####### 테이블 찾기
        handle = urllib.request.urlopen(link2)
        # print(handle)
        data = handle.read()
        soup3 = BeautifulSoup(data, 'html.parser', from_encoding='utf-8')
        # print(soup3)

        tables = soup3.findAll("table")  # list of the all tables in the link2 which means number of tables
        print('len(tables):', len(tables))  # length of the list

        if len(tables) == 0:
            print('해당사항없음, 판단 -> n_tables 컬럼 ')
            # continue 하게 되면. 누락문제발생
            continue
        elif len(tables) == 1:  # 재무제표만
            div_table = soup3.find("table")
        elif 2 <= len(tables) <= 6:  # 단위, 재무제표
            div_table = soup3.findAll("table")[1]
        else:
            print('일단 한번 보자, 이후에 이유쓰는 란 추가 ')
            break

        # print(div_table)  # 예외처리 후 한가지의 table 파싱

        # rows
        div_trs = div_table.findAll('tr')  # list of the all tr in that tables
        print('len(div_trs)', len(div_trs))  # the length of the rows
        print(div_trs)

        # columns -> 4개 ok, 2,5개 예외처리 해야함
        div_tds = div_trs[1]  # list of the all td in that tables
        print('len(div_tds)', len(div_tds))  # the number of columns
        print(div_tds)

        ####### 딕셔너리에 넣기
        dict1 = defaultdict(list)

        for i in range(len(div_trs)):
            for j in range(1, 4):
                try:
                    key = cleansing(div_trs[i].findAll('td')[0].text.strip())
                    val = div_trs[i].findAll('td')[j].text.strip()
                    dict1[key].append(val)
                except IndexError:
                    pass
        print(dict(dict1))
        dict1['STK_CD'].append(crpCd)
        dict1['STK_NM_KOR'].append(crpNm)
        dict1['rcp_NO'].append(rcpNo)
        dict1['rcp_DT'].append(rcpDt)
        dict1['url'].append(link)

        dart_div_list.append(dict(dict1))

    print("")
    print("")
    print('dart_div_list')
    print(dart_div_list)

    ##### 엑셀에 저장하기
    cur_dir = os.getcwd()

    workbook_name = "DART_dividends_baseline.xlsx"

    workbook = xlsxwriter.Workbook(workbook_name)

    # 포멧지정
    worksheet_result = workbook.add_worksheet('result')
    filter_format = workbook.add_format({'bold': True,
                                         'fg_color': '#D7E4BC'
                                         })

    percent_format = workbook.add_format({'num_format': '0.00%'})

    roe_format = workbook.add_format({'bold': True,
                                      'underline': True,
                                      'num_format': '0.00%'})

    num_format = workbook.add_format({'num_format': '0.00'})
    num2_format = workbook.add_format({'num_format': '#,##0'})
    num3_format = workbook.add_format({'num_format': '#,##0.00',
                                       'fg_color': '#FCE4D6'})

    worksheet_result.set_column('A:A', 10)
    worksheet_result.set_column('B:B', 10)
    worksheet_result.set_column('C:C', 10)
    worksheet_result.set_column('D:D', 10)
    worksheet_result.set_column('H:H', 10)
    worksheet_result.set_column('I:I', 10)
    worksheet_result.set_column('J:J', 10)
    worksheet_result.set_column('K:K', 10)
    worksheet_result.set_column('L:L', 10)
    worksheet_result.set_column('M:M', 10)
    worksheet_result.set_column('N:N', 10)
    worksheet_result.set_column('O:O', 10)
    worksheet_result.set_column('P:P', 10)

    worksheet_result.write(0, 0, "STK_CD", filter_format)
    worksheet_result.write(0, 1, "STK_NM_KOR", filter_format)
    worksheet_result.write(0, 2, "rcp_NO", filter_format)
    worksheet_result.write(0, 3, "rcp_DT", filter_format)

    worksheet_result.write(0, 4, "자산_양수도전_자산총계", filter_format)
    worksheet_result.write(0, 5, "자산_증가감소_자산총계", filter_format)
    worksheet_result.write(0, 6, "자산_양수도후_자산총계", filter_format)

    worksheet_result.write(0, 7, "자산_양수도전_유동자산", filter_format)
    worksheet_result.write(0, 8, "자산_증가감소_유동자산", filter_format)
    worksheet_result.write(0, 9, "자산_양수도후_유동자산", filter_format)

    worksheet_result.write(0, 10, "자산_양수도전_자본총계", filter_format)
    worksheet_result.write(0, 11, "자산_증가감소_자본총계", filter_format)
    worksheet_result.write(0, 12, "자산_양수도후_자본총계", filter_format)

    worksheet_result.write(0, 13, "자산_양수도전_유동부채", filter_format)
    worksheet_result.write(0, 14, "자산_증가감소_유동부채", filter_format)
    worksheet_result.write(0, 15, "자산_양수도후_유동부채", filter_format)

    worksheet_result.write(0, 16, "자산_양수도전_비유동부채", filter_format)
    worksheet_result.write(0, 17, "자산_증가감소_비유동부채", filter_format)
    worksheet_result.write(0, 18, "자산_양수도후_비유동부채", filter_format)

    worksheet_result.write(0, 19, "url", filter_format)
    worksheet_result.write(0, 20, "n_tables", filter_format)
    worksheet_result.write(0, 21, "n_trs", filter_format)
    worksheet_result.write(0, 21, "n_tds", filter_format)

    ##### 데이터 작성하기
    for k in range(len(dart_div_list)):
        STK_CD = dart_div_list[k].get('STK_CD')[0]  # 기업코드
        STK_NM_KOR = dart_div_list[k].get('STK_NM_KOR')[0]  # 기업명
        rcp_NO = dart_div_list[k].get('rcp_NO')[0]  # 보고서번호
        rcp_DT = dart_div_list[k].get('rcp_DT')[0]  # 보고기일
        url = dart_div_list[k].get('url')[0]  # 보고서url

        AADJ2 = get_AADJ2(k, dart_div_list)
        AADJ8 = get_AADJ8(k, dart_div_list)
        AADJ4 = get_AADJ4(k, dart_div_list)
        AADJ6 = get_AADJ6(k, dart_div_list)
        AADJ7 = get_AADJ7(k, dart_div_list)

        asset_before_AADI2 = AADJ2[0]  # 자산_양수도전_자산총계
        asset_increase_AADI2 = AADJ2[1]  # 자산_증가감소_자산총계
        asset_after_AADJ2 = AADJ2[2]  # 자산_양수도후_자산총계

        asset_before_AADI8 = AADJ8[0]  # 자산_양수도전_유동자산
        asset_increase_AADI8 = AADJ8[1]  # 자산_증가감소_유동자산
        asset_after_AADJ8 = AADJ8[2]  # 자산_양수도후_유동자산

        asset_before_AADI4 = AADJ4[0]  # 자산_양수도전_자본총계
        asset_increase_AADI4 = AADJ4[1]  # 자산_증가감소_자본총계
        asset_after_AADJ4 = AADJ4[2]  # 자산_양수도후_자본총계

        asset_before_AADI6 = AADJ6[0]  # 자산_양수도전_유동부채
        asset_increase_AADI6 = AADJ6[1]  # 자산_증가감소_유동부채
        asset_after_AADJ6 = AADJ6[2]  # 자산_양수도후_유동부채

        asset_before_AADI7 = AADJ7[0]  # 자산_양수도전_비유동부채
        asset_increase_AADI7 = AADJ7[1]  # 자산_증가감소_비유동부채
        asset_after_AADJ7 = AADJ7[2]  # 자산_양수도후_비유동부채

        worksheet_result.write(k + 1, 0, STK_CD)
        worksheet_result.write(k + 1, 1, STK_NM_KOR)
        worksheet_result.write(k + 1, 2, rcp_DT)
        worksheet_result.write(k + 1, 3, rcp_NO)

        worksheet_result.write(k + 1, 4, asset_before_AADI2)
        worksheet_result.write(k + 1, 5, asset_increase_AADI2)
        worksheet_result.write(k + 1, 6, asset_after_AADJ2)

        worksheet_result.write(k + 1, 7, asset_before_AADI8)
        worksheet_result.write(k + 1, 8, asset_increase_AADI8)
        worksheet_result.write(k + 1, 9, asset_after_AADJ8)

        worksheet_result.write(k + 1, 10, asset_before_AADI4)
        worksheet_result.write(k + 1, 11, asset_increase_AADI4)
        worksheet_result.write(k + 1, 12, asset_after_AADJ4)

        worksheet_result.write(k + 1, 13, asset_before_AADI6)
        worksheet_result.write(k + 1, 14, asset_increase_AADI6)
        worksheet_result.write(k + 1, 15, asset_after_AADJ6)

        worksheet_result.write(k + 1, 16, asset_before_AADI7)
        worksheet_result.write(k + 1, 17, asset_increase_AADI7)
        worksheet_result.write(k + 1, 18, asset_after_AADJ7)

        worksheet_result.write(k + 1, 19, url)
        worksheet_result.write(k + 1, 20, len(tables))
        worksheet_result.write(k + 1, 21, len(div_trs))
        worksheet_result.write(k + 1, 22, len(div_tds))

    workbook.close()


# Main
if __name__ == "__main__":
    main()