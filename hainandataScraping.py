import requests
import re
import os
import pandas as pd
from pandas import DataFrame

# 按商品交易市场类别分宏观经济指标年度数据
wblink = 'http://data.hainan.gov.cn//api/invoke/91d9276643ef4f3d9ced472ef7ffb655?authToken=6029a8af21d5864a8356b8b0038d14b8&BGQ=20150000'
lk1 = 'http://data.hainan.gov.cn//api/invoke/91d9276643ef4f3d9ced472ef7ffb655?authToken=6029a8af21d5864a8356b8b0038d14b8&BGQ='
year = 2015
lk2 = '0000'

def wblink(lk1, year, lk2):
    return lk1 + str(year) + lk2


wblink(lk1, 2017, lk2)

r = requests.get(wblink)
print(type(r))
print(r.status_code)

r.encoding = 'utf-8'
print(r.encoding)
print(r.text)
text = r.text

os.chdir(r'D:\CityDNA\Data\hainanDataScraping')
test = pd.read_excel('test.xlsx')
texttest = pd.read_excel('test.xlsx')['id'][0]
# ========================
text1 = re.findall(r'[[](.*?)[]]', text)

pattern = re.compile("'(.*)'")
text2 = pattern.findall(text1[0])

text1[0].strip('\'''{}[]')
text[0].replace("'", '')

text1_list = text1[0].split("},{")
text1_list[1]

# split [1]
text1_0 = text1_list[0].split(",")
text1_0[0].index(':')  # 查找index

m0 = re.findall('"([^"]+)"', text1_0[0])
m1 = re.findall('"([^"]+)"', text1_0[1])
m2 = re.findall('"([^"]+)"', text1_0[2])

lst = []
lst.append(re.findall('"([^"]+)"', text1_0[0]))
lst.append(re.findall('"([^"]+)"', text1_0[1]))
lst.append(re.findall('"([^"]+)"', text1_0[2]))


df = pd.DataFrame([m0, m1])
df = pd.DataFrame([re.findall('"([^"]+)"', text1_0[0]), re.findall('"([^"]+)"', text1_0[1])])
df = pd.DataFrame(lst)

# ===============================================================================================begin
import requests
import re
import os
import pandas as pd


def wblink(link1, yearnum, link2):
    return link1 + str(yearnum) + link2


# 按商品交易市场类别分宏观经济指标年度数据
# 'http://data.hainan.gov.cn//api/invoke/91d9276643ef4f3d9ced472ef7ffb655?authToken=6029a8af21d5864a8356b8b0038d14b8&BGQ=20150000'
info = pd.read_excel(r'D:\CityDNA\Data\hainanDataScraping\按商品交易市场类别分宏观经济指标年度数据\info.xlsx')
info[['lk2']] = info[['lk2']].astype(str)
lk1 = 'http://data.hainan.gov.cn//api/invoke/91d9276643ef4f3d9ced472ef7ffb655?authToken=6029a8af21d5864a8356b8b0038d14b8&BGQ='
year = 2015
lk2 = '0000'
wblink(lk1, 2015, lk2)
r = requests.get(wblink)
text = r.text

text1 = re.findall(r'[[](.*?)[]]', text)
text1_list = text1[0].split("},{")

# create dataframe
text1_0 = text1_list[0].split(",")
lst = []
for i in range(len(text1_0)):
    lst.append(re.findall('"([^"]+)"', text1_0[i]))
lst.append(['year', '2015'])
df = pd.DataFrame(lst)
df.columns = ['index', 'val']

# addon to df
i = 1
for i in range(1, len(text1_list)):
    row = text1_list[i].split(",")
    newlst = []
    for j in range(len(row)):
        newlst.append(re.findall('"([^"]+)"', row[j]))
    newlst.append(['year', '2015'])
    dfnew = pd.DataFrame(newlst)
    dfnew.columns = ['index', 'val']

    # merge df and newdf as df
    df = pd.merge(df, dfnew, how='outer', on='index', sort=False)

# merge with table name
tbname = pd.read_excel(r'D:\CityDNA\Data\hainanDataScraping\按商品交易市场类别分宏观经济指标年度数据\name.xlsx')
tbmerge = pd.merge(tbname, df, how='outer', left_on='英文名称', right_on='index', sort=False)
tbmerge.to_excel(r'D:\CityDNA\Data\hainanDataScraping\按商品交易市场类别分宏观经济指标年度数据\tbmerge.xlsx')

# 五年循环 + 表名循环
os.chdir(r'D:\CityDNA\Data\hainanDataScraping')
list_file = os.listdir()
for fl in list_file:
    if os.path.isfile(fl):
        list_file.remove(fl)
# !!!!!!!!!!!

year = 2012
lk1 = info['lk1'][0]
lk2 = '0000'
years = info['year'].tolist()

# readin FILE
fileinfo = pd.read_excel(r'D:\CityDNA\Data\hainanDataScraping\FILEs.xlsx')
filename = fileinfo['filename'][0]
fileyear = fileinfo['year'][0].split(", ")  # string type
fileyear = [int(x) for x in fileyear]
lk1 = fileinfo['lk1'][0]
lk2 = fileinfo['lk2'][0].replace(',', '')

# ===============================================================================================begin
import requests
import re
import os
import pandas as pd


def wblink(link1, yearnum, link2):
    return link1 + str(yearnum) + link2


# 第j个file finished(0,1,2,3,4,5,6,8,10,12,13,14,15,16,17,18 ) unfinished(7[no1516], 9[no 1516], 11[暂无访问权限], )
# add, 7 add 2011 no 2010;
fileinfo = pd.read_excel(r'D:\CityDNA\Data\hainanDataScraping\FILEs.xlsx')
j = 18  # j=18
filename = fileinfo['filename'][j]
fileyear = fileinfo['year'][j].split(", ")  # string type
fileyear = [int(x) for x in fileyear]
lk1 = fileinfo['lk1'][j]
lk2 = fileinfo['lk2'][j].replace(',', '')

year = 2016
for year in fileyear:
# for year in range(2014, 2017):
    print(str(year) + ' start...')
    link = wblink(lk1, year, lk2)
    r = requests.get(link)
    text = r.text
    # '''
    # json
    jsdata = pd.read_excel(r'D:\CityDNA\Data\hainanDataScraping\tables\\' + filename + '\\jsdata.xlsx')
    year = 2013
    text = jsdata.iloc[1, 1]
    # '''
    text1 = re.findall(r'[[](.*?)[]]', text)
    text1_list = text1[0].split("},{")
    # text1_list = text1[0].split("}, {") 表格多列情况需要调整

    # create dataframe
    print('creat dataframe as df...')
    text1_0 = text1_list[0].split(",")
    lst = []
    for i in range(len(text1_0)):
        lst.append(re.findall('"([^"]+)"', text1_0[i]))
    lst.append(['year', str(year)])
    df = pd.DataFrame(lst)
    df.columns = ['index', 'val']

    # addon to df
    print('add to df...')
    i = 1
    for i in range(1, len(text1_list)):
        row = text1_list[i].split(",")
        newlst = []
        for j in range(len(row)):
            newlst.append(re.findall('"([^"]+)"', row[j]))
        newlst.append(['year', str(year)])
        dfnew = pd.DataFrame(newlst)
        dfnew.columns = ['index', 'val']

        # merge df and newdf as df
        df = pd.merge(df, dfnew, how='outer', on='index', sort=False)

    # merge with table name
    print('merge table name...')
    tbname = pd.read_excel(r'D:\CityDNA\Data\hainanDataScraping\tables\\' + filename + '\\name.xlsx')
    tbmerge = pd.merge(tbname, df, how='outer', left_on='英文名称', right_on='index', sort=False)
    tbmerge.to_excel(r'D:\CityDNA\Data\hainanDataScraping\tables\\' + filename + '\\tb' + str(year) + '.xlsx')

    print(str(year) + ' finished!!!')


# merge 5-year table
base = pd.read_excel(r'D:\CityDNA\Data\hainanDataScraping\tables\\' + filename + '\\tb' + str(fileyear[0]) + '.xlsx')
tbmerge = []
for year in fileyear[1:5]:  # 1:6！！！！！！！！
    tbadd = pd.read_excel(r'D:\CityDNA\Data\hainanDataScraping\tables\\' + filename + '\\tb' + str(year) + '.xlsx')
    tbadd = tbadd.iloc[:, 4:]
    tbmerge = pd.merge(base, tbadd, how='outer', on='index', sort=False)
    base = tbmerge

# 矩阵转置，加字段名称，写入table
colnm = tbmerge.iloc[:, 3].tolist()
colnm[-1] = '年份'
tbmerget = pd.DataFrame(tbmerge.iloc[:, 5:].values.T, columns=colnm)
tbmerget.to_excel(r'D:\CityDNA\Data\hainanDataScraping\tablesT\\' + filename + '.xlsx')


# test last one file
os.chdir(r'D:\CityDNA\Data\hainanDataScraping\tablesT')
files = os.listdir()
for i in range(len(fileinfo)):
    filename = fileinfo['filename'][i]
    for j in range(len(files)):
        if filename in files[j]:
            print(str(i) + ' in files')

# 海南数据表格处理，按照代码字典修改
os.chdir(r'D:\CityDNA\Data\hainanDataScraping\tablesT')
files = os.listdir()

codedic = pd.read_excel(r'D:\CityDNA\Data\hainanDataScraping\代码字典.xlsx')
codediccol = codedic.columns.values.tolist()
dicname = ['按能源种类分（按使用能源分类, 按能源的基本形态分类）', '地区', '按消费类型分（按消费形态分）', '按三大产业分组', '按加工转换方式分',
           '按轻重工业分', '按商品交易市场类别分', '按农产品种类分类(按农林牧渔业分)']


i = 12
for i in range(len(files)):
    print(str(i) + files[i] + '...')
    df = pd.read_excel(files[i])
    dfnew = df
    dfcol = df.columns.values.tolist()
    for j in range(len(dfcol)):
        for k in range(len(dicname)):
            if dfcol[j] in dicname[k] or dfcol[j] == dicname[k] or dicname[k] in dfcol[j]:
                print(dicname[k])
                for l in range(len(df[dfcol[j]])):
                    for m in range(len(codedic[dicname[k]])):
                        # print('————————' + str(df[dfcol[j]][l]))
                        # print('————————' + str(codedic[dicname[k]][m]))
                        if str(df[dfcol[j]][l]) == str(codedic[dicname[k]][m]):
                            # print(codedic[dicname[k]][m])
                            dfnew[dfcol[j]][l] = codedic[dicname[k] + 'name'][m]

    dfnew.to_excel(r'D:\CityDNA\Data\hainanDataScraping\tablesT2\\' + files[i])
    print(str(i) + files[i] + ' finished!')



