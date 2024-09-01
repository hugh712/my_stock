#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from os.path import exists
import pandas as pd
import io
import os
import datetime
import numbers
import sqlite3
import datetime



def parse_args():
    import argparse

    usage = '%(prog)s [options]'
    parser = argparse.ArgumentParser(usage=usage)
    parser.add_argument(
        '--year',
        dest='target_year',
        default='',
        type=str,
        help='the target year to query',
    )   
    parser.add_argument(
        '--company',
        dest='target_company',
        default='',
        type=str,
        help='the target company to query',
    ) 
    parser.add_argument(
        '--sup',
        dest='sumup',
        action="store_true",
        help='sumup for the year',
    ) 
    parser.add_argument(
        '--ssup',
        dest='ssumup',
        action="store_true",
        help='sumup for the year for each stock',
    ) 
    args, _ = parser.parse_known_args()
    return args

def parse_necessaries(target_list):
    # only need time, name and deposit
    found_datetime = False
    necessaries = []
    for ll in target_list:
        if isinstance(ll, datetime.date):
            found_datetime = True
            necessaries.append(ll.strftime("%Y-%m-%d"))
        elif found_datetime and isinstance(ll, numbers.Number):
            necessaries.append(ll)
            break
    last_column = target_list[-1].split(" ")[0] 
    necessaries.append(last_column)
    return necessaries


def remove_list_empty(target_list):
    new_list = []
    skip_words = ["nan", "NaT"]
    for  ll in target_list:
        if str(ll) in skip_words:
            continue
        else:
            new_list.append(ll)
    return new_list

def update_db(excel_folder, db_path):

    keywords = ["現金股息","基金配息"]
    final_dict = {}
    
    for filename in os.listdir(excel_folder):
        f = os.path.join(excel_folder, filename)
        if not f[-5:] == ".xlsx":
            continue
        df = pd.read_excel(f)
        df = df.reset_index()

        #excel format ["Date","Memo","Withdrawal","Deposit","Balance","Remarks"]
        for index, row in df.iterrows():
            row_list = row.to_list()
            row_list = remove_list_empty(row_list)
            for keyword in keywords:
                if keyword in row_list:
                    tmp_list = parse_necessaries(row_list)
                    if not tmp_list[2] in final_dict:
                        final_dict[tmp_list[2]] = {}
                    final_dict[tmp_list[2]][tmp_list[0]]=tmp_list[1] 
    
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='stock' ''')
    
    #if the count is 1, then table exists
    cur.execute('CREATE TABLE IF NOT EXISTS stock("company" TEXT, "date" DATE, "money" INTEGER)')
    
    for company in final_dict:
        for date in final_dict[company]:
            tmp_date_time = datetime.datetime.strptime(date, '%Y-%m-%d')
            cur.execute("insert or ignore into stock (company, date, money) values (?, ?, ?)", (company, tmp_date_time, final_dict[company][date]))
            con.commit()

    con.close()

def query_db(db_path, company, year, sumup, ssumup):
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cmd_extra=""

    if company:
        cmd_extra += 'company=\'{}\' '.format(company)

    
    if year:
        if company:
            cmd_extra += ' and '
        cmd_extra += 'strftime(\'%Y\', date)=\'{}\' '.format(year)


    if not cmd_extra == "":
        cmd_extra = "WHERE " + cmd_extra
    
    cmd='''SELECT *  FROM stock {};'''.format(cmd_extra)
    cur.execute(cmd)
    print_db(cur.fetchall(), sumup, ssumup)

    con.close()

def print_db(db_data, sumup, ssumup):

    if sumup:
        sum=0
        for line in db_data:
            sum+=line[2]

        print("Sum is ", sum)
        return 0
    elif ssumup:
        #pdb.set_trace()
        stock_dict={}
        for line in db_data:
            if line[0] in stock_dict:
                stock_dict[line[0]]=stock_dict[line[0]] + line[2]
            else:
                stock_dict[line[0]]=line[2]
        for line in stock_dict:
            print('[%5s %s]' % (stock_dict[line], line))
        return 0
    else:
        
        for line in db_data:
            print(line)


if __name__ == '__main__':
    args = parse_args()
    excel_path="Yuanta" 
    db_path="stock.db"
    
    # due to I dont know how to udpate the db content without duplicate, so remove the db file for now
    os.remove(db_path)
    update_db(excel_path, db_path)
    query_db(db_path, args.target_company, args.target_year,args.sumup, args.ssumup)
