import json
import requests
import re
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from pandas import json_normalize
from datetime import datetime
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from cfuzzyset import cFuzzySet as FuzzySet

investment_categories = [
    {
        'category': 'non_affiliated',
        'type': 'investment',
        'terms': ['noncontrollednonaffiliate',
                  'noncontrollednonaffiliated',
                  'noncontrolledunaffiliate',
                  'noncontrolledunaffiliated'
                  'noncontrolnonaffiliate',
                  'noncontrolnonaffiliated',
                  'noncontrolunaffiliate',
                  'noncontrolunaffiliated']
    },
    {
        'category': 'affiliated',
        'type': 'investment',
        'terms': ['noncontrolledaffiliate',
                  'noncontrolledaffiliated',
                  'noncontrolaffiliate',
                  'noncontrolaffiliated',
                  'affiliate']
    },
    {
        'category': 'control',
        'type': 'investment',
        'terms': ['controlledaffiliate',
                  'controlledaffiliated',
                  'controlaffiliate',
                  'controlaffiliated',
                  'control']
    },
    ]

income_categories = [
    {
        'category': 'interest',
        'type': 'income',
        'terms': ['interestincome',
                  'incomeexcluding']
    },
    {
        'category': 'dividend',
        'type': 'income',
        'terms': ['dividend']
    },
    {
        'category': 'fee',
        'type': 'income',
        'terms': ['fee']
    },
    {
        'category': 'total',
        'type': 'income',
        'terms': ['total', 
                  'interestfeeanddividend',
                  'totalinvestment']
    },
    { 
        'category': 'pik',
        'type': 'income',
        'terms': ['pik', 
                  'paymentinkind', 
                  'paidinkind']
    },
    {
        'category': 'other',
        'type': 'income',
        'terms': ['other']
    }
]

def extract_investment_rows (df, start_cell_str, end_cell_str):
    try:
        start_row = df[df.iloc[:, 0].apply(clean_text_2) == start_cell_str].index[0]
        end_row = df[df.iloc[:, 0].apply(clean_text_2) == end_cell_str].index[0]
    except IndexError:
        print("Start or end cell string not found in the DataFrame.")
        return
    portion = df.iloc[start_row + 1 : end_row + 1, :]
    return portion

def clean_text_2 (row):
    no_punc = re.sub(r'[^\w\s]', '', row)
    lower = no_punc.lower()
    no_space = re.sub(r'\s+','', lower)
    return no_space

def find_income (row):
    row = row.lower()
    if 'income' in row:
        return row.split('income')[0]
    else: return row

def clean_investment (string):
    words = string.lower().split()
    output_string = ''
    for word in words:
        if 'control' in word or 'affiliate' in word:
            output_string += word
    return clean_text_2(output_string)

def clean_income (string):
    return clean_text_2(find_income(string))

def top_score (row, terms, fuzzy_function, clean_function):
    scores = []
    standard_row = clean_function (row)
    for search_string in terms:
        # standard_term = clean_function (search_string)
        score = fuzzy_function (standard_row, search_string)
        # if standard_row in standard_term:
        #     score += 10
        scores.append(score)
    # print(scores)
    if max(scores) >= 40:
        return max(scores)
    else: return 0

def best_match (string, dict, fuzzy_function, clean_function):
    best_match, best_match_type, best_match_score = '', '', 0
    for key in dict:
        score = top_score (string, key['terms'], fuzzy_function, clean_function)
        if score > best_match_score:
            best_match, best_match_type, best_match_score = key['category'], key['type'], score
    if best_match_score <= 50:
        best_match, best_match_type = 'No match', 'No match'
    return best_match, best_match_type, best_match_score

def generate_headings (df):
    new_df = df.copy()
    headings = []
    current_heading, current_heading_type = 'No match', 'investment'
    for i in range(len(df)):
        row = df.iloc[i]
        if pd.isnull(row[1]) and pd.isnull(row[2]):
           if clean_investment(row[0]) != '':
              heading_guess, heading_guess_type, _ = best_match (row[0], investment_categories, fuzz.token_set_ratio, clean_investment)
           else: 
              heading_guess, heading_guess_type, _ = best_match (row[0], income_categories, fuzz.token_set_ratio, clean_income)
           current_heading, current_heading_type = heading_guess, heading_guess_type
        headings.append((current_heading, current_heading_type))
    # Account for last row
    headings[-1] = ('total', 'investment')
    new_df['headings'] = headings
    return new_df

def generate_subheadings (df):
    new_df = df.copy()
    subheadings = []
    for i in range(len(df)):
        row = df.iloc[i]
        _, current_heading_type = row['headings']
        if not(pd.isnull(row[1]) and pd.isnull(row[2])):
           if current_heading_type == 'investment':
               subheading_guess, subheading_guess_type, _ = best_match (row[0], income_categories, fuzz.partial_token_set_ratio, clean_income)
           if current_heading_type == 'income':
                subheading_guess, subheading_guess_type, _ = best_match (row[0], investment_categories, fuzz.token_set_ratio, clean_investment)
           subheadings.append((subheading_guess, subheading_guess_type))
        else:
            subheadings.append(('No match', 'investment'))
    # Account for last row
    subheadings[-1] = ('total', 'income')
    new_df['subheadings'] = subheadings
    return new_df

def generate_coordinates (df):
    return generate_subheadings(generate_headings(df))

def create_investment_dicts ():
    investment_dicts = []
    for investment in ['non_affiliated', 'affiliated', 'control']:
        for income in ['interest', 'total', 'dividend', 'pik', 'fee', 'other']:
            investment_dicts.append({
                'investment': investment,
                'income': income,
                'value1': pd.NA,
                'value2': pd.NA
            })
    investment_dicts.append({
        'investment': 'total',
        'income': 'total',
        'value1': pd.NA,
        'value2': pd.NA
    })
    return investment_dicts

def format_in_dicts (df):
    new_dicts = create_investment_dicts ()
    for i in range(len(df)):
        row = df.iloc[i]
        heading, _ = row['headings']
        subheading, _ = row['subheadings']
        for dict in new_dicts:
            if ((dict['investment'] == heading and dict['income'] == subheading) 
                or (dict['investment'] == subheading and dict['income'] == heading)):
                dict['value1'] = row[1]
                dict['value2'] = row[2]
    return new_dicts

df = pd.read_csv('owl_rock_technology_finance_corp_operations.csv')
df_investments = extract_investment_rows(df, 'investmentincome', 'totalinvestmentincome')
formatted_df = generate_coordinates(df_investments)
formatted_dicts = format_in_dicts(formatted_df)