import os.path
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import string
from lxml import etree
import lxml.html
import requests
import json
import pandas as pd
from nltk import tokenize
from operator import itemgetter
import math
import nltk
from random import randint
from time import sleep

# nltk.download('punkt')
# nltk.download('stopwords')

stop_words = set(stopwords.words('english'))

#search = "product manager"
#location = "san francisco california"

print("Search keywords: ")
search = input()
print("Search keywords: " + search)

print("Search location: ")
location = input()
print("Search location: " + location)

filename = str('outputs\\'+search+" " +
               location+'.csv').replace(" ", "")
print('filename: '+str(filename))

num = 0
df_0 = pd.DataFrame()
num_rows = df_0.shape[0]
print('num_rows: '+str(num_rows))

if os.path.isfile(filename):
    jkid = pd.read_csv(filename)
    try:
        jkid = jkid['jk'].to_list()
    except:
        print('error in job key id lookup...')
        jkid = []

else:
    jkid = []

print('list of existing Job Key IDs:')
print(len(jkid))
print(jkid)

# try:
while num_rows <= 1500:
    url = 'https://www.indeed.com/jobs?q=' + \
        search.replace(" ", '+')+'&l='+location.replace(" ",
                                                        '+') + '&start=' + str(num)

    html = requests.get(url)
    # print(html.headers)
    doc = lxml.html.fromstring(html.content)
    # print(doc)

    print('URL to scrape: \n' + str(url))

    script = doc.xpath('//script/text()')

    # print(len(script))
    # if len(script) == 0:
    #    print('--- NO RESULTS RETURNED ---')
    #    break

    nextpage = doc.xpath('//a/href/text()')
    nextpage

    for s in script:
        if "var jobmap = {};" in s:
            listings = s.split("var jobmap = {};")[1].split(";\n")
            print('Num Listings Found: '+str(len(listings)))

    c = []

    for l in listings:
        a = (l.split(']= ')[-1]).strip()
        a = a.split("',")
        for b in a:
            b = b.replace(":'", ":").replace(": '", ":").replace(
                "{", "").replace("'}", "").strip()
            b = str(b.split(":")[0])
            if b not in c and len(b) > 0:
                c.append(b)
    # print(c)

    df = {}

    for col in c[:]:
        splitter = str(col) + ":"
        l_vals = []
        for l in listings:
            l = l.split("}")[0].split(
                splitter)[-1].split(',')[0].strip().strip("'")
            l_vals.append(l)
        d = {col: l_vals}
        # print(d)
        # print(len(l_vals))
        # print('\n')
        df.update(d)

    df = pd.DataFrame(df)

    nan_value = float("NaN")
    df.replace("", nan_value, inplace=True)
    df.dropna(subset=['jk'], inplace=True)

    def get_description(jk):
        url = 'https://www.indeed.com/viewjob?jk=' + str(jk)

        sleepy = randint(5, 15)
        #print("--- SLEEPING --- "+str(sleepy)+" seconds")
        sleep(sleepy)
        print('Scraping Description: '+url)
        html = requests.get(url)
        doc = lxml.html.fromstring(html.content)
        text = doc.xpath('//*[@id="jobDescriptionText"]//text()')
        text = '\n'.join(text)
        return text

    def score_keywords(doc):
        total_words = doc.split()

        table = str.maketrans('', '', string.punctuation)
        total_words = [w.translate(table) for w in total_words]
        total_words = [w.lower() for w in total_words]
        total_words = [w for w in total_words if w.isalpha()]

        total_word_length = len(total_words)

        total_sentences = tokenize.sent_tokenize(doc)
        total_sent_len = len(total_sentences)

        tf_score = {}
        for each_word in total_words:
            each_word = each_word.replace('.', '')
            if each_word not in stop_words:
                if each_word in tf_score:
                    tf_score[each_word] += 1
                else:
                    tf_score[each_word] = 1

        tf_score.update((x, y/int(total_word_length))
                        for x, y in tf_score.items())

        def check_sent(word, sentences):
            final = [all([w in x for w in word]) for x in sentences]
            sent_len = [sentences[i]
                        for i in range(0, len(final)) if final[i]]
            return int(len(sent_len))

        idf_score = {}
        for each_word in total_words:
            each_word = each_word.replace('.', '')
            if each_word not in stop_words:
                if each_word in idf_score:
                    idf_score[each_word] = check_sent(
                        each_word, total_sentences)
                else:
                    idf_score[each_word] = 1

        try:
            idf_score.update((x, math.log(int(total_sent_len)/y))
                             for x, y in idf_score.items())
        except:
            idf_score.update((x, math.log(int(total_sent_len)/1))
                             for x, y in idf_score.items())

        tf_idf_score = {key: tf_score[key] *
                        idf_score.get(key, 0) for key in tf_score.keys()}
        res = dict(sorted(tf_idf_score.items(),
                          key=itemgetter(1), reverse=True)[:100])
        return res

    print('before de-dupe: '+str(df.shape[0]))
    df = df[~df['jk'].isin(jkid)]
    print('after de-dupe: '+str(df.shape[0]))

    if df.shape[0] == 0:
        num = num + 10
        print('--- ONLY DUPES... trying again ---')
        sleepy = randint(5, 20)
        print("--- SLEEPING --- "+str(sleepy)+" seconds")
        sleep(sleepy)
        continue

    for row in df['jk'].to_list():
        jkid.append(row)

    df['description'] = df.apply(
        lambda row: get_description(row['jk']), axis=1)

    df['keywords'] = df.apply(
        lambda row: score_keywords(row['description']), axis=1)

    print("New df shape: " + str(df.shape))
    #df_0 = df_0.append(df, ignore_index=True)
    # print(df_0)
    #print("New df_0 shape: " + str(df_0.shape))
    num = num + 10
    num_rows = num_rows + df.shape[0]
    print('num_rows: '+str(num_rows))
    sleepy = randint(5, 20)
    print("--- SLEEPING --- "+str(sleepy)+" seconds")
    sleep(sleepy)

    if os.path.isfile(filename):
        print("File exist")
        df_file = pd.read_csv(filename)
        os.remove(filename)
        df_file = df_file.append(df, ignore_index=True)
        df_file.drop_duplicates(subset=['jk'], keep='first', inplace=True)
        df_file.to_csv(filename, index=False)
        print('file updated, '+str(df_file.shape[0])+' rows')
    else:
        print("File not exist")
        df.to_csv(filename, index=False)
        print('file created, '+str(df.shape[0])+' rows')
# except:
#    print('--- STOPPED BY ERROR ---')

# df_0.to_csv("indeed_scraper2.csv")
