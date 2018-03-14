# scraper for education-evaluations
# format for inputfile: VAKACODE-YEAR (example: FEM11090-15)
import os # os interactions, from std library
import re # regular expressions, from std library
import logging # logging, from std library
import sys # system interactions, from std library
from datetime import datetime # for dates + times, from std. library

import requests
from bs4 import BeautifulSoup

# globals
SPSS_DIR = 'SIN_SPSS'
EXCEL_DIR = 'SIN_excel'
DEFINITION_DIR = 'SIN_definitions'
LOGGING_DIR = 'logs'

# set up logging
logging.getLogger("urllib3").setLevel(logging.WARNING) # disable verbose logging by urllib
logging.getLogger("requests").setLevel(logging.WARNING) # disable verbose logging by requests
logging.basicConfig(filename = os.path.join(os.getcwd(), 
                                LOGGING_DIR, 'download_' + datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '.log'), 
                                level = logging.INFO,
                                format = '%(asctime)s: %(message)s', datefmt = '%Y_%m_%d_%H:%M:%S')

# functions
def sin_login():
    # login, return session
    sess = requests.session()
    pwd = # inclide pwd from local file
    payload = {'login_user' : 'evaluaties@few', 'login_passwd' : pwd}
    sess.post('https://ese.sin-online.nl/channel/index.html?SSOIMPORT=NONE', data = payload)
    print 'Logged into SIN-online'
    logging.info('Logged into SIN-online')

    return sess

def download_results(filename, sess):
    # request channel page for course and find channel id
    print filename, ': get channel ID,',
    url = 'http://ese.sin-online.nl/channel/pub/channel.html?mod=' + filename
    channel_page = sess.get(url).text
    if channel_page:
        soup_channel_page = BeautifulSoup(channel_page, 'lxml')
        channel_title_tag = str(soup_channel_page.find('h2', class_ = 'channel_title'))
        channel_title = re.findall('#([0-9]+)', channel_title_tag)
        if channel_title:
            channel_title = channel_title[0]
            print 'done,',
        else:
            print 'no channel title found.'
            logging.info('FAILURE, no channel title found, %s skipped', filename)
            return None
    else:
        print 'problem loading channel page.'
        logging.info('FAILURE: problem loading channel page, %s skipped', filename)
        return None

    # request questionnaire page for found channel id and find course evaluation ids
    print 'get course eval ID(s),',
    url = 'http://ese.sin-online.nl/channel/quest/object.html?chid=' + channel_title
    evaluation_page = sess.get(url).text
    if evaluation_page:
        soup_evaluation_page = BeautifulSoup(evaluation_page, 'lxml')
        evaluation_id_tags = soup_evaluation_page.find_all('a', string = re.compile('course evaluation|onderwijsevaluatie', re.IGNORECASE)) 
        if evaluation_id_tags:
            evaluation_id_collection = {}
            for tag in evaluation_id_tags:
                evaluation_id = re.findall('objid=([0-9]+)', str(tag))
                if evaluation_id:
                    evaluation_id_collection[evaluation_id[0]] = tag.text.lower()
            print 'done,',
        else:
            print 'no course eval ID(s) found.'
            logging.info('FAILURE: no course eval ID(s) found, %s skipped', filename)
            return None
    else:
        print 'problem loading evaluation page.'
        logging.info('FAILURE: problem loading evaluation page, %s skipped', filename)
        return None

    # request definitions for found course evaluation id(s) and save to file
    print 'get defs,',
    for evaluation_id in evaluation_id_collection:
        url = 'https://ese.sin-online.nl/channel/quest/dump.html-nf?objid=' + evaluation_id
        definitions = sess.get(url).text
        if definitions:
            with open(os.path.join(DEFINITION_DIR, evaluation_id + '.txt'), 'w') as file:
                file.write(definitions.encode('utf-8'))
        else:
            print 'defs not found.'
            logging.info('FAILURE: course definitions not not found, %s (eval_id: %s) skipped', filename, evaluation_id)
            return None
    print 'done,',
    
    # request spss downloader for found course evaluation id(s) and save to file
    print 'get results,',
    for evaluation_id in evaluation_id_collection:
        url = 'http://ese.sin-online.nl/channel/quest/report_spss.html-nf?objid=' + evaluation_id
        results = sess.get(url).text
        if results:
            filename = filename.replace('-', '_')
            with open(os.path.join(SPSS_DIR, filename + '_' + evaluation_id_collection[evaluation_id] + '_' + evaluation_id + '.txt'), 'w') as file:
                file.write(results.encode('utf-8'))
                logging.info('SUCCESS: course: %s, evaluation id: %s, processed', filename, evaluation_id)
        else:
            logging.info('FAILURE: course evaluation results not found, %s (eval_id: %s) skipped', filename, evaluation_id)
            return None
    print 'done,',
    
    # request excel workbooks with open answers for found course evaluation id(s) and save to file
    print 'get open answers,',
    for evaluation_id in evaluation_id_collection:
        url = 'http://ese.sin-online.nl/channel/quest/report_xls.html?objid=' + evaluation_id
        xls = sess.get(url).content
        if xls:
            with open(os.path.join(EXCEL_DIR, evaluation_id + '.xls'), 'wb') as file:
                file.write(xls)
        else:
            print 'open answers not found, ',
    
    print 'done.'

# main
if len(sys.argv) < 2:
    print 'Provide name inputfile as command line argument'
    exit()
logging.info('Start log')
start_time = datetime.now()
sess = sin_login()
files = open(sys.argv[1])
for filename in files:
    filename = filename.rstrip()
    download_results(filename, sess)
end_time = datetime.now()
print 'Elapsed time:', str(end_time - start_time).split('.')[0]