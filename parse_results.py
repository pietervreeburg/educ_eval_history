#  parser for education evaluations (spps files)
import re # for regular expressions, from std library
import os # for os level functions, from std library
import json # to import / export json, from std library
import logging # enables logging, from std library
import io # to open files with explicit unicode encoding, from std library
from collections import Counter # special dictionary for counting stuff, from std library
from datetime import datetime # for dates + times, from std. library

from langdetect import detect # to detect question language
from sqlalchemy import create_engine, Table, MetaData, select # for sql expressions
import xlrd # to read MS Excel-files

# globals
# dirs
SPSS_DIR = 'SIN_SPSS'
#SPSS_DIR = 'SIN_SPSS_debug' # place testfiles here for debugging
EXCEL_DIR = 'SIN_excel'
DEFINITION_DIR = 'SIN_definitions'
LOGGING_DIR = 'logs'

# vars
NA_VALUE = None
FAC = 'FEW'

# set up logging
logging.basicConfig(filename = os.path.join(os.getcwd(), 
                        LOGGING_DIR, 'parse_' + datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '.log'), 
                        level = logging.INFO,
                        format = '%(asctime)s: %(message)s', datefmt = '%Y_%m_%d_%H:%M:%S')

# functions
def write_json_file(table, writefile):
    outfile = open(writefile + '.txt', 'w')
    json.dump(table, outfile, indent = 4)
    
def reflect_db(db):
    tables = {}
    meta = MetaData()
    tables['table_evaluation'] = Table('EVAL_EVALUATIE', meta, autoload = True, autoload_with = db)
    tables['table_questions'] = Table('EVAL_VRAAG', meta, autoload = True, autoload_with = db)
    tables['table_evaluation_questions'] = Table('EVAL_EVALUATIE_VRAAG', meta, autoload = True, autoload_with = db)
    tables['table_mc_answer_options'] = Table('EVAL_VRAAG_ANTW_MC', meta, autoload = True, autoload_with = db)
    tables['table_open_answer_options'] = Table('EVAL_VRAAG_ANTW_OPEN', meta, autoload = True, autoload_with = db)
    tables['table_mc_results'] = Table('EVAL_EVALUATIE_RESULT_MC', meta, autoload = True, autoload_with = db)
    tables['table_open_results'] = Table('EVAL_EVALUATIE_RESULT_OPEN', meta, autoload = True, autoload_with = db)
    
    return tables
    
def parse_spss_file(file):
    # parse SPSS files and return dict parsed
    parsed = {}
    lines = io.open(os.path.join(SPSS_DIR, file), encoding = 'utf-8').read().splitlines()

    # parse question_ids
    if len(lines) == 0:
        logging.info('File contains no usable data, file skipped')
        return None
    question_ids = [col for col in lines[0].split(')')[-1][:-1].strip().split(' ')]
        
    # parse results
    line_num = 2
    for line in lines[line_num:]:
        line_num += 1
        if line == 'END DATA.':
            break
        row = line.split(',')
        respondent_id = row[0]
        question_results = row[1:]
        for question_id, question_result in zip(question_ids, question_results):
            question_result = int(question_result)
            if question_id not in parsed:
                parsed[question_id] = {
                'text' : NA_VALUE,
                'tag' : NA_VALUE,
                'language' : NA_VALUE,
                'person' : NA_VALUE,
                'session_form' : NA_VALUE,
                'question_ordernr' : NA_VALUE,
                'type' : NA_VALUE,
                'db_question_id' : NA_VALUE,
                'answer_options' : {},
                'results' : {}
                }
            parsed[question_id]['results'][respondent_id] = question_result
    if len(parsed) == 0:
        logging.info('File contains no usable results, file skipped')
        return None
 
    # parse questions, incl name, sessionform and question order
    line_num += 3
    question_ordernr = 1
    for line in lines[line_num:]:
        if line == '':
            break
        line_num += 1
        line_items = line[1:].split('\t')
        question_id, text = [item for item in line_items if len(item)>0]
        text = ' '.join(text.split()) # remove double spaces
        if question_id not in parsed:
            continue
        
        # use raw string notation for regular expressions, prevents a lot of escaping backslashes
        parse_name = re.compile(r'''
            mentor\s(\S\S.+?)(?:\bheeft\b|\bhas\b|$)|
            docent\s(\S\S.+?)(?:\bheeft\b|\bmaakt\b|\bgeeft\b|$)|
            lecturer\s(\S\S.+?)(?:\bhas\b|\bmakes\b|\bgives\b|\bprovides\b|\bis\b|$)''',
            re.IGNORECASE | re.VERBOSE)
        name = parse_name.findall(text)
        if name:
            name = ''.join(name[0]).strip()
            if name.startswith('heb'): # for name parsing edge cases
                name = None
            else:
                text = text.replace(name, '%p')
                parsed[question_id]['person'] = name

        text = re.sub('[^A-Za-z %]', '', text) # remove punctuation after parse_name to retain punctuation in name (except %)
                
        parse_session_form = re.compile(r'''
            (sommen|vaardigheden|(?<![+])practic|mentor)|
            (skills|(?<![+])tutorial|exercise|mentor)''',
            re.IGNORECASE | re.VERBOSE)
        session_form = parse_session_form.findall(text)
        if session_form:
            session_form = ''.join(session_form[0]).strip().lower()
            if session_form == 'sommen' or session_form == 'exercise':
                session_form = 'exercise lecture'
            elif session_form == 'vaardigheden' or session_form == 'skills':
                session_form = 'skills tutorial'
            elif session_form == 'mentor':
                session_form = 'guidance'
            else:
                session_form = 'tutorial'
            # replacing the session form in the question text is too complicated for now, implement later
            parsed[question_id]['session_form'] = session_form
        
        parsed[question_id]['text'] = text
        parsed[question_id]['question_ordernr'] = question_ordernr
        parsed[question_id]['type'] = 'mc'
        question_ordernr += 1
    
    # parse answer options
    line_num += 2
    previous_question_id = None
    for line in lines[line_num:]:
        line_items = [item for item in line.split('\t') if len(item)>0]
        if 'VAR' in line_items[0]:
            question_id, answer_option_id, answer_option_text = line_items
            if question_id.startswith('/'):
                question_id = question_id[1:]
            previous_question_id = question_id
        else:
            question_id = previous_question_id
            answer_option_id, answer_option_text = line_items
        if question_id not in parsed:
            continue
        answer_option_id = int(answer_option_id[1:-1])
        parsed[question_id]['answer_options'][answer_option_id] = answer_option_text[1:-1]
    
    # detect language for every mc question, use language with highest count
    language_collection = Counter()
    for question_id, content in parsed.items():
        language = detect(content['text'])
        language_collection[language] += 1
    language = language_collection.most_common()[0][0]
    for question_id, content in parsed.items():
        parsed[question_id]['language'] = language
    
    # find corresponding definition file and add question definitions to parsed
    SIN_evaluation_id = file[:-4].split('_')[-1]
    try:
        definition_file = io.open(os.path.join(DEFINITION_DIR, SIN_evaluation_id + '.txt'), 
            encoding = 'utf-8').read().splitlines()
    except IOError:
        logging.info('FAILURE: no definition file found, file skipped')
        return None
    for line in definition_file:
        if line.startswith('#Q'):
            question_id_tag = line[2:].split('<br>')[0]
            for ind, char in enumerate(question_id_tag):
                if not char.isdigit():
                    question_id = question_id_tag[:ind]
                    question_tag = question_id_tag[ind:]
                    parsed['VAR' + question_id]['tag'] = question_tag
                    break
    
    # parse excel files for open answers and add to parsed
    try:
        excelbook = xlrd.open_workbook(os.path.join(EXCEL_DIR, SIN_evaluation_id + '.xls'), 
            logfile = open(os.path.join(LOGGING_DIR, 'xlrd.log'), 'w'))
    except IOError:
        logging.info('WARNING: no results for open answers available, mc results parsed')
        return parsed
    questionsheet = excelbook.sheet_by_index(2)
    rows = questionsheet.get_rows()
    question_ordernr = -1 # to account for the header row in the questionsheet
    for question_id, question_text, question_type in rows:
        question_ordernr += 1
        if question_type.value == 'OPEN':
            question_id = str(question_id.value)[:-2] # slice to get rid of the decimal .0
            answersheet = excelbook.sheet_by_index(1)
            label_row = answersheet.row_values(rowx = 0)
            for column_index, column_label in enumerate(label_row):
                column_label = column_label.split(' ')[-1]
                if column_label == question_id:
                    answerrows = answersheet.get_rows()
                    next(answerrows)
                    for row in answerrows:
                        result = row[column_index].value
                        if result:
                            parsed.setdefault(question_id, {
                            'text' : question_text.value,
                            'tag' : NA_VALUE,
                            'language' : language,
                            'person' : NA_VALUE,
                            'session_form' : NA_VALUE,
                            'question_ordernr' : question_ordernr,
                            'type' : 'open',
                            'db_question_id' : NA_VALUE,
                            'answer_options' : NA_VALUE,
                            'results' : {}
                            })['results'][row[0].value] = row[column_index].value

    # write parsed file for debugging purposes
    write_json_file(parsed, os.path.join(os.getcwd(), 'debug_files', 'parsed_file_' + file))

    return parsed

def insert_data(file, parsed, connection, tables):
    # insert evaluation
    code, year, SIN_evaluation_name, SIN_evaluation_id = file[:-4].split('_', 3)
    year = int('20' + year)
    SIN_evaluation_id = int(SIN_evaluation_id)
    block = re.findall(r'block(.+?)\D|blok(.+?)\D', SIN_evaluation_name)
    if block:
        block = ''.join(block[0]).strip()
    else:
        logging.info('IMPORT ERROR: block not listed in filename')
        return None
    sql_select = select([tables['table_evaluation']])
    result = connection.execute(sql_select)
    for item in result:
        if item['EVL_SIN_ID'] == SIN_evaluation_id:
            logging.info('IMPORT ERROR: file already imported')
            return None
    ins = connection.execute(
        tables['table_evaluation'].insert().values({
        'EVL_JAAR' : year,
        'EVL_PERIODE' : block,
        'EVL_VAK' : code,
        'EVL_FAC' : FAC,
        'EVL_SIN_ID' : SIN_evaluation_id
        })
        )
    key_evaluation = ins.inserted_primary_key[0]
    logging.debug('Inserted evaluation')
    
    # insert questions, mc_answer_options and open_answer_options
    for question_id, content in parsed.items():
        if content['type'] == 'mc':
            parsed_question_ao = {}
            for answer_option_id, answer_option_text in content['answer_options'].items():
                parsed_question_ao.setdefault(content['text'], {})[answer_option_id] = answer_option_text
            sql_select = select([tables['table_questions'], 
                                 tables['table_mc_answer_options']]).where(
                                 tables['table_questions'].c.VRG_ID == 
                                 tables['table_mc_answer_options'].c.AMC_VRAAG)
            result = connection.execute(sql_select)
            db_question_data = {}
            for row in result:
                db_question_data.setdefault(
                    row['VRG_ID'],
                    {}).setdefault(row['VRG_TEXT'],
                    {})[row['AMC_ORDERID']] = row['AMC_TEXT']
            for db_question_id, db_question_ao in db_question_data.items():
                if db_question_ao == parsed_question_ao:
                    parsed[question_id]['db_question_id'] = db_question_id
                    break
        else: # type == open
            parsed_question = content['text']
            sql_select = select([tables['table_questions']])
            result = connection.execute(sql_select)
            for row in result:
                if row['VRG_TEXT'] == parsed_question:
                    parsed[question_id]['db_question_id'] = row['VRG_ID']
                    break
        if parsed.get(question_id).get('db_question_id') == None:
            ins = connection.execute(tables['table_questions'].insert().values({
                'VRG_TEXT' : content['text'],
                'VRG_TAG' : content['tag'],
                'VRG_TAAL' : content['language'],
                'VRG_TYPE' : content['type'],
                'VRG_SCHAAL' : NA_VALUE,
                'VRG_N_ANTW' : 1,
                'VRG_FAC' : FAC
                })
                )
            key_question = ins.inserted_primary_key[0]
            parsed[question_id]['db_question_id'] = key_question
            if content['type'] == 'mc':
                for answer_option_id, answer_option_text in content['answer_options'].items():
                    connection.execute(tables['table_mc_answer_options'].insert().values({
                        'AMC_VRAAG' : key_question,
                        'AMC_TEXT' : answer_option_text,
                        'AMC_ORDERID' : answer_option_id,
                        'AMC_FAC' : FAC
                        })
                        )
            else:
                connection.execute(tables['table_open_answer_options'].insert().values({
                    'AOP_VRAAG' : key_question,
                    'AOP_FAC' : FAC
                    })
                    )
                
        
    logging.debug('Inserted questions & answer options')
    
    # insert evaluation_questions, mc_results and open results
    for question_id, content in parsed.items():
        ins = connection.execute(tables['table_evaluation_questions'].insert().values({
        'EEV_EVALUATIE' : key_evaluation,
        'EEV_VRAAG' : content['db_question_id'],
        'EEV_PARAM_DOCENT_NAAM' : content['person'],
        'EEV_PARAM_DOCENT' : NA_VALUE,
        'EEV_PARAM_SESSIE' : content['session_form'],
        'EEV_ORDERID' : content['question_ordernr'],
        'EEV_FAC' : FAC
        })
        )
        key_evaluation_questions = ins.inserted_primary_key[0]
        parsed[question_id]['evaluation_question_id'] = key_evaluation_questions
    
        for respondent_id, result in content['results'].items():
            if result == -1:
                continue
            if content['type'] == 'mc':
                connection.execute(tables['table_mc_results'].insert().values({
                'RMC_STUDENT' : respondent_id,
                'RMC_ANTWOORD_SUBID' : result,
                'RMC_ANTWOORD_ID' : content['db_question_id'],
                'RMC_VRAAG' : key_evaluation_questions,
                'RMC_FAC' : FAC
                })
                )
            else: # type == open
                connection.execute(tables['table_open_results'].insert().values({
                    'ROP_STUDENT' : respondent_id,
                    'ROP_CONTENT' : result,
                    'ROP_ANTWOORD_ID' : content['db_question_id'],
                    'ROP_VRAAG' : key_evaluation_questions,
                    'ROP_FAC' : FAC
                    })
                    )
            
    logging.debug('Inserted evaluation_questions & results')

    # write keyed file for debugging purposes
    write_json_file(parsed, os.path.join(os.getcwd(), 'debug_files', 'keyed_file_' + file)) 
    
    return True

def main():
    logging.info('Start log')
    start_time = datetime.now()
    db = create_engine('sqlite:///test_evaldb.db', echo = False)
    tables = reflect_db(db)
    connection = db.connect()
    files = os.listdir(SPSS_DIR)
    total_files = len(files)
    file_counter = 0
    print 'Check filenames for block info...'
    for file in files:
        if file.find('block') == -1 and file.find('blok') == -1:
            print 'Filename(s) without block info found, please add block info to filename(s)'
            logging.info('Filename(s) without block info found, script terminated')
            exit()
    for file in files:
        file_counter += 1
        print 'Processing:', file, '(%s of %s)' %(file_counter, total_files)
        logging.info('Importing file \'%s\'', file)
        parsed = parse_spss_file(file)
        # parsed = None # for debugging: enable parsing only, create a CLI switch for this
        if not parsed:
            continue
        logging.debug('File parsed')
        transaction = connection.begin()
        success = insert_data(file, parsed, connection, tables)
        if success:
            # transaction.rollback() # for debugging, rollback after every succesfull entry
            transaction.commit()
            logging.info('SUCCES: file imported')
        else:
            transaction.rollback()
            logging.info('FAILURE: import error, file not imported')
    end_time = datetime.now()
    print 'Elapsed time:', str(end_time - start_time).split('.')[0]
    
if __name__ == '__main__':
    main()