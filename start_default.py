import json
from xmlrpc.client import Boolean
import followthemoney.model as model
from sqlalchemy import false, null
from alephclient.api import AlephAPI
import transform_ru_tenders_streaming as tr
import os


def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r"):
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
    # Print New Line on Complete
    if iteration == total: 
        print()

def file_parser(tender_file, i_success = 0, i_errors = 0):
    for line in tender_file:
            try:
                data = json.loads(line)
                a = tr.get_contract_award(data, collection_id = collection_id, api_client = api)
                i_success = i_success + 1
                printProgressBar(i_success, total_lines, prefix = 'Progress:', suffix = 'Complete', length = 50)
            except Exception:
                #print(line)
                print(Exception)
                i_errors = i_errors + 1
                continue




key = '9amGN0pkotrYG97LjfQPp1MUdciJghdXOUUYbYlojQI'
api = AlephAPI(host = 'https://aleph.k8s.gidno.org', api_key = key)

foreign_id = '52d1eeec73494d00aeb5ccfd07030778'
collection = api.load_collection_by_foreign_id(foreign_id)
collection_id = collection.get('id')

file_size = os.path.getsize('contracts_223fz_201505-20220315.jsonl')
total_lines = 0
if (file_size <= 500000000):
    with open ("contracts_223fz_201505-20220315.jsonl","r",  encoding='utf-8') as tender_file:
        print("small file")
        lines = tender_file.readlines()
        total_lines = len(lines)
        printProgressBar(0, total_lines, prefix = 'Progress:', suffix = 'Complete', length = 50)
        file_parser(lines)
        tender_file.close()
else:
    with open ("contracts_223fz_201505-20220315.jsonl","r",  encoding='utf-8') as tender_file:
        print("large file")
        count = 0
        for line in tender_file:
            count += 1
        total_lines = count
        printProgressBar(0, total_lines, prefix = 'Progress:', suffix = 'Complete', length = 50)
        file_parser(tender_file)
        tender_file.close()