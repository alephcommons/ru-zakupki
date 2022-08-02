import json
import string
import followthemoney.model as model
import re
from forex_python.converter import CurrencyRates
from datetime import datetime,timedelta
from sqlalchemy import null
from alephclient.api import AlephAPI

def api_entity_streaming(entity: model, collection_id: string, api_client: AlephAPI):
    entities = [entity.to_dict()]
    api_client.write_entities(collection_id, entities)

def if_in_range(array, index):
    if len(array) >= index + 1:
        return array[index]
    else:
        return ''

def currency_convertor(data):
    c = CurrencyRates()
    if 'contractCreateDate' in data and 'price' in data:
        try:
            contract_date = datetime.strptime(data['contractCreateDate'], '%Y-%m-%d').date()
            return round(c.convert('RUB', 'USD', data['price'] , contract_date),2)
        except Exception:
            print(Exception)
            contract_date = datetime.strptime(data['contractCreateDate'], '%Y-%m-%d').timedelta(days=2).date()
            return round(c.convert('RUB', 'USD', data['price'] , contract_date),2)
    else:
        return null

def in_object(child, obj):
    if child in obj:
        return True
    else: 
        return False

def get_if_exist(obj, obj_field_path: list):
    for i in obj_field_path:
        if i in obj:
            obj = obj[i]
            continue
        else:
            return ''
    return obj;

def check_if_exist(obj, obj_field_path: list):
    for i in obj_field_path:
        if i in obj:
            obj = obj[i]
            continue
        else:
            return False
    return True

def set_if_exist(ftm_model: model, property_name: string, obj, obj_field_path: list):
    for i in obj_field_path:
        if i in obj:
            obj = obj[i]
            continue
        else:
            return null
    try:
        ftm_model.add(property_name, obj)
    except Exception:
        print(Exception)

def get_address(address_string, collection_id, api_client: AlephAPI):
    address = model.make_entity(model.get('Address'))
    address.make_id(address_string)
    if re.match(address_string, 'Москва', re.IGNORECASE):
        address_string = address_string.split(',')
        address.add('region', 'Москва')
        address.add('city', if_in_range(address_string,1))
        address.add('street', if_in_range(address_string,2))
        address.add('street2', if_in_range(address_string,3))
    else:
        address_string = address_string.split(',')
        address.add('region', if_in_range(address_string,1))
        address.add('city', if_in_range(address_string,2))
        address.add('street', if_in_range(address_string,3))
        address.add('street2', if_in_range(address_string,4))
    address.add('postalCode', address_string[0])
    api_entity_streaming(address, collection_id, api_client)
    return address

def get_supplier(supplier, collection_id, api_client: AlephAPI):
    supplier_legal_entity = model.make_entity(model.get('Company'))
    supplier_legal_entity.make_id(
        get_if_exist(supplier, ['ogrn']), 
        get_if_exist(supplier, ['inn'])
    )
    set_if_exist(supplier_legal_entity, 'name', supplier, ['organizationName'])

    if 'ogrn' in supplier:
        reg_number = 'RU-OGRN-'+get_if_exist(supplier, ['ogrn'])
        set_if_exist(supplier_legal_entity, 'ogrnCode', supplier, ['ogrn'])
        supplier_legal_entity.add('registrationNumber',reg_number)

    set_if_exist(supplier_legal_entity, 'taxNumber', supplier, ['inn'])
    set_if_exist(supplier_legal_entity, 'innCode', supplier, ['inn'])
    set_if_exist(supplier_legal_entity, 'kppCode', supplier, ['kpp'])
    set_if_exist(supplier_legal_entity, 'ogrnCode', supplier, ['ogrn'])
    api_entity_streaming(supplier_legal_entity, collection_id, api_client)
    return supplier_legal_entity    


def get_authority(data, collection_id, api_client: AlephAPI):
    authorithy_le = model.make_entity(model.get('Company'))
    if 'placer' in data and 'mainInfo' in data['placer']:
        authorithy_le.make_id(
            get_if_exist(data, ['placer','mainInfo','ogrn']), 
            get_if_exist(data, ['placer','mainInfo','inn'])
        )
        authorithy_le.add('country', 'ru')
        authorithy_le.add('addressEntity', get_address(
                get_if_exist(data,['placer','mainInfo','legalAddress']),
                collection_id, 
                api_client
            )
        )

        if 'ogrn' in data['placer']['mainInfo']:
            reg_number = 'RU-OGRN-'+ data['placer']['mainInfo']['ogrn']
            authorithy_le.add('ogrnCode', data['placer']['mainInfo']['ogrn'])   
            authorithy_le.add('registrationNumber',reg_number)
        
        authorithy_le.add('name', get_if_exist(data['placer']['mainInfo'], ['fullName']))  
        authorithy_le.add('email', get_if_exist(data['placer']['mainInfo'], ['email']))  
        authorithy_le.add('phone', get_if_exist(data['placer']['mainInfo'], ['phone']))  
        authorithy_le.add('innCode', get_if_exist(data['placer']['mainInfo'], ['inn']))  
        authorithy_le.add('taxNumber', get_if_exist(data['placer']['mainInfo'], ['inn']))  
        authorithy_le.add('okpoCode', get_if_exist(data['placer']['mainInfo'], ['okpo']))  
        authorithy_le.add('ogrnCode', get_if_exist(data['placer']['mainInfo'], ['ogrn']))  
        authorithy_le.add('kppCode', get_if_exist(data['placer']['mainInfo'], ['kpp']))    
        # set_if_exist(authorithy_le, 'email', data['placer']['mainInfo'], ['email'])    
        # set_if_exist(authorithy_le, 'phone', data['placer']['mainInfo'], ['phone'])    
        # set_if_exist(authorithy_le, 'innCode', data['placer']['mainInfo'], ['inn'])    
        # set_if_exist(authorithy_le, 'taxNumber', data['placer']['mainInfo'], ['inn'])    
        # set_if_exist(authorithy_le, 'okpoCode', data['placer']['mainInfo'], ['okpo'])    
        # set_if_exist(authorithy_le, 'ogrnCode', data['placer']['mainInfo'], ['ogrn'])    
        # set_if_exist(authorithy_le, 'kppCode', data['placer']['mainInfo'], ['kpp']) 
        api_entity_streaming(authorithy_le, collection_id, api_client)
    return authorithy_le


def get_contract(data, collection_id, api_client: AlephAPI):
    contract = model.make_entity(model.get('Contract'))
    contract.make_id(
        get_if_exist(data, ['placer','mainInfo','ogrn']), 
        get_if_exist(data, ['number'])
    )
    authorithy = get_authority(data, collection_id, api_client)
    contract.add('authority', authorithy)
    contract.add('language', 'ru')
    contract.add('currency', 'RUB')
    contract.add('amountUsd',currency_convertor(data)) 
    set_if_exist(contract, 'title', data, ['lot','subject'])
    set_if_exist(contract, 'amount', data, ['price'])
    set_if_exist(contract, 'contractDate', data, ['contractCreateDate']) 
    api_entity_streaming(contract, collection_id, api_client)
    return contract

def get_contract_award(data, collection_id,  api_client: AlephAPI):
    contract_award = model.make_entity(model.get('ContractAward'))
    contract_award.make_id(
        get_if_exist(data, ['number'])
    )
    contract = get_contract(data, collection_id, api_client)
    contract_award.add('contract', contract)
    contract_award.add('currency', 'RUB')
    contract_award.add('amountUsd',currency_convertor(data))
    if 'suppliers' in data:
        for sup in data['suppliers']:
            supplier = get_supplier(sup,  collection_id, api_client)
            contract_award.add('supplier', supplier)
    set_if_exist(contract_award, 'lotNumber', data, ['number'])
    set_if_exist(contract_award, 'documentNumber', data, ['number'])
    set_if_exist(contract_award, 'amount', data, ['price'])
    set_if_exist(contract_award, 'startDate', data, ['contractCreateDate'])
    set_if_exist(contract_award, 'date', data, ['publishDate'])
    set_if_exist(contract_award, 'sourceUrl', data, ['printFormUrl'])
    set_if_exist(contract_award, 'description', data, ['lot','subject'])
    api_entity_streaming(contract_award, collection_id, api_client)
    return contract_award