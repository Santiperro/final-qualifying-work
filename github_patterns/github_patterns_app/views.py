from django.shortcuts import render
from django.http import JsonResponse
from django.db import transaction
from datetime import datetime
import pandas as pd
import json
import asyncio
from django.views.decorators.csrf import csrf_exempt
from github_patterns_app.models import SampleParams, RepositoryData, AttributeInfo
from modules.service_connector import ServiceConnector
from modules.github_data_converter import GithubDataConverter
from modules.pattern_miner import PatternMiner
from modules.exceptions import *


TRANSACTION_ITEMS_NAMES = list(pd.read_json("dtype_conf.json")["columnName"])
transaction_items_decode_names = list(pd.read_json("dtype_conf.json")["decode"])
transaction_items_data_type = list(pd.read_json("dtype_conf.json")["dtype"])


def find_patterns(request):
    samples = SampleParams.objects.all()

    return render(request, 'find_patterns.html', {'samples': samples})


def request_data(request):
    items = get_items_name_dtype_dict()
    return render(request, 'request_data.html', {'items': items})


@csrf_exempt
def load_data_submit(request):
    if request.method == 'POST':
        request_params = json.loads(request.body)
        print(request_params)
        
        current_transaction_items_names = []
        items_decode_name_name_dict = get_items_decode_name_name_dict()
        
        for decode_name in request_params["items"]:
            if decode_name in items_decode_name_name_dict:
                decode_name = items_decode_name_name_dict[decode_name]
                current_transaction_items_names.append(decode_name)
        
        service_connector = ServiceConnector()
        try:
            github_data = asyncio.run(service_connector.get_data_from_services(
                current_transaction_items_names,
                start_date=request_params["startDate"],
                end_date=request_params["endDate"],
                limit=int(request_params["numRepos"]),
                min_members_count=int(request_params["minParticipants"]),
                min_watch_event_count=int(request_params["minStars"]),
                is_new_repos=bool(request_params["isNewRepos"])))
        except (EmptyTableError, 
                InsufficientRowsError) as e:
            return JsonResponse({'Error': str(e)}, status=400)
        
        attribute_info_objects = {} 
        for decode_name in transaction_items_decode_names:
            if decode_name in request_params["items"]:
                attribute_info = AttributeInfo(
                    status='ON',
                    division=str(request_params["items"][decode_name]).upper()
                )
                item_name = items_decode_name_name_dict[decode_name]
                attribute_info_objects[item_name] = attribute_info
                attribute_info.save()
            else:
                attribute_info = AttributeInfo(
                    status='OFF',
                    division='NONE'
                )
                item_name = items_decode_name_name_dict[decode_name]
                attribute_info_objects[item_name] = attribute_info
                attribute_info.save()
        print(attribute_info_objects)
        
        sample_params = SampleParams(
            save_time=datetime.now(),
            start_date=request_params["startDate"],
            end_date=request_params["endDate"],
            repos_count=request_params["numRepos"],
            min_members_count=request_params["minParticipants"],
            min_watch_count=request_params["minStars"],
            is_new_repos=request_params["isNewRepos"],
            note=request_params["note"],
            pushes_duration_info=attribute_info_objects.get('pushes'),
            avg_push_size_info=attribute_info_objects.get('avg_push_size'),
            pull_requests_info=attribute_info_objects.get('pull_requests'),
            merged_pull_requests_ratio_info=attribute_info_objects.get('merged_pull_requests_ratio'),
            issues_info=attribute_info_objects.get('issues'),
            closed_issues_ratio_info=attribute_info_objects.get('closed_issues_ratio'),
            watches_info=attribute_info_objects.get('watches'),
            forks_info=attribute_info_objects.get('forks'),
            new_members_info=attribute_info_objects.get('new_members'),
            language_info=attribute_info_objects.get('language'),
            license_name_info=attribute_info_objects.get('license_name'),
            is_deleted_or_private_info=attribute_info_objects.get('is_deleted_or_private')
        )
        sample_params.save()
        
        with transaction.atomic():
            for index, row in github_data.iterrows():
                repo_data = RepositoryData(
                    data_params_id=sample_params,
                    repo_name=row['repo'],
                    pushes=row['pushes'],
                    avg_push_size=row['avg_push_size'],
                    pull_requests=row['pull_requests'],
                    merged_pull_requests_ratio=row['merged_pull_requests_ratio'],
                    issues=row['issues'],
                    closed_issues_ratio=row['closed_issues_ratio'],
                    watches=row['watches'],
                    forks=row['forks'],
                    new_members=row['new_members'],
                    language=row['language'],
                    license_name=row['license_name'],
                    is_deleted_or_private=row['is_deleted_or_private']
                )
                repo_data.save()
        
        return JsonResponse({'status': 'ok'})  # Отправляем ответ обратно на клиент
    else:
        return JsonResponse({'error': 'Invalid request method'}, status=400)

@csrf_exempt
def delete_sample(request, id):
    if request.method == 'DELETE':
        try:
           with transaction.atomic():
                sample = SampleParams.objects.get(id=id)
                if AttributeInfo.objects.filter(id=sample.pushes_duration_info.id).exists():
                    AttributeInfo.objects.filter(id=sample.pushes_duration_info.id).delete()
                    AttributeInfo.objects.filter(id=sample.avg_push_size_info.id).delete()
                    AttributeInfo.objects.filter(id=sample.pull_requests_info.id).delete()
                    AttributeInfo.objects.filter(id=sample.merged_pull_requests_ratio_info.id).delete()
                    AttributeInfo.objects.filter(id=sample.issues_info.id).delete()
                    AttributeInfo.objects.filter(id=sample.closed_issues_ratio_info.id).delete()
                    AttributeInfo.objects.filter(id=sample.watches_info.id).delete()
                    AttributeInfo.objects.filter(id=sample.forks_info.id).delete()
                    AttributeInfo.objects.filter(id=sample.new_members_info.id).delete()
                    AttributeInfo.objects.filter(id=sample.language_info.id).delete()
                    AttributeInfo.objects.filter(id=sample.license_name_info.id).delete()
                    AttributeInfo.objects.filter(id=sample.is_deleted_or_private_info.id).delete()

                sample.delete()
                
                return JsonResponse({'success': True})
        except SampleParams.DoesNotExist:
            return JsonResponse({'error': 'Sample not found', 'success': False})\
  
                
@csrf_exempt
def find_patterns_submit(request):
    if request.method == 'POST':
        data = json.loads(request.body)
        ids = data['ids']

        try:
            repository_data, quantile_config = get_github_repository_data(ids)
        except ValueError as e:
            return JsonResponse({'Error': str(e)}, status=400)
        
        github_data_converter = GithubDataConverter()
        transactions = github_data_converter.convert_data_to_transactions(repository_data, 
                                                                          quantile_config)
        pattern_miner = PatternMiner()
        github_patterns = pattern_miner.mine_patterns(transactions, 
                                                    min_supp=float(data['minsup']),
                                                    min_conf=float(data['minconf']),
                                                    min_lift=float(data['lift']),
                                                    max_left_elements=int(data['antecedent']),
                                                    max_right_elements=int(data['consequent']))
        if github_patterns.empty:
            return JsonResponse({'Error': 'Шаблоны не найдены. Попробуйте изменить параметры'}, status=400)
        else:
            return JsonResponse(github_patterns.to_dict(orient='records'), 
                                    safe=False)
        
    
def get_github_repository_data(sample_ids):
    attribute_dict = {}
    df = pd.DataFrame()
    for id in sample_ids:
        if id == '':
            continue
        sample = SampleParams.objects.get(id=int(id))
        repository_data = RepositoryData.objects.filter(data_params_id=sample.id)

        df_sample = pd.DataFrame.from_records(repository_data.values())

        df = pd.concat([df, df_sample])
    
        if not attribute_dict:
            attribute_dict = {
                'pushes': sample.pushes_duration_info.division.lower() if sample.pushes_duration_info.status == 'ON' else 'None',
                'avg_push_size': sample.avg_push_size_info.division.lower() if sample.avg_push_size_info.status == 'ON' else 'None',
                'pull_requests': sample.pull_requests_info.division.lower() if sample.pull_requests_info.status == 'ON' else 'None',
                'merged_pull_requests_ratio': sample.merged_pull_requests_ratio_info.division.lower() if sample.merged_pull_requests_ratio_info.status == 'ON' else 'None',
                'issues': sample.issues_info.division.lower() if sample.issues_info.status == 'ON' else 'None',
                'closed_issues_ratio': sample.closed_issues_ratio_info.division.lower() if sample.closed_issues_ratio_info.status == 'ON' else 'None',
                'watches': sample.watches_info.division.lower() if sample.watches_info.status == 'ON' else 'None',
                'forks': sample.forks_info.division.lower() if sample.forks_info.status == 'ON' else 'None',
                'new_members': sample.new_members_info.division.lower() if sample.new_members_info.status == 'ON' else 'None',
                'language': sample.language_info.division.lower() if sample.language_info.status == 'ON' else 'None',
                'license_name': sample.license_name_info.division.lower() if sample.license_name_info.status == 'ON' else 'None',
                'is_deleted_or_private': sample.is_deleted_or_private_info.division.lower() if sample.is_deleted_or_private_info.status == 'ON' else 'None'
            }
        else:
            # Проверяем, соответствует ли словарь атрибутов текущей выборке
            current_attribute_dict = {
                'pushes': sample.pushes_duration_info.division.lower() if sample.pushes_duration_info.status == 'ON' else 'None',
                'avg_push_size': sample.avg_push_size_info.division.lower() if sample.avg_push_size_info.status == 'ON' else 'None',
                'pull_requests': sample.pull_requests_info.division.lower() if sample.pull_requests_info.status == 'ON' else 'None',
                'merged_pull_requests_ratio': sample.merged_pull_requests_ratio_info.division.lower() if sample.merged_pull_requests_ratio_info.status == 'ON' else 'None',
                'issues': sample.issues_info.division.lower() if sample.issues_info.status == 'ON' else 'None',
                'closed_issues_ratio': sample.closed_issues_ratio_info.division.lower() if sample.closed_issues_ratio_info.status == 'ON' else 'None',
                'watches': sample.watches_info.division.lower() if sample.watches_info.status == 'ON' else 'None',
                'forks': sample.forks_info.division.lower() if sample.forks_info.status == 'ON' else 'None',
                'new_members': sample.new_members_info.division.lower() if sample.new_members_info.status == 'ON' else 'None',
                'language': sample.language_info.division.lower() if sample.language_info.status == 'ON' else 'None',
                'license_name': sample.license_name_info.division.lower() if sample.license_name_info.status == 'ON' else 'None',
                'is_deleted_or_private': sample.is_deleted_or_private_info.division.lower() if sample.is_deleted_or_private_info.status == 'ON' else 'None'
            }
            if attribute_dict != current_attribute_dict:
                raise ValueError("Атрибуты выборок не совпадают")
    
    attribute_dict = {key: value for key, value in attribute_dict.items() if value != 'None'}
    
    if df.empty:
        raise EmptyTableError("Данные не получены. Перезагрузите страницу, и выберите заново")
        
    return df, attribute_dict
            
        
        
def get_items_name_dtype_dict():
    items_data_type_dict = {}
    for decode_name, dtype in zip(transaction_items_decode_names, 
                           transaction_items_data_type):
        items_data_type_dict[decode_name] = dtype
    return items_data_type_dict


def get_items_decode_name_name_dict():
    items_decode_name_name_dict = {}
    for decode_name, name  in zip(transaction_items_decode_names, 
                                 TRANSACTION_ITEMS_NAMES):
        items_decode_name_name_dict[decode_name] = name
    return items_decode_name_name_dict

# def get_items_name_decode_name_dict():
#     items_name_decode_name_dict = {}
#     for name, decode_name  in zip(TRANSACTION_ITEMS_NAMES, 
#                                  transaction_items_decode_names):
#         items_name_decode_name_dict[name] = decode_name
#     return items_name_decode_name_dict

