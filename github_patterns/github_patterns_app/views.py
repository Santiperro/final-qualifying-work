from django.shortcuts import render
from django.http import JsonResponse
from django.db import transaction
from datetime import datetime
import pandas as pd
import json
import asyncio
from django.views.decorators.csrf import csrf_exempt
from github_patterns_app.models import SampleParams, RepositoryData
from modules.service_connector import ServiceConnector, GithubDataParams
from modules.github_data_converter import GithubDataConverter
from modules.pattern_miner import PatternMiner
from modules.exceptions import *


TRANSACTION_ITEMS_NAMES = list(pd.read_json("dtype_conf.json")["columnName"])
TRANSACTION_ITEMS_DECODE_NAMES = list(pd.read_json("dtype_conf.json")["decode"])
TRANSACTION_ITEMS_DATA_TYPE = list(pd.read_json("dtype_conf.json")["dtype"])


def find_patterns(request):
    samples = SampleParams.objects.all().order_by('-save_time')
    return render(request, 'find_patterns.html', {'samples': samples})


def request_data(request):
    items = get_items_name_dtype_dict()
    return render(request, 'request_data.html', {'items': items})


@csrf_exempt
def load_data_submit(request):
    if request.method == 'POST':
        request_params = json.loads(request.body)
        
        current_transaction_items_names = []
        items_decode_name_name_dict = get_items_decode_name_name_dict()
        
        for decode_name in request_params["items"]:
            if decode_name in items_decode_name_name_dict:
                columnName = items_decode_name_name_dict[decode_name]
                current_transaction_items_names.append(columnName)
                
        
        service_connector = ServiceConnector()
        try:
            data_params = GithubDataParams(
                transaction_composition=current_transaction_items_names,
                start_date=request_params["startDate"],
                end_date=request_params["endDate"],
                limit=int(request_params["numRepos"]),
                min_members_count=int(request_params["minParticipants"]),
                min_watch_event_count=int(request_params["minStars"]),
                is_new_repos=bool(request_params["isNewRepos"])
            )
            github_data = asyncio.run(service_connector.get_data_from_services(data_params))
        except (EmptyTableError, InsufficientRowsError) as e:
            return JsonResponse({'Error': str(e)}, status=400)
        
        attribute_info = {} 
        for decode_name in TRANSACTION_ITEMS_DECODE_NAMES:
            status_value = 'ON' if decode_name in request_params["items"] else 'OFF'
            division_value = str(request_params["items"].get(decode_name, 'NONE')).upper()
            
            attribute_info[items_decode_name_name_dict[decode_name]] = {
                'status': status_value,
                'division': division_value
            }
        
        sample_params = SampleParams(
            save_time=datetime.now(),
            start_date=request_params["startDate"],
            end_date=request_params["endDate"],
            repos_count=len(github_data),
            min_members_count=request_params["minParticipants"],
            min_watch_count=request_params["minStars"],
            is_new_repos=request_params["isNewRepos"],
            note=request_params["note"],
            pushes_duration_status=attribute_info.get('pushes', {}).get('status'),
            pushes_duration_division=attribute_info.get('pushes', {}).get('division'),
            avg_push_size_status=attribute_info.get('avg_push_size', {}).get('status'),
            avg_push_size_division=attribute_info.get('avg_push_size', {}).get('division'),
            pull_requests_status=attribute_info.get('pull_requests', {}).get('status'),
            pull_requests_division=attribute_info.get('pull_requests', {}).get('division'),
            merged_pull_requests_ratio_status=attribute_info.get('merged_pull_requests_ratio', {}).get('status'),
            merged_pull_requests_ratio_division=attribute_info.get('merged_pull_requests_ratio', {}).get('division'),
            issues_status=attribute_info.get('issues', {}).get('status'),
            issues_division=attribute_info.get('issues', {}).get('division'),
            closed_issues_ratio_status=attribute_info.get('closed_issues_ratio', {}).get('status'),
            closed_issues_ratio_division=attribute_info.get('closed_issues_ratio', {}).get('division'),
            watches_status=attribute_info.get('watches', {}).get('status'),
            watches_division=attribute_info.get('watches', {}).get('division'),
            forks_status=attribute_info.get('forks', {}).get('status'),
            forks_division=attribute_info.get('forks', {}).get('division'),
            new_members_status=attribute_info.get('new_members', {}).get('status'),
            new_members_division=attribute_info.get('new_members', {}).get('division'),
            language_status=attribute_info.get('language', {}).get('status'),
            license_name_status=attribute_info.get('license_name', {}).get('status'),
            is_deleted_or_private_status=attribute_info.get('is_deleted_or_private', {}).get('status'),
        )
        sample_params.save()
        
        with transaction.atomic():
            for index, row in github_data.iterrows():
                repo_data = RepositoryData(
                    data_params_id=sample_params,
                    repo_name=row.get('repo', None),
                    pushes=row.get('pushes', None),
                    avg_push_size=row.get('avg_push_size', None),
                    pull_requests=row.get('pull_requests', None),
                    merged_pull_requests_ratio=row.get('merged_pull_requests_ratio', None),
                    issues=row.get('issues', None),
                    closed_issues_ratio=row.get('closed_issues_ratio', None),
                    watches=row.get('watches', None),
                    forks=row.get('forks', None),
                    new_members=row.get('new_members', None),
                    language=row.get('language', None),
                    license_name=row.get('license_name', None),
                    is_deleted_or_private=row.get('is_deleted_or_private', None)
                )
                repo_data.save()
        
        return JsonResponse({'status': 'ok'}) 
    else:
        return JsonResponse({'error': 'Ошибка сервера'}, status=400)


@csrf_exempt
def delete_sample(request, id):
    if request.method == 'DELETE':
        try:
           with transaction.atomic():
                sample = SampleParams.objects.get(id=id)
                sample.delete()
                return JsonResponse({'success': True})
            
        except SampleParams.DoesNotExist:
            return JsonResponse({'error': 'Выборка не найдена', 'success': False})
  
                
@csrf_exempt
def find_patterns_submit(request):
    if request.method == 'POST':
        data = json.loads(request.body)
        ids = data['ids']

        try:
            repository_data, quantile_config = get_github_repository_data(ids)
        except ValueError as e:
            return JsonResponse({'Error': str(e)}, status=400)
        except EmptyTableError as e:
            return JsonResponse({'Error': str(e)}, status=400)
        
        github_data_converter = GithubDataConverter()
        transactions = github_data_converter.convert_data_to_transactions(repository_data, 
                                                                          quantile_config)
        quartiles = github_data_converter.quartile_table
        deciles = github_data_converter.decile_table
        
        pattern_miner = PatternMiner()
        try:
            github_patterns = pattern_miner.mine_patterns(transactions, 
                                                        min_supp=float(data['minsup']),
                                                        min_conf=float(data['minconf']),
                                                        min_lift=float(data['lift']),
                                                        min_left_elements=int(data['antecedent']),
                                                        min_right_elements=int(data['consequent']), 
                                                        max_left_elements=int(data['antecedent_max']),
                                                        max_right_elements=int(data['consequent_max']))
        except NoPatternsException as e:
            return JsonResponse({'Error': str(e)}, status=400)
        
        if github_patterns.empty:
            return JsonResponse({'Error': 'Шаблоны не найдены. Попробуйте изменить параметры'}, status=400)
        else:
            response_data = {
                'patterns': github_patterns.to_dict(orient='records'),
                'quartiles': quartiles.to_dict(orient='records') if quartiles is not None else [],
                'deciles': deciles.to_dict(orient='records') if deciles is not None else []
            }
            return JsonResponse(response_data, safe=False)
        
    
def get_github_repository_data(sample_ids):
    df = pd.DataFrame()
    attribute_dict = {}
    
    for sample_id in sample_ids:
        if not sample_id:
            continue

        sample = SampleParams.objects.get(id=int(sample_id))
        repository_data = RepositoryData.objects.filter(data_params_id=sample.id)
        df_sample = pd.DataFrame.from_records(repository_data.values())
        df = pd.concat([df, df_sample], ignore_index=True)
        
        current_attributes = get_attributes_dict(sample)
        
        if not attribute_dict:
            attribute_dict = current_attributes
        elif attribute_dict != current_attributes:
            raise ValueError("Параметры атрибутов данных не совпадают")
    
    if df.empty:
        raise EmptyTableError("Данные не получены. Перезагрузите страницу, и выберите заново")
    
    return df, attribute_dict


def get_attributes_dict(sample):
    attributes = {
        'pushes': sample.pushes_duration_division.lower() if sample.pushes_duration_status == 'ON' else "OFF",
        'avg_push_size': sample.avg_push_size_division.lower() if sample.avg_push_size_status == 'ON' else "OFF",
        'pull_requests': sample.pull_requests_division.lower() if sample.pull_requests_status == 'ON' else "OFF",
        'merged_pull_requests_ratio': sample.merged_pull_requests_ratio_division.lower() if sample.merged_pull_requests_ratio_status == 'ON' else "OFF",
        'issues': sample.issues_division.lower() if sample.issues_status == 'ON' else "OFF",
        'closed_issues_ratio': sample.closed_issues_ratio_division.lower() if sample.closed_issues_ratio_status == 'ON' else "OFF",
        'watches': sample.watches_division.lower() if sample.watches_status == 'ON' else "OFF",
        'forks': sample.forks_division.lower() if sample.forks_status == 'ON' else "OFF",
        'new_members': sample.new_members_division.lower() if sample.new_members_status == 'ON' else "OFF",
        'language': 'None' if sample.language_status == 'ON' else "OFF",
        'license_name': 'None' if sample.license_name_status == 'ON' else "OFF",
        'is_deleted_or_private': 'None' if sample.is_deleted_or_private_status == 'ON' else "OFF"
    }
    return {k: v for k, v in attributes.items() if v != "OFF"}
            
        
def get_items_name_dtype_dict():
    items_data_type_dict = {}
    for decode_name, dtype in zip(TRANSACTION_ITEMS_DECODE_NAMES, 
                           TRANSACTION_ITEMS_DATA_TYPE):
        items_data_type_dict[decode_name] = dtype
    return items_data_type_dict


def get_items_decode_name_name_dict():
    items_decode_name_name_dict = {}
    for decode_name, name  in zip(TRANSACTION_ITEMS_DECODE_NAMES, 
                                 TRANSACTION_ITEMS_NAMES):
        items_decode_name_name_dict[decode_name] = name
    return items_decode_name_name_dict
