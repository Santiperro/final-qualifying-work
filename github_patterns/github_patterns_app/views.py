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


transaction_items_names = list(pd.read_json("dtype_conf.json")["columnName"])
transaction_items_decode_names = list(pd.read_json("dtype_conf.json")["decode"])
transaction_items_data_type = list(pd.read_json("dtype_conf.json")["dtype"])


def find_patterns(request):
    # Заглушка для данных
    samples = SampleParams.objects.none()

    # Добавляем пример данных вручную
    samples = list(samples)
    for i in range(10):
        samples.append(SampleParams(id=i, 
                                    save_time="2024-05-05 23:58:14", 
                                    time_interval="2024-05-05 - 2024-05-05", 
                                    repos_count=10, min_members_count=5, 
                                    min_watch_count=100, 
                                    is_new_repos=True, 
                                    note="Выборка " + str(i)))

    return render(request, 'find_patterns.html', {'samples': samples})


def request_data(request):
    items = get_items_name_dtype_dict()
    return render(request, 'request_data.html', {'items': items})


@csrf_exempt
def load_data_submit(request):
    if request.method == 'POST':
        request_params = json.loads(request.body)
        print(request_params)
        
        transaction_items_names = []
        items_decode_name_name_dict = get_items_decode_name_name_dict()
        
        for decode_name in request_params["items"]:
            if decode_name in items_decode_name_name_dict:
                item_name = items_decode_name_name_dict[decode_name]
                transaction_items_names.append(item_name)
        
        service_connector = ServiceConnector()
        github_data = asyncio.run(service_connector.get_data_from_services(
            transaction_items_names,
            start_date=request_params["startDate"],
            end_date=request_params["endDate"],
            limit=int(request_params["numRepos"]),
            min_new_members_count=int(request_params["minParticipants"]),
            min_watch_event_count=int(request_params["minStars"]),
            is_new_repos=bool(request_params["isNewRepos"])))
        
        attribute_info_objects = {} 
        for decode_name in request_params["items"]:
            if decode_name in items_decode_name_name_dict:
                attribute_info = AttributeInfo(
                    status='ON',
                    division=request_params["items"][item_name].upper()
                )
                attribute_info.save()
                attribute_info_objects[item_name] = attribute_info
        
        # Обработка остальных элементов
        for decode_name in request_params["items"]:
            if decode_name not in items_decode_name_name_dict:
                attribute_info = AttributeInfo(
                    status='OFF',
                    division='NONE'
                )
                attribute_info.save()
                # Сохраните объект AttributeInfo в словаре
                attribute_info_objects[decode_name] = attribute_info
        
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
            new_members_info=attribute_info_objects.get('new_members')
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


def save_dataframe_to_model(df: pd.DataFrame, data_params: dict):
    with transaction.atomic():
        github_data_params = SampleParams()
        github_data_params.repos_count = data_params['repos_count']
        github_data_params.is_new_repos = data_params['is_new_repos']
        github_data_params.min_members_count = data_params['min_members_count']
        github_data_params.min_watch_count = data_params['min_watch_count']
        github_data_params.save()
        
        
def get_items_name_dtype_dict():
    items_data_type_dict = {}
    for decode_name, dtype in zip(transaction_items_decode_names, 
                           transaction_items_data_type):
        items_data_type_dict[decode_name] = dtype
    return items_data_type_dict


def get_items_decode_name_name_dict():
    items_name_decode_name_dict = {}
    for decode_name, name  in zip(transaction_items_decode_names, 
                                 transaction_items_names):
        items_name_decode_name_dict[decode_name] = name
    return items_name_decode_name_dict

