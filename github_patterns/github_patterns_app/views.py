from django.shortcuts import render
from django.db import transaction
from github_patterns_app.models import GithubData, GithubDataParams
import pandas as pd


def find_patterns(request):

    return render(request, 'find_patterns.html')


def request_data(request):
    
    return render(request, 'request_data.html')


def save_dataframe_to_model(df: pd.DataFrame, data_params: dict):
    with transaction.atomic():
        github_data_params = GithubDataParams()
        github_data_params.repos_count = data_params['repos_count']
        github_data_params.is_new_repos = data_params['is_new_repos']
        github_data_params.min_members_count = data_params['min_members_count']
        github_data_params.min_watch_count = data_params['min_watch_count']
        github_data_params.save()
        
        github_data = GithubData()
        github_data.data_params_id = github_data_params
        github_data.repo_name = df['repo_name']
        github_data.pushes = df['pushes']
        github_data.avg_push_size = df['avg_push_size']
        github_data.pull_requests = df['pull_requests']
        github_data.merged_pull_requests_ratio = df['merged_pull_requests_ratio']
        github_data.issues = df['issues']
        github_data.closed_issues_ratio = df['closed_issues_ratio']
        github_data.watches = df['watches']
        github_data.forks = df['forks']
        github_data.language = df['language']
        github_data.license_name = df['license_name']
        github_data.is_deleted_or_private = df['is_deleted_or_private']
        github_data.save()
        

