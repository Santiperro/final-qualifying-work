import pandas as pd
import numpy as np
from datetime import datetime
from dateutil import relativedelta
import time
import aiohttp
import asyncio
import environ
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from .queries import *
from .exceptions import *


class ServiceConnector():
    CLICKHOUSE_REQUEST_URL = 'https://play.clickhouse.com/play?user=play'
    GITHUB_API_REQUEST_URL = 'https://api.github.com/repos/{repo_name}'
    REQUEST_DELAY = 10
    
    DEFAULT_LIMIT = 1000
    DEFAULT_WATCH_EVENT_COUNT = 10
    DEFAULT_MIN_MEMBERS_COUNT = 3
    
    TYPES_DICT = {"int": int, 
                  "float": float, 
                  "str:": str, 
                  "bool": bool}
    
    def __init__(self):
        env = environ.Env()
        environ.Env.read_env()
        self.github_api_token =  env('GITHUB_KEY')
        
        self.headers = {'Authorization': f'token {self.github_api_token}'}
        self.data_configuration = pd.read_json(r"dtype_conf.json")
            
    async def get_data_from_services(self, 
                       transaction_composition: list[str],
                       start_date: str,
                       end_date: str | None = None,  
                       limit: int = DEFAULT_LIMIT,
                       min_watch_event_count: int = DEFAULT_WATCH_EVENT_COUNT,
                       min_members_count: int = DEFAULT_MIN_MEMBERS_COUNT,
                       is_new_repos: bool = False) -> pd.DataFrame:
        """
        Получение данных из сервисов.
        
        transaction_composition: список строк, определяющих состав транзакции.
        start_date: начальная дата в формате 'YYYY-MM-DD'.
        end_date: конечная дата в формате 'YYYY-MM-DD'. Если None, то используется текущая дата.
        limit: максимальное количество записей для получения. По умолчанию 1000.
        min_watch_event_count: минимальное количество событий просмотра. По умолчанию 10.
        min_members_count: минимальное количество участников. По умолчанию 3.
        is_new_repos: флаг, указывающий на то, являются ли репозитории новыми. По умолчанию False.
        
        Возвращает DataFrame с данными репозиториев.
        
        Пример: transaction_composition:
        ['pushes',
        'avg_push_size',
        'pull_requests',
        'merged_pull_requests_ratio',
        'issues',
        'closed_issues_ratio',
        'watches',
        'forks',
        'new_members',
        'language',
        'license_name',
        'is_deleted_or_private']
        """
        
        self.__validate_date_range(start_date, end_date)
        
        min_new_members_count = min_members_count - 1
        
        query = self.__get_query_by_params(start_date,
                                           end_date, 
                                           limit,
                                           min_watch_event_count,
                                           min_new_members_count,
                                           is_new_repos,
                                           transaction_composition)
        
        delay = self.__get_delay(start_date, 
                                 end_date,
                                 min_watch_event_count,
                                 min_new_members_count,
                                 is_new_repos)

        clickhouse_data = self.__get_clickhouse_data(query, delay)
        filtered_data = self.__filter_clickhouse_data_columns(clickhouse_data, 
                                                        transaction_composition)
        
        api_columns = self.__get_github_api_columns(transaction_composition)
        
        github_api_data = await self.__get_github_api_data(
                                                    filtered_data["repo"], 
                                                    api_columns)
        
        repo_data = filtered_data.join(github_api_data.set_index("repo"), 
                                         on="repo")
    
        repo_data = self.__format_data_types(repo_data)
        return repo_data
    
    def __get_delay(self, 
                    start_date,
                    end_date,
                    min_watch_event_count,
                    min_members_count,
                    is_new_repos):
        
        delay = self.REQUEST_DELAY
        
        if min_watch_event_count > 0:
            delay += 10
        if min_members_count > 1:
            delay += 10
        if is_new_repos:
            delay += 10
            
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
        
        difference = relativedelta.relativedelta(end_date, start_date)
        months_difference = difference.years * 12 + difference.months
        delay += 2 * months_difference
        return delay
    
    def __get_github_api_columns(self, transaction_composition: list):
        source_condition = self.data_configuration["source"] == "githubApi"
        api_columns = list(self.data_configuration[source_condition]["columnName"])
        api_columns = [column for column in api_columns if column in transaction_composition]
        api_columns.append("repo")
        return api_columns
    
    def __filter_clickhouse_data_columns(self, 
                              clickhouse_data: pd.DataFrame, 
                              transaction_composition: list) -> pd.DataFrame:
        filtered_data = clickhouse_data.copy()
        
        for column in filtered_data.columns:
            if not (column in transaction_composition or column == 'repo'):
                filtered_data.drop(columns=[column], inplace=True)
            
        return filtered_data

    def __get_query_by_params(self, 
                            start_date: str,
                            end_date: str,  
                            limit: int,
                            min_watch_event_count: int,
                            min_new_members_count: int,
                            is_new_repos: bool,
                            selected_attributes: list):

        attribute_queries = {
            'pushes': "pushEvents.pushEventCount as pushes",
            'avg_push_size': "pushEvents.avgPushSize as avg_push_size",
            'pull_requests': "pullRequestEvents.pullRequestEventCount as pull_requests",
            'merged_pull_requests_ratio': "pullRequestEvents.mergedPullRequestEventRatio as merged_pull_requests_ratio",
            'issues': "issuesEvents.issuesEventCount as issues",
            'closed_issues_ratio': "issuesEvents.closedIssuesRatio as closed_issues_ratio",
            'watches': "watchEvents.watchEventCount as watches",
            'forks': "forkEvents.forkEventCount as forks",
            'new_members': "newMemberEvents.memberEventCount as new_members"
        }

        # Include 'repo' as a default attribute
        select_clause = ["outerQuery.repo_name as repo"]
        join_clauses = []

        if 'pushes' in selected_attributes or 'avg_push_size' in selected_attributes:
            select_clause.extend([
                attribute_queries['pushes'],
                attribute_queries['avg_push_size']
            ])
            join_clauses.append(PUSH_EVENTS_QUERY.format(start_date=start_date, end_date=end_date))

        if 'pull_requests' in selected_attributes or 'merged_pull_requests_ratio' in selected_attributes:
            select_clause.extend([
                attribute_queries['pull_requests'],
                attribute_queries['merged_pull_requests_ratio']
            ])
            join_clauses.append(PULL_REQUEST_EVENTS_QUERY.format(start_date=start_date, end_date=end_date))

        if 'issues' in selected_attributes or 'closed_issues_ratio' in selected_attributes:
            select_clause.extend([
                attribute_queries['issues'],
                attribute_queries['closed_issues_ratio']
            ])
            join_clauses.append(ISSUES_EVENTS_QUERY.format(start_date=start_date, end_date=end_date))

        if 'watches' in selected_attributes:
            select_clause.append(attribute_queries['watches'])
            join_clauses.append(WATCH_EVENTS_QUERY.format(start_date=start_date, end_date=end_date))

        if 'forks' in selected_attributes:
            select_clause.append(attribute_queries['forks'])
            join_clauses.append(FORK_EVENTS_QUERY.format(start_date=start_date, end_date=end_date))

        if 'new_members' in selected_attributes:
            select_clause.append(attribute_queries['new_members'])
            join_clauses.append(NEW_MEMBER_EVENTS_QUERY.format(start_date=start_date, end_date=end_date))

        select_clause_str = ", ".join(select_clause)
        join_clauses_str = " ".join(join_clauses)
        
        new_repos_filter = NEW_REPOS_QUERY_FILTER if is_new_repos else ''
        
        min_watches_filter = (MIN_WATCHES_QUERY_FILTER.format(
            start_date=start_date, 
            end_date=end_date,
            watch_event_count=min_watch_event_count
        ) if min_watch_event_count > 0 else '')
            
        min_members_filter = (MIN_MEMBERS_QUERY_FILTER.format(
            start_date=start_date, 
            end_date=end_date,
            new_members_count=min_new_members_count
        ) if min_new_members_count > 0 else '')
        
        query = CLICKHOUSE_DATA_QUERY.format(select_clause_str=select_clause_str,
                                            start_date=start_date,
                                            end_date=end_date,
                                            new_repos_filter=new_repos_filter,
                                            min_watches_filter=min_watches_filter,
                                            min_members_filter=min_members_filter,
                                            join_clauses_str=join_clauses_str,
                                            limit=limit)
    
        return query
    
    def __format_data_types(self, data):
        formatted_copy = data.copy()
        data_conf = self.data_configuration
        
        for col in formatted_copy.columns:
            if not col in list(data_conf["columnName"]):
                continue
            
            col_type = data_conf[data_conf['columnName'] == col]['dtype'].values[0]
            if col_type in self.TYPES_DICT:
                formatted_copy[col] = formatted_copy[col].astype(self.TYPES_DICT[col_type])
        
            formatted_copy[col] = formatted_copy[col].astype(col_type)
            
            if col_type != "float":
                continue

            col_decimal_places = data_conf[data_conf["columnName"] == col]["decimalPlaces"].values[0]
            formatted_copy[col] = formatted_copy[col].round(int(col_decimal_places))
            
        return formatted_copy
    
    async def _fetch(self, session, url):
        async with session.get(url, headers=self.headers) as response:
            return await response.json(), response.status
    
    async def __get_github_api_data(self, repo_names, data_columns):
        tasks = []
        
        data = {'repo': []}
        for column in data_columns:
            data[column] = []
    
        async with aiohttp.ClientSession() as session:
            for repo_name in repo_names:
                url = self.GITHUB_API_REQUEST_URL.format(repo_name=repo_name)
                tasks.append(self._fetch(session, url))

            responses = await asyncio.gather(*tasks)
            
            for repo_name, (response, status) in zip(repo_names, responses):
                data['repo'].append(repo_name)
                
                if 'license_name' in data_columns:
                    data['license_name'].append(response.get('license', {}).get('name', None) if response.get('license') else None)
                    
                if 'is_deleted_or_private' in data_columns:
                    data['is_deleted_or_private'].append(True if status == 404 else False)
                
                if 'language' in data_columns:
                    data['language'].append(response.get('language', None))

        df = pd.DataFrame(data)
        return df

    def __get_clickhouse_data(self, query, delay=30):
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--window-size=1920x1080")
        options.add_argument("--disable-gpu")

        driver = webdriver.Chrome(options=options)
        driver.get(self.CLICKHOUSE_REQUEST_URL)

        query_input = driver.find_element(By.ID, 'query')
        query_input.send_keys(query)
        query_input.send_keys(Keys.CONTROL, Keys.RETURN)
        
        time.sleep(delay)
        try:
            WebDriverWait(driver, 0).until(EC.presence_of_element_located((By.ID, 'data-table')))
        except TimeoutException:
            driver.quit()
            raise EmptyTableError("Слишком сложный запрос, данные не получены. Попробуйте изменить параметры запроса")

        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        table = soup.find('table', {'id': 'data-table'})
        driver.quit()
        
        if not table:
            raise EmptyTableError("Данные не получены. "
                + "Попробуйте изменить параметры запроса")
   
        header_elements = table.find_all('th')[1:]
        headers = [header.text for header in header_elements]
        
        table_data = []
        rows = table.find_all('tr')
        
        for row in rows[1:]:
            columns = row.find_all('td')[1:]
            data = [col.text.strip() for col in columns]
            table_data.append(data)
            
        if not table_data:
            raise EmptyTableError("Данные не получены. "
                + "Попробуйте изменить параметры запроса")

        try:
            df = pd.DataFrame(table_data, columns=headers)
        except Exception:
            raise EmptyTableError("Данные не получены. "
                + "Попробуйте изменить параметры запроса")
        
        if df.shape[0] < 200:
            raise InsufficientRowsError("В таблице меньше 200 записей" 
                                        + "Попробуйте изменить параметры запроса")
        
        return df
          
    def __validate_date_range(self, start_date_string: str, end_date_string: str) -> bool:
        try:
            start_date = datetime.strptime(start_date_string, '%Y-%m-%d')
            end_date = datetime.strptime(end_date_string, '%Y-%m-%d')
            
            if start_date > end_date:
                return False
            return True
        except ValueError:
            return False