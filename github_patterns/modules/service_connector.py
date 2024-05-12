import pandas as pd
import numpy as np
from datetime import datetime
from dateutil import relativedelta
import time
import aiohttp
import asyncio
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
        # self.token = getpass.getpass('Введите ваш GitHub токен: ')
        self.github_api_token = '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'
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
        transaction_composition example:
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
                                           is_new_repos)
        
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
        
        print(delay)

        
        clickhouse_data = self.__get_clickhouse_data(query, delay)
        clickhouse_data = self.__filter_clickhouse_data_columns(clickhouse_data, 
                                                        transaction_composition)
        
        api_columns = self.__get_github_api_columns(transaction_composition)
        github_api_data = await self.__get_github_api_data(
                                                    clickhouse_data["repo"], 
                                                    api_columns)
        
        repo_data = clickhouse_data.join(github_api_data.set_index("repo"), 
                                         on="repo")
    
        repo_data = self.__format_data_types(repo_data)
        return repo_data
    
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
                filtered_data[column].fillna(value=np.nan, inplace=True)
            
        return filtered_data

    def __get_query_by_params(self, 
                              start_date: str,
                              end_date: str,  
                              limit: int,
                              min_watch_event_count: int,
                              min_new_members_count: int,
                              is_new_repos: bool):
        
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
            
        # making sql query
        query = CLICKHOUSE_DATA_QUERY.format( 
                start_date=start_date, 
                end_date=end_date,
                limit=limit,
                new_repos_filter=new_repos_filter,
                min_watches_filter=min_watches_filter,
                min_members_filter=min_members_filter
        )
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
        driver = webdriver.Chrome()
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
    
        df = pd.DataFrame(table_data, columns=headers)
        
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