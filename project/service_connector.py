import pandas as pd
from datetime import datetime
import time
import aiohttp
import asyncio
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from exceptions.invalid_date_format_exception import InvalidDateFormatException
from queries import *


class ServiceConnector():
    CLICKHOUSE_REQUEST_URL = 'https://play.clickhouse.com/play?user=play'
    GITHUB_API_REQUEST_URL = 'https://api.github.com/repos/{repo_name}'
    
    def __init__(self):
        # self.token = getpass.getpass('Введите ваш GitHub токен: ')
        self.github_api_token = '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'
        self.headers = {'Authorization': f'token {self.github_api_token}'}
            
    async def get_data(self, start_date: str,
                 end_date: str | None = None,  
                 limit=1000,
                 repo_watch_event_count=10,
                 repo_members_count=3):
        """
        Асинхронный метод для получения данных из ClickHouse и GitHub API.

        Параметры:
        :param start_date: Начальная дата для запроса данных.
        :type start_date: str
        :param end_date: Конечная дата для запроса данных. Если не указана, то используется start_date.
        :type end_date: str, optional
        :param limit: Ограничение на количество строк в запросе к ClickHouse.
        :type limit: int, optional
        :param repo_watch_event_count: Количество событий "watch" в репозитории.
        :type repo_watch_event_count: int, optional
        :param repo_members_count: Количество участников в репозитории.
        :type repo_members_count: int, optional

        Возвращает:
        :return: DataFrame, содержащий данные из ClickHouse и GitHub API.
        :rtype: pandas.DataFrame
        """
        REQUEST_DELAY = 8
        
        if not start_date:
            return
        
        if not end_date:
            end_date = start_date
        
        if not (self.__validate_date(start_date) 
                and self.__validate_date(end_date)):
            return
        
        query = CLICKHOUSE_DATA_QUERY.format( 
                start_date=start_date, 
                end_date=end_date,
                limit=limit,
                watch_event_count=repo_watch_event_count)
        
        clickhouse_data = self.__get_clickhouse_data(query, REQUEST_DELAY)
        
        # TODO исправить в json
        clickhouse_data['pushes'] = clickhouse_data['pushes'].astype(int)
        clickhouse_data['pull_requests'] = clickhouse_data['pull_requests'].astype(int)
        clickhouse_data['issues'] = clickhouse_data['issues'].astype(int)
        clickhouse_data['watches'] = clickhouse_data['watches'].astype(int)
        clickhouse_data['forks'] = clickhouse_data['forks'].astype(int)
        clickhouse_data['avg_push_size'] = clickhouse_data['avg_push_size'].astype(float)
        clickhouse_data['merged_pull_requests_ratio'] = clickhouse_data['merged_pull_requests_ratio'].astype(float)
        clickhouse_data['closed_issues_ratio'] = clickhouse_data['closed_issues_ratio'].astype(float)
        
        github_api_columns = ['language', 'license_name', 'is_deleted_or_private']
        github_api_data = await self._get_github_api_data(clickhouse_data["repo"], github_api_columns)
        
        repo_data = clickhouse_data.join(github_api_data.set_index("repo"), on="repo")
        return repo_data
    
    async def _fetch(self, session, url):
        async with session.get(url, headers=self.headers) as response:
            return await response.json(), response.status
    
    async def _get_github_api_data(self, repo_names, data_columns):
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

    def __get_clickhouse_data(self, query, delay=30, quantiles=False):
        # Создаем новую вкладку для каждого запроса
        driver = webdriver.Chrome()
        driver.get(self.CLICKHOUSE_REQUEST_URL)
        query_input = driver.find_element(By.ID, 'query')
        query_input.send_keys(query)
        query_input.send_keys(Keys.CONTROL, Keys.RETURN)
        time.sleep(delay)
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        table = soup.find('table', {'id': 'data-table'})
        driver.quit()
        
        if not table:
            return
        
        headers = ["quantiles"]

        if not quantiles:
            header_elements = table.find_all('th')[1:]
            headers = [header.text for header in header_elements]
        
        table_data = []
        rows = table.find_all('tr')
        if not quantiles:
            rows = rows[1:]
        for row in rows:
            columns = row.find_all('td')
            if not quantiles:
                columns = columns[1:]
            data = [col.text.strip() for col in columns]
            table_data.append(data)
        
        # Создаем DataFrame
        df = pd.DataFrame(table_data, columns=headers)
        
        return df
        
        
    def __validate_date(self, date_string):
        try:
            datetime.strptime(date_string, '%Y-%m-%d')
            return True
        except ValueError:
            return False