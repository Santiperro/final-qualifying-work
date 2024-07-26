import pandas as pd
from datetime import datetime
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


class GithubDataParams:
    """
    Represents parameters for fetching data related to GitHub repositories.

    Args:
        transaction_composition (list[str]): List of transaction attributes to include in the data.
        start_date (str): Start date for the data retrieval (format: 'YYYY-MM-DD').
        end_date (str, optional): End date for the data retrieval (format: 'YYYY-MM-DD').
        limit (int, optional): Maximum number of records to retrieve (default: 1000).
        min_watch_event_count (int, optional): Minimum watch event count for repositories (default: 10).
        min_members_count (int, optional): Minimum members count for repositories (default: 2).
        is_new_repos (bool, optional): Whether to include only new repositories (default: False).
    """
    DEFAULT_LIMIT = 1000
    DEFAULT_WATCH_EVENT_COUNT = 10
    DEFAULT_MIN_MEMBERS_COUNT = 3
    DEFAULT_IS_NEW_REPOS = False

    
    def __init__(self, transaction_composition: list[str], 
                 start_date: str, 
                 end_date: str | None = None,  
                 limit: int = DEFAULT_LIMIT, 
                 min_watch_event_count: int = DEFAULT_WATCH_EVENT_COUNT,
                 min_members_count: int = DEFAULT_MIN_MEMBERS_COUNT, 
                 is_new_repos: bool = DEFAULT_IS_NEW_REPOS):
        self.transaction_composition = transaction_composition
        self.start_date = start_date
        self.end_date = end_date
        self.limit = limit
        self.min_watch_event_count = min_watch_event_count
        self.min_members_count = min_members_count - 1
        self.is_new_repos = is_new_repos


class ServiceConnector():
    """
    Connects to services (ClickHouse and GitHub API) to retrieve data for repositories.

    Attributes:
        CLICKHOUSE_REQUEST_URL (str): ClickHouse query URL.
        GITHUB_API_REQUEST_URL (str): GitHub API base URL.

    Methods:
        get_data_from_services(data_params: GithubDataParams) -> pd.DataFrame:
            Retrieves data from ClickHouse and GitHub API based on specified parameters.
    """
    CLICKHOUSE_REQUEST_URL = 'https://play.clickhouse.com/play?user=play'
    GITHUB_API_REQUEST_URL = 'https://api.github.com/repos/{repo_name}'
    
    def __init__(self):
        env = environ.Env()
        environ.Env.read_env()
        github_api_token = env('GITHUB_KEY', default='')
        self.headers = {'Authorization': f'token {github_api_token}'}
        self.data_configuration = pd.read_json(r"dtype_conf.json")
            
    async def get_data_from_services(self, 
                                     data_params: GithubDataParams) -> pd.DataFrame:
        """
        Retrieves data from services based on the provided parameters.

        Args:
            data_params (GithubDataParams): Parameters for data retrieval.

        Returns:
            pd.DataFrame: Formatted data.

        Raises:
            EmptyTableError: If the query results in an empty table.
            InsufficientRowsError: If fewer than 200 records are retrieved.
            ValueError: If data params are not correct.
        """
        self.__validate_date_range(data_params.start_date, data_params.end_date)
        
        query = self.__get_query_by_params(data_params)

        clickhouse_data = self.__get_clickhouse_data(query)
        
        api_columns = self.__get_github_api_columns(data_params.transaction_composition)
        
        github_api_data = await self.__get_github_api_data(
                                                    clickhouse_data["repo"], 
                                                    api_columns)
        
        repo_data = clickhouse_data.join(github_api_data.set_index("repo"), 
                                        on="repo")

        formatted_data = self.__format_data_types(repo_data)
        return formatted_data
    
    def __get_github_api_columns(self, transaction_composition: list) -> list:
        source_condition = self.data_configuration["source"] == "githubApi"
        api_columns = list(self.data_configuration[source_condition]["columnName"])
        api_columns = [column for column in api_columns if column in transaction_composition]
        api_columns.append("repo")
        return api_columns

    def __get_query_by_params(self, query_params: GithubDataParams) -> str:
        
        def build_select_clause(query_params):
            select_clause = ["outerQuery.repo_name as repo"]
            join_clauses = []

            for attribute in query_params.transaction_composition:
                config_row = self.data_configuration.loc[self.data_configuration['columnName'] == attribute]                
                join_query_name = config_row.iloc[0]['joinQuery']
                if pd.isnull(join_query_name):
                    continue
                
                join_query = JOIN_CLAUSES[join_query_name]
                
                if not join_query in join_clauses:
                    join_clauses.append(join_query)
                    
                if not attribute in SELECT_CLAUSES:
                    continue
                
                select_clause.append(SELECT_CLAUSES[attribute])

            select_clause_str = ", ".join(select_clause)
            join_clauses_str = " ".join(join_clauses).format(
                start_date=query_params.start_date, 
                end_date=query_params.end_date
            )
            return select_clause_str, join_clauses_str

        def build_filters(query_params):
            new_repos_filter = NEW_REPOS_QUERY_FILTER if query_params.is_new_repos else ''
            
            min_watches_filter = (MIN_WATCHES_QUERY_FILTER.format(
                start_date=query_params.start_date, 
                end_date=query_params.end_date,
                watch_event_count=query_params.min_watch_event_count
            ) if query_params.min_watch_event_count > 0 else '')
                
            min_members_filter = (MIN_MEMBERS_QUERY_FILTER.format(
                start_date=query_params.start_date, 
                end_date=query_params.end_date,
                new_members_count=query_params.min_members_count - 1
            ) if query_params.min_members_count > 0 else '')
            
            return new_repos_filter, min_watches_filter, min_members_filter
        
        
        select_clause_str, join_clauses_str = build_select_clause(query_params)
        new_repos_filter, min_watches_filter, min_members_filter = build_filters(query_params)

        query = CLICKHOUSE_DATA_QUERY.format(
            select_clause_str=select_clause_str,
            start_date=query_params.start_date,
            end_date=query_params.end_date,
            new_repos_filter=new_repos_filter,
            min_watches_filter=min_watches_filter,
            min_members_filter=min_members_filter,
            join_clauses_str=join_clauses_str,
            limit=query_params.limit
        )
        return query
    
    def __format_data_types(self, data: pd.DataFrame) -> pd.DataFrame:
        formatted_data = data.copy()
        data_conf = self.data_configuration
        
        for col in formatted_data.columns:
            if not col in list(data_conf["columnName"]):
                continue
            
            col_type = data_conf[data_conf['columnName'] == col]['dtype'].values[0]
            if col_type in ["int", "float", "str", "bool"]:
                formatted_data[col] = formatted_data[col].astype(eval(col_type))
        
            formatted_data[col] = formatted_data[col].astype(col_type)
            
            if col_type != "float":
                continue

            col_decimal_places = data_conf[data_conf["columnName"] == col]["decimalPlaces"].values[0]
            formatted_data[col] = formatted_data[col].round(int(col_decimal_places))
            
        return formatted_data
        
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

        return pd.DataFrame(data)

    def __get_clickhouse_data(self, query):
        class AnyEc:
            def __init__(self, *args):
                self.ecs = args

            def __call__(self, driver):
                for fn in self.ecs:
                    try:
                        if fn(driver): return True
                    except:
                        pass
                    
                    
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--window-size=1920x1080")
        options.add_argument("--disable-gpu")
        driver = webdriver.Chrome(options=options)
        driver.get(self.CLICKHOUSE_REQUEST_URL)

        query_input = driver.find_element(By.ID, 'query')
        query_input.send_keys(query)
        query_input.send_keys(Keys.CONTROL, Keys.RETURN)
        
        try:
            WebDriverWait(driver, 100).until(AnyEc(
                EC.presence_of_element_located((By.XPATH, 
                                                '//*[@id="data-table"]/tbody/tr')),
                lambda d: "empty result" in d.find_element(By.ID, 'data-table').get_attribute('innerHTML'),
                lambda d: d.find_element(By.ID, 'error').is_displayed()
            ))
        except TimeoutException:
            driver.quit()
            raise EmptyTableError("Слишком сложный запрос, данные не получены.\
                Попробуйте изменить параметры запроса")
        
        if "empty result" in driver.find_element(By.ID, 'data-table').get_attribute('innerHTML'):
            driver.quit()
            raise EmptyTableError("Данные не получены. Попробуйте изменить \
                параметры запроса, чтобы в выборку попало больше репозиториев")
        
        error_element = driver.find_element(By.ID, 'error')
        if error_element.is_displayed():
            error_text = error_element.text
            driver.quit()
            raise EmptyTableError(f"Ошибка сервиса ClickHouse: \"{error_text}\"\
                Попробуйте изменить параметры запроса")

        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        table = soup.find('table', {'id': 'data-table'})
        driver.quit()
   
        header_elements = table.find_all('th')[1:]
        headers = [header.text for header in header_elements]
        table_data = []
        
        rows = table.find_all('tr')
        for row in rows[1:]:
            columns = row.find_all('td')[1:]
            data = [col.text.strip() for col in columns]
            table_data.append(data)
            
        df = pd.DataFrame(table_data, columns=headers)
        
        if df.shape[0] < 200:
            raise InsufficientRowsError("Получено меньше 200 записей \
                репозиториев. Попробуйте изменить параметры запроса")
        
        return df
          
    def __validate_date_range(self, start_date_string: str, end_date_string: str) -> bool:
        try:
            start_date = datetime.strptime(start_date_string, '%Y-%m-%d')
            end_date = datetime.strptime(end_date_string, '%Y-%m-%d')
            
            if start_date > end_date:
                return False
            
        except ValueError:
            return False
        
        return True