import pandas as pd
import json
from datetime import datetime
import os
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from exceptions.invalid_date_format_exception import InvalidDateFormatException


class ClickHouseDemo():
    CLICKHOUSE_DEMO_URL = 'https://play.clickhouse.com/play?user=play'
     
    def get_repos_data(self, start_date, end_date=None, limit=1000, watch_event_count=10):
        CLICKHOUSE_DATA_QUERY = """
        SELECT 
            outerQuery.repo_name as repo,
            pushEvents.pushEventCount as pushes,
            pushEvents.avgPushSize as avg_push_size,
            pullRequestEvents.pullRequestEventCount as pull_requests,
            pullRequestEvents.mergedPullRequestEventRatio as merged_pull_requests_ratio,
            issuesEvents.issuesEventCount as issues,
            issuesEvents.closedIssuesRatio as closed_issues_ratio,
            watchEvents.watchEventCount as watches,
            forkEvents.forkEventCount as forks
        FROM
            (SELECT repo_name
            FROM github_events 
            WHERE '{start_date}' <= created_at 
                AND created_at < '{end_date}' 
                AND event_type = 'CreateEvent' 
                AND ref_type = 'repository') as outerQuery
        INNER JOIN
            (SELECT repo_name
            FROM github_events 
            WHERE '{start_date}' <= created_at 
                AND created_at < '{end_date}' 
                AND event_type = 'WatchEvent'
            GROUP BY repo_name
            HAVING COUNT(*) >= {watch_event_count}) as reposWithWatchEvent
        ON outerQuery.repo_name = reposWithWatchEvent.repo_name
        ANY LEFT JOIN
            (SELECT repo_name, COUNT(*) as pushEventCount, AVG(push_size) as avgPushSize
            FROM github_events 
            WHERE '{start_date}' <= created_at 
                AND created_at < '{end_date}' 
                AND event_type = 'PushEvent' 
            GROUP BY repo_name) as pushEvents
        ON outerQuery.repo_name = pushEvents.repo_name
        ANY LEFT JOIN
            (SELECT 
                repo_name, COUNT(*) as pullRequestEventCount, 
                SUM(CASE WHEN merged = 1 THEN 1 ELSE 0 END) / COUNT(*) as mergedPullRequestEventRatio
            FROM github_events 
            WHERE '{start_date}' <= created_at 
                AND created_at < '{end_date}' 
                AND event_type = 'PullRequestEvent'
            GROUP BY repo_name) as pullRequestEvents
        ON outerQuery.repo_name = pullRequestEvents.repo_name
        ANY LEFT JOIN
            (SELECT 
                repo_name, COUNT(*) as issuesEventCount, 
                SUM(CASE WHEN state = 'closed' THEN 1 ELSE 0 END) / COUNT(*) as closedIssuesRatio
            FROM github_events 
            WHERE '{start_date}' <= created_at 
                AND created_at < '{end_date}' 
                AND event_type = 'IssuesEvent' 
            GROUP BY repo_name) as issuesEvents
        ON outerQuery.repo_name = issuesEvents.repo_name 
        ANY LEFT JOIN
            (SELECT repo_name, COUNT(*) as watchEventCount
            FROM github_events 
            WHERE '{start_date}' <= created_at 
                AND created_at < '{end_date}' 
                AND event_type = 'WatchEvent' 
            GROUP BY repo_name) as watchEvents
        ON outerQuery.repo_name = watchEvents.repo_name
        ANY LEFT JOIN
            (SELECT repo_name, COUNT(*) as forkEventCount
            FROM github_events 
            WHERE '{start_date}' <= created_at 
                AND created_at < '{end_date}' 
                AND event_type = 'ForkEvent' 
            GROUP BY repo_name) as forkEvents
        ON outerQuery.repo_name = forkEvents.repo_name
        ORDER BY RAND()
        LIMIT {limit};
        """
        
        if not start_date:
            return
        
        if not end_date:
            end_date = start_date
        
        if not (self._validate_date(start_date) 
                and self._validate_date(end_date)):
            return
        
        query = CLICKHOUSE_DATA_QUERY.format( 
                start_date=start_date, 
                end_date=end_date,
                limit=limit,
                watch_event_count=watch_event_count)
        
        delay = 8
        
        data = self._clickhouse_demo_request(query, delay)
        data['pushes'] = data['pushes'].astype(int)
        data['pull_requests'] = data['pull_requests'].astype(int)
        data['issues'] = data['issues'].astype(int)
        data['watches'] = data['watches'].astype(int)
        data['forks'] = data['forks'].astype(int)
        
        data['avg_push_size'] = data['avg_push_size'].astype(float)
        data['merged_pull_requests_ratio'] = data['merged_pull_requests_ratio'].astype(float)
        data['closed_issues_ratio'] = data['closed_issues_ratio'].astype(float)
        return data
    
    
    def get_quantiles(self, start_date, end_date, table_columns, quantiles):
        FORKS_QUANTILES_QUERY = """
        SELECT 
            quantile({quantile1})(forkEventCount) as quantile1_fork_events,
            quantile({quantile2})(forkEventCount) as quantile2_fork_events,
            quantile({quantile3})(forkEventCount) as quantile3_fork_events,
            quantile({quantile4})(forkEventCount) as quantile4_fork_events,
            quantile({quantile5})(forkEventCount) as quantile5_fork_events
        FROM
            (SELECT repo_name, COUNT(*) as forkEventCount
            FROM github_events 
            WHERE '{start_date}' <= created_at 
                AND created_at < '{end_date}' 
                AND event_type = 'ForkEvent' 
            GROUP BY repo_name)
        """
        
        PUSHES_QUANTILES_QUERY = """
        SELECT 
            quantile({quantile1})(pushEventCount) as quantile1_push_events,
            quantile({quantile2})(pushEventCount) as quantile2_push_events,
            quantile({quantile3})(pushEventCount) as quantile3_push_events,
            quantile({quantile4})(pushEventCount) as quantile4_push_events,
            quantile({quantile5})(pushEventCount) as quantile5_push_events
        FROM
            (SELECT repo_name, COUNT(*) as pushEventCount
            FROM github_events 
            WHERE '{start_date}' <= created_at 
                AND created_at < '{end_date}' 
                AND event_type = 'PushEvent' 
            GROUP BY repo_name)
        """
        AVG_PUSH_SIZE_QUANTILES_QUERY = """
        SELECT 
            quantile({quantile1})(avgPushSize) as quantile1_avg_push_size,
            quantile({quantile2})(avgPushSize) as quantile2_avg_push_size,
            quantile({quantile3})(avgPushSize) as quantile3_avg_push_size,
            quantile({quantile4})(avgPushSize) as quantile4_avg_push_size,
            quantile({quantile5})(avgPushSize) as quantile5_avg_push_size
        FROM
            (SELECT repo_name, AVG(push_size) as avgPushSize
            FROM github_events 
            WHERE '{start_date}' <= created_at 
                AND created_at < '{end_date}' 
                AND event_type = 'PushEvent' 
            GROUP BY repo_name)
        """
        
        PULL_REQUESTS_QUANTILES_QUERY = """
        SELECT 
            quantile({quantile1})(pullRequestEventCount) as quantile1_pull_requests,
            quantile({quantile2})(pullRequestEventCount) as quantile2_pull_requests,
            quantile({quantile3})(pullRequestEventCount) as quantile3_pull_requests,
            quantile({quantile4})(pullRequestEventCount) as quantile4_pull_requests,
            quantile({quantile5})(pullRequestEventCount) as quantile5_pull_requests
        FROM
            (SELECT 
                repo_name, COUNT(*) as pullRequestEventCount
            FROM github_events 
            WHERE '{start_date}' <= created_at 
                AND created_at < '{end_date}' 
                AND event_type = 'PullRequestEvent'
            GROUP BY repo_name)
        """
        
        MERGED_PULL_REQUESTS_RATIO_QUANTILES_QUERY = """
        SELECT 
            quantile({quantile1})(mergedPullRequestEventRatio) as quantile1_merged_pull_request_ratio,
            quantile({quantile2})(mergedPullRequestEventRatio) as quantile2_merged_pull_request_ratio,
            quantile({quantile3})(mergedPullRequestEventRatio) as quantile3_merged_pull_request_ratio,
            quantile({quantile4})(mergedPullRequestEventRatio) as quantile4_merged_pull_request_ratio,
            quantile({quantile5})(mergedPullRequestEventRatio) as quantile5_merged_pull_request_ratio
        FROM
            (SELECT 
                repo_name, SUM(CASE WHEN merged = 1 THEN 1 ELSE 0 END) / COUNT(*) as mergedPullRequestEventRatio
            FROM github_events 
            WHERE '{start_date}' <= created_at 
                AND created_at < '{end_date}' 
                AND event_type = 'PullRequestEvent'
            GROUP BY repo_name)
        """
        
        ISSUES_QUANTILES_QUERY = """
        SELECT 
            quantile({quantile1})(issuesEventCount) as quantile1_issues,
            quantile({quantile2})(issuesEventCount) as quantile2_issues,
            quantile({quantile3})(issuesEventCount) as quantile3_issues,
            quantile({quantile4})(issuesEventCount) as quantile4_issues,
            quantile({quantile5})(issuesEventCount) as quantile5_issues
        FROM
            (SELECT 
                repo_name, COUNT(*) as issuesEventCount
            FROM github_events 
            WHERE '{start_date}' <= created_at 
                AND created_at < '{end_date}' 
                AND event_type = 'IssuesEvent' 
            GROUP BY repo_name)
        """
        
        CLOSED_ISSUES_RATIO_QUANTILES_QUERY = """
        SELECT 
            quantile({quantile1})(closedIssuesRatio) as quantile1_closed_issues_ratio,
            quantile({quantile2})(closedIssuesRatio) as quantile2_closed_issues_ratio,
            quantile({quantile3})(closedIssuesRatio) as quantile3_closed_issues_ratio,
            quantile({quantile4})(closedIssuesRatio) as quantile4_closed_issues_ratio,
            quantile({quantile5})(closedIssuesRatio) as quantile5_closed_issues_ratio
        FROM
            (SELECT 
                repo_name, SUM(CASE WHEN state = 'closed' THEN 1 ELSE 0 END) / COUNT(*) as closedIssuesRatio
            FROM github_events 
            WHERE '{start_date}' <= created_at 
                AND created_at < '{end_date}' 
                AND event_type = 'IssuesEvent' 
            GROUP BY repo_name)
        """
        
        WATCHES_QUANTILES_QUERY = """
        SELECT 
            quantile({quantile1})(watchEventCount) as quantile1_watch_events,
            quantile({quantile2})(watchEventCount) as quantile2_watch_events,
            quantile({quantile3})(watchEventCount) as quantile3_watch_events,
            quantile({quantile4})(watchEventCount) as quantile4_watch_events,
            quantile({quantile5})(watchEventCount) as quantile5_watch_events
        FROM
            (SELECT repo_name, COUNT(*) as watchEventCount
            FROM github_events 
            WHERE '{start_date}' <= created_at 
                AND created_at < '{end_date}' 
                AND event_type = 'WatchEvent' 
            GROUP BY repo_name)
        """
        
        COLUMN_NAME_QUANTILE_QUERY = {
            'forks': FORKS_QUANTILES_QUERY, 
            'pushes': PUSHES_QUANTILES_QUERY, 
            'avg_push_size': AVG_PUSH_SIZE_QUANTILES_QUERY,
            'pull_requests': PULL_REQUESTS_QUANTILES_QUERY,
            'merged_pull_requests_ratio': MERGED_PULL_REQUESTS_RATIO_QUANTILES_QUERY,
            'issues': ISSUES_QUANTILES_QUERY,
            'closed_issues_ratio': CLOSED_ISSUES_RATIO_QUANTILES_QUERY, 
            'watches': WATCHES_QUANTILES_QUERY}
        
        if not table_columns or len(table_columns) == 0:
            return
    
        filtered_table_columns = [item for item in table_columns if item in COLUMN_NAME_QUANTILE_QUERY]
        
        deciles_df = pd.DataFrame(columns=quantiles)
        
        for column in filtered_table_columns:
            query = COLUMN_NAME_QUANTILE_QUERY[column].format( 
                start_date=start_date, 
                end_date=end_date,
                quantile1 = quantiles[0],
                quantile2 = quantiles[1],
                quantile3 = quantiles[2],
                quantile4 = quantiles[3],
                quantile5 = quantiles[4])
            
            quantiles_df = self._clickhouse_demo_request(query, delay=3, quantiles=True)
            deciles_df.loc[column] = list(quantiles_df["quantiles"])
        deciles_df = deciles_df.astype(float)
        return deciles_df.T
    

    def _clickhouse_demo_request(self, query, delay=30, quantiles=False):

        # Создаем новую вкладку для каждого запроса
        driver = webdriver.Chrome()
        driver.get(self.CLICKHOUSE_DEMO_URL)
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
        
        
    def _validate_date(self, date_string):
        try:
            datetime.strptime(date_string, '%Y-%m-%d')
            return True
        except ValueError:
            return False