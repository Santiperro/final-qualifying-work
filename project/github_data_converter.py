import pandas as pd
import numpy as np


class GithubDataConverter():

    def merge_data_tables_by_repo(self, table1, table2):
        repos_data = table1.join(table2.set_index("repo"), on="repo")
        repos_data.drop_duplicates(inplace=True)
        return repos_data.drop('repo', axis=1)
    
    def convert_data_to_quantiles(self, repos_data, quantiles_data: pd.DataFrame):
        repos_data_copy = repos_data.copy()
        
        def find_nearest_quantile(value, quantiles):
            return (quantiles - value).abs().idxmin()
    
        for column in repos_data_copy.select_dtypes(include=[np.number]).columns:
            if column in quantiles_data.columns:
                repos_data_copy[column] = repos_data_copy[column].apply(find_nearest_quantile, quantiles=quantiles_data[column])
        
        return repos_data_copy

    def encode_quantiles(self, quantile_data, quantiles): 
        
        quantile_data_copy = quantile_data.copy()
        # Cловарь для кодирования квантилей
        quantile_encoding = {
            quantiles[0]: 'Мало',
            quantiles[1]: 'Среднее число',
            quantiles[2]: 'Выше среднего',
            quantiles[3]: 'Много',
            quantiles[4]: 'Очень много'
        }

        # Cловарь для кодирования столбцов
        column_encoding = {
            'pushes': 'пушей',
            'avg_push_size': 'размера пушей',
            'pull_requests': 'запросов на слияние',
            'merged_pull_requests_ratio': 'слияний запросов',
            'issues': 'ишью',
            'closed_issues_ratio': 'закрытых ишью',
            'watches': 'звезд',
            'forks': 'форков'
        }
        
        # Кодируем квантили и столбцы
        for column in quantile_data_copy.select_dtypes(include=[np.number]).columns:
            if column in column_encoding:
                quantile_data_copy[column] = quantile_data_copy[column].map(quantile_encoding) + ' ' + column_encoding[column]

        return quantile_data_copy