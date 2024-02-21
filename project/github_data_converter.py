import pandas as pd
import numpy as np


class GithubDataConverter():
    def convert_data_to_transactions(self, 
                                     repos_data:pd.DataFrame,
                                     quantiles_data: pd.DataFrame):
        """
        Метод для преобразования данных репозиториев в транзакции.

        Параметры:
        :param repos_data: Данные репозиториев.
        :type repos_data: pandas.DataFrame
        :param quantiles_data: Данные квантилей.
        :type quantiles_data: pandas.DataFrame

        Возвращает:
        :return: Транзакции.
        :rtype: pandas.DataFrame
        """
        cleaned_data = self._clean_data(repos_data)
        cleaned_data = self._convert_data_to_quantiles(cleaned_data, quantiles_data)
        transactions = self._convert_quantiles_to_items(cleaned_data, [0.75, 0.85, 0.95, 0.99, 0.999])
        return transactions
    
    def _clean_data(self, repo_data: pd.DataFrame):
        """
        Метод для очистки данных репозиториев.

        Параметры:
        :param repo_data: Данные репозиториев.
        :type repo_data: pandas.DataFrame

        Возвращает:
        :return: Очищенные данные.
        :rtype: pandas.DataFrame
        """
        clean_data = repo_data.drop_duplicates()
        clean_data.drop('repo', axis=1, inplace=True)
        return clean_data
    
    def _convert_data_to_quantiles(self, repo_data: pd.DataFrame, 
                                        quantiles_data: pd.DataFrame):
        """
        Метод для преобразования данных репозиториев в квантили.

        Параметры:
        :param repo_data: Данные репозиториев.
        :type repo_data: pandas.DataFrame
        :param quantiles_data: Данные квантилей.
        :type quantiles_data: pandas.DataFrame

        Возвращает:
        :return: Данные репозиториев, преобразованные в квантили.
        :rtype: pandas.DataFrame
        """
        def find_nearest_quantile(value, quantiles):
            return (quantiles - value).abs().idxmin()
        
        repo_data_copy = repo_data.copy()

        for column in repo_data_copy.select_dtypes(include=[np.number]).columns:
            if column in quantiles_data.columns:
                repo_data_copy[column] = repo_data_copy[column].apply(find_nearest_quantile, quantiles=quantiles_data[column])
                
        return repo_data_copy
                
    def _convert_quantiles_to_items(self, 
                         data_with_quantiles:pd.DataFrame, 
                         quantile_values):
        """
        Метод для преобразования квантилей в элементы транзакции.

        Параметры:
        :param data_with_quantiles: Данные с квантилями.
        :type data_with_quantiles: pandas.DataFrame
        :param quantile_values: Значения квантилей.
        :type quantile_values: list

        Возвращает:
        :return: Данные с квантилями, преобразованные в элементы.
        :rtype: pandas.DataFrame
        """
        quantile_data_copy = data_with_quantiles.copy()
        
        # Cловарь для кодирования квантилей
        quantile_encoding = {
            quantile_values[0]: 'Мало',
            quantile_values[1]: 'Среднее число',
            quantile_values[2]: 'Выше среднего',
            quantile_values[3]: 'Много',
            quantile_values[4]: 'Очень много'
        }

        # Cловарь для декодирования столбцов
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