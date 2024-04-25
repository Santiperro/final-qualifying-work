import pandas as pd
import numpy as np


class GithubDataConverter():
    SEPARATION = {'qua': [0.25, 0.5, 0.75],
            'dec': [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]}
    
    def __init__(self):
        self.data_configuration = pd.read_json("dtype_conf.json")

    def convert_data_to_transactions(self, 
                                     repos_data:pd.DataFrame, 
                                     quantile_conf: dict[str, str]):
        """
        Метод для преобразования данных репозиториев в транзакции.

        Параметры:
        :param repos_data: Данные репозиториев.
        :type repos_data: pandas.DataFrame

        Возвращает:
        :return: Транзакции.
        :type: pandas.DataFrame
        """
        def edit_column_names(string):
            string = string.replace('_', '', 1)  # Удаление только первых символов '_'
            string = string.replace('_', ' ')  # Замена всех остальных символов '_' на пробел
            return string

        cleaned_data = self._clean_data(repos_data)
        quantile_data = self._convert_data_to_quantiles(cleaned_data,
                                                        quantile_conf)

        # Добавление назввания столбца в товар
        for column in quantile_data.columns:
            if np.issubdtype(quantile_data[column].dtype, np.number):
                quantile_data[column] = quantile_data[column].apply(lambda x: f'{column} {x}')

        transactions = pd.get_dummies(quantile_data, prefix='')

        # Удаление _ из названия столбцов
        for column in transactions.columns:
            transactions.rename(
                columns={column: edit_column_names(column)},
                inplace=True)
            
        while 'None' in transactions.columns:
            transactions.drop(columns='None', inplace=True)
            
        if 'Other' in transactions.columns:
            transactions.drop(columns='Other', inplace=True)
        
        return transactions
    
    def _convert_data_to_quantiles(self, 
                                   repo_data: pd.DataFrame, 
                                   quantile_conf: dict):
        """
        Метод для преобразования данных репозиториев в квантили.

        Параметры:
        :param repo_data: Данные репозиториев.
        :type repo_data: pandas.DataFrame
        :param quantile_column_values: Данные квантилей.
        :type quantiles_data: pandas.DataFrame

        Возвращает:
        :return: Данные репозиториев, преобразованные в квантили.
        :rtype: pandas.DataFrame
        """
        def find_nearest_value(value, quantiles):
            difference = abs(quantiles - value)
            idx = difference.idxmin()
            nearest_num = quantiles[idx]
            return nearest_num
        
        repo_data_copy = repo_data.copy()
        
        quantile_column_values = self._find_quantile_columns(repo_data_copy, 
                                                             quantile_conf)

        for column in repo_data_copy.select_dtypes(include=[np.number]).columns:
            if column in quantile_column_values.columns:
                repo_data_copy[column] = repo_data_copy[column].apply(find_nearest_value, quantiles=quantile_column_values[column].dropna())
                
        return repo_data_copy
    
    def _find_quantile_columns(self, 
                               dataframe: pd.DataFrame, 
                               quantile_conf: dict[str, str]):
        
        quantiles_by_column = {}
        numeric_columns = dataframe.select_dtypes(include=['number'])
        for column in numeric_columns:
            if column in quantile_conf.keys():
                quantile_type = quantile_conf[column]
                quantiles = self.SEPARATION[quantile_type]
                quantiles_by_column[column] = numeric_columns[column].quantile(quantiles)
                
        return pd.DataFrame(quantiles_by_column)
    
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