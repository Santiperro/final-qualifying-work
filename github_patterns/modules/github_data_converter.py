import pandas as pd
import numpy as np


class GithubDataConverter():
    SEPARATION = {'qua': [0.25, 0.5, 0.75],
            'dec': [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]}
    
    def __init__(self):
        self.data_configuration = pd.read_json(r"dtype_conf.json")

    def convert_data_to_transactions(self, 
                                     repos_data:pd.DataFrame, 
                                     quantile_config: dict[str, str]):
        """
        quantile_conf example:
        {'pushes': 'dec',
        'avg_push_size': 'dec',
        'pull_requests': 'qua',
        'merged_pull_requests_ratio': 'dec',
        'issues': 'dec',
        'closed_issues_ratio': 'qua',
        'watches': 'dec',
        'forks': 'dec',
        'new_members': 'dec',
        'language': 'None',
        'license_name': 'None',
        'is_deleted_or_private': 'None'}
        """
        cleaned_data = self._clean_data(repos_data)
        
        quantile_column_values = self.__find_quantiles(cleaned_data, 
                                                quantile_config)

        quantile_data = self.__convert_data_to_quantile_numbers(cleaned_data,
                                                        quantile_column_values)
        
        quantile_data = self.__add_column_name_in_items(quantile_data)

        transactions = pd.get_dummies(quantile_data, prefix='')
        
        transactions = self.__delete__column_names(transactions)
        
        return transactions
    
    def __add_column_name_in_items(self, data):
        edited_column_names = data.copy()
        
        # Добавление назввания столбца в товар
        for column in edited_column_names.columns:
            if np.issubdtype(edited_column_names[column].dtype, np.number):
                edited_column_names[column] = edited_column_names[column].apply(
                    lambda x: f'{column} {x}')
        return edited_column_names
    
    def __convert_data_to_quantile_numbers(self, 
                                           repo_data: pd.DataFrame,
                                           quantile_column_values: pd.DataFrame):
        
        def find_quantile_rank(value, quantiles):
            quantiles = sorted(quantiles, reverse=True)
            for i, q in enumerate(quantiles, 1):
                if value >= q:
                    return i
            return len(quantiles) + 1
        
        quantile_numbers_data = repo_data.copy()

        number_columns = quantile_numbers_data.select_dtypes(include=[np.number])
        for column in number_columns.columns:
            if column in quantile_column_values.columns:
                quantile_numbers_data[column] = quantile_numbers_data[column].apply(
                    find_quantile_rank,
                    quantiles=quantile_column_values[column].dropna())
                
        return quantile_numbers_data
    
    def __find_quantiles(self, 
                         data: pd.DataFrame, 
                         quantile_conf: dict[str, str]):
        
        quantiles_by_column = {}
        numeric_columns = data.select_dtypes(include=['number'])
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
        for column in clean_data.columns:
            if not column in self.data_configuration["columnName"].values:
                clean_data.drop(column, axis=1, inplace=True)
       
        return clean_data
    
    def __delete__column_names(self, transaction_data):
        def edit_column_names(string):
            string = string.replace('_', '', 1)  # Удаление только первых символов '_'
            string = string.replace('_', ' ')  # Замена всех остальных символов '_' на пробел
            return string
        
        rename_columns_data = transaction_data.copy()
        
        # Удаление _ из названия столбцов
        for column in rename_columns_data.columns:
            rename_columns_data.rename(
                columns={column: edit_column_names(column)},
                inplace=True)
            
        while 'None' in rename_columns_data.columns:
            rename_columns_data.drop(columns='None', inplace=True)
            
        if 'Other' in rename_columns_data.columns:
            rename_columns_data.drop(columns='Other', inplace=True)
            
        return rename_columns_data