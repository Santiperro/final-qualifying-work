import pandas as pd
import numpy as np


class GithubDataConverter():
    SEPARATION = {'quartiles': [0.25, 0.5, 0.75],
            'deciles': [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]}
    
    COLUMN_ENCODING = {
        'pushes': 'push',
        'avg_push_size': 'средний размер push',
        'pull_requests': 'запросов на слияние',
        'merged_pull_requests_ratio': 'слитых pr ко всем',
        'issues': 'issue',
        'closed_issues_ratio': 'закрытых issue ко всем',
        'watches': 'watches',
        'forks': 'forks'
    }
    
    ROUNDING = {
        'repo': (0, 'int'),
        'pushes': (0, 'int'),
        'avg_push_size': (2, 'float'),
        'pull_requests': (0, 'int'),
        'merged_pull_requests_ratio': (2, 'float'),
        'issues': (0, 'int'),
        'closed_issues_ratio': (2, 'float'),
        'watches': (0, 'int'),
        'forks': (0, 'int'),
        'language': (0, 'str'),
        'license_name': (0, 'str'),
        'is_deleted_or_private': (0, 'bool')
    }
    
    def convert_data_to_transactions(self, 
                                     repos_data:pd.DataFrame,
                                     quantile_type: str='quartiles'):
        """
        Метод для преобразования данных репозиториев в транзакции.

        Параметры:
        :param repos_data: Данные репозиториев.
        :type repos_data: pandas.DataFrame
        :param quantiles_data: Данные квантилей.
        :type quantiles_data: pandas.DataFrame

        Возвращает:
        :return: Транзакции.
        :type: pandas.DataFrame
        """
        def edit_column_names(string):
            string = string.replace('_', '', 1)  # Удаление только первых символов '_'
            string = string.replace('_', ' ')  # Замена всех остальных символов '_' на пробел
            return string
        
        cleaned_data = self._clean_data(repos_data)
        column_quantile_values = self._find_quantile_columns(cleaned_data, quantile_type=quantile_type) #TODO Переместить в _convert_data_to_quantiles
        quantile_data = self._convert_data_to_quantiles(cleaned_data, column_quantile_values)
        
        # Форматирование числовых значений согласно схеме ROUNDING
        for column, (decimals, dtype) in self.ROUNDING.items():
            if column in quantile_data.columns:
                quantile_data[column] = quantile_data[column].round(decimals)
                quantile_data[column] = quantile_data[column].astype(dtype)
        
        # Добавление назввания столбца в товар
        for column in quantile_data.columns:
            if np.issubdtype(quantile_data[column].dtype, np.number):
                quantile_data[column] = quantile_data[column].apply(lambda x: f'{column} {x}')
                
        # if 'is_deleted_or_private' in quantile_data.columns:
        #     quantile_data = quantile_data.rename(columns={'is_deleted_or_private': 'Удален или скрыт'})
        
        transactions = pd.get_dummies(quantile_data, prefix='')
        
        # Удаление _ из названия столбцов
        for column in transactions.columns:
            transactions.rename(
                columns={column: edit_column_names(column)},
                inplace=True)
        
         # transactions = self._convert_quantiles_to_items(quantile_data, )
        return transactions
    
    def _convert_data_to_quantiles(self, repo_data: pd.DataFrame, 
                                        quantile_column_values: pd.DataFrame):
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

        for column in repo_data_copy.select_dtypes(include=[np.number]).columns:
            if column in quantile_column_values.columns:
                repo_data_copy[column] = repo_data_copy[column].apply(find_nearest_value, quantiles=quantile_column_values[column])
                
        return repo_data_copy
    
    def _find_quantile_columns(self, dataframe: pd.DataFrame, quantile_type: str='quartiles'):
        """
        Возвращает список названий числовых колонок, в которых содержатся квартили (3) или децили (9).
        :param dataframe: pandas DataFrame с данными
        :param quantile_type: тип квантилей ('quartiles' или 'deciles')
        :return: список названий колонок
        """
        
        numeric_columns = dataframe.select_dtypes(include=['number'])

        quantiles = numeric_columns.quantile(self.SEPARATION[quantile_type])

        return quantiles
    
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
    
            
    # def _convert_quantiles_to_items(self, 
    #                      data_with_quantiles:pd.DataFrame, 
    #                      quantile_values):
    #     """
    #     Метод для преобразования квантилей в элементы транзакции.

    #     Параметры:
    #     :param data_with_quantiles: Данные с квантилями.
    #     :type data_with_quantiles: pandas.DataFrame
    #     :param quantile_values: Значения квантилей.
    #     :type quantile_values: list

    #     Возвращает:
    #     :return: Данные с квантилями, преобразованные в элементы.
    #     :rtype: pandas.DataFrame
    #     """
        
    #     quantile_data_copy = data_with_quantiles.copy()
        
    #     # Cловарь для кодирования квантилей
    #     quantile_encoding = {
    #         quantile_values[0]: 'Мало',
    #         quantile_values[1]: 'Среднее число',
    #         quantile_values[2]: 'Выше среднего',
    #         quantile_values[3]: 'Много',
    #         quantile_values[4]: 'Очень много'
    #     }

    #     # Cловарь для декодирования столбцов
        
        
    #     # Кодируем квантили и столбцы
    #     for column in quantile_data_copy.select_dtypes(include=[np.number]).columns:
    #         if column in self.COLUMN_ENCODING:
    #             quantile_data_copy[column] = quantile_data_copy[column].map(quantile_encoding) + ' ' + column_encoding[column]

    #     return quantile_data_copy