import pandas as pd
import numpy as np


class GithubDataConverter():
    SEPARATION = {'qua': [0.25, 0.5, 0.75],
            'dec': [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]}
    
    def __init__(self):
        self.data_configuration = pd.read_json(r"dtype_conf.json")
        self.column_name_short_decode_dict = {}
        for name, decode_name in zip(self.data_configuration["columnName"], 
                                    self.data_configuration["shortDecode"]):
            self.column_name_short_decode_dict[name] = decode_name

    def convert_data_to_transactions(self, 
                                     repos_data:pd.DataFrame, 
                                     quantile_config: dict[str, str],
                                     return_quantiles=False):
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
        quantile_config_keys = quantile_config.keys()
        
        # for column_name in quantile_config_keys:
        #     if column_name in self.column_name_short_decode_dict:
        #         quantile_config[self.column_name_short_decode_dict[column_name]] = quantile_config.pop(column_name)
        
        cleaned_data = self._clean_data(repos_data, quantile_config)

        quantile_column_values = self.__find_quantiles(cleaned_data, 
                                                quantile_config)

        quantile_data = self.__convert_data_to_quantile_numbers(cleaned_data,
                                                        quantile_column_values)
        
        quantile_data = self.__add_column_name_in_items(quantile_data)

        transactions = pd.get_dummies(quantile_data, prefix='')
        
        transactions = self.__format_column_names(transactions)
        
        qurtile_table, decile_table = self.__split_to_quartiles_and_deciles(quantile_column_values, quantile_config)
        
        if return_quantiles:
            return transactions, qurtile_table, decile_table
        
        return transactions
    
    def __split_to_quartiles_and_deciles(self, quantile_table, quantile_config):
        
        def edit_for_user_view(df, column_name):
            
            if df.empty:
                return
            # Сортировка строк в обратном порядке
            quntile_table = df.iloc[::-1]

            # Сброс индекса и начало отсчета с 1
            quntile_table = quntile_table.reset_index(drop=True)
            quntile_table.index = quntile_table.index + 1

            # Удаление лишних строк
            quntile_table = quntile_table.T.dropna().T
            
            
            quntile_table = quntile_table.reset_index().rename(columns={'index': column_name})
            
            for column in quntile_table.columns:
                quntile_table.rename(columns={column: column.replace('_', ' ')},
                        inplace=True)
            
            for column in quntile_table:
                quntile_table[column] = quntile_table[column].round(2)

            return quntile_table
        
        # Разделение на 2 таблицы 

        formatted_table = quantile_table.copy()
        # print(quantile_config)
        # print(quantile_table)
        quartiles = formatted_table[[column for column in formatted_table.columns if quantile_config.get(column) == 'qua']].dropna()
        deciles = quantile_table[[column for column in quantile_table.columns if quantile_config.get(column) == 'dec']].dropna()

        # print(quartiles)
        # print(deciles)

        # Редактирование вида таблиц
        quartiles = edit_for_user_view(quartiles, column_name='№ Квартиля')
        deciles = edit_for_user_view(deciles, '№ Дециля')
        
        return quartiles, deciles
    
    def __add_column_name_in_items(self, data):
        edited_column_names = data.copy()
        
        # Добавление назввания столбца в товар
        for column in edited_column_names.columns:
            if np.issubdtype(edited_column_names[column].dtype, np.number):
                edited_column_names[column] = edited_column_names[column].apply(
                    lambda x: f'{column} {x}')
                
        if 'language' in edited_column_names.columns:
            edited_column_names['language'] = edited_column_names['language'].apply(
                lambda x: 'language_' + x if x != 'None' and x != None else x)
                
        if 'license_name' in edited_column_names.columns:
            edited_column_names['license_name'] = edited_column_names['license_name'].apply(
                lambda x: 'license_' + x.replace('License', '') if x != 'None' and x is not None else x)
            
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
    
    def _clean_data(self, repo_data, quantile_config):
        clean_data = repo_data.drop_duplicates()
        
        for column in clean_data.columns:
            if (not column in self.data_configuration["columnName"].values 
                or not column in quantile_config 
                or clean_data[column].isnull().all() 
                or (clean_data[column] == 'None').all() 
                or (clean_data[column] == 0).mean() > 0.5):
                
                clean_data.drop(column, axis=1, inplace=True)
                
            # if column in self.column_name_short_decode_dict:
            #     clean_data.rename(columns={column: self.column_name_short_decode_dict[column]}, inplace=True)
        return clean_data
    
    def __format_column_names(self, transaction_data):
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
        
        # Так как в данных не может быть лицензии и языка
        while 'None' in rename_columns_data.columns:
            rename_columns_data.drop(columns='None', inplace=True)
            
        if 'Other' in rename_columns_data.columns:
            rename_columns_data.drop(columns='Other', inplace=True)
            
        return rename_columns_data