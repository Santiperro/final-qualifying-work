import pandas as pd
import numpy as np


class GithubDataConverter():
    """
    A class to convert Github data into transactions.

    Attributes:
        SEPARATION_TYPES (dict): A dictionary with types of data separation.
        data_configuration (DataFrame): Data configuration read from a JSON file.
    """
    SEPARATION_TYPES = {'qua': [0, 0.25, 0.5, 0.75, 1],
            'dec': [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]}
    
    def __init__(self):
        self.data_configuration = pd.read_json(r"dtype_conf.json")
        self.quartile_table = None
        self.decile_table = None

    def convert_data_to_transactions(self, 
                                     repos_data: pd.DataFrame, 
                                     quantile_config: dict[str, str]):
        """
        Converts the repository data into transactions based on the quantile configuration.

        Args:
            repos_data (DataFrame): The repository data to be converted.
            quantile_config (dict): The configuration for quantile conversion.
            return_quantiles (bool, optional): Whether to return quantiles. Defaults to False.

        Returns:
            DataFrame: The converted transactions.
        """
        # Remove dublicates and attribures with a lot of zero values
        cleaned_data = self._clean_data(repos_data, quantile_config)

        quantile_column_values = self.__find_quantiles(cleaned_data, 
                                                quantile_config)

        quantile_data = self.__convert_data_to_quantile_numbers(cleaned_data,
                                                        quantile_column_values)
        
        quantile_data = self.__add_column_name_in_items(quantile_data)

        transactions = pd.get_dummies(quantile_data, prefix='')
        
        transactions = self.__format_column_names(transactions)
        
        qurtile_table, decile_table = self.__split_to_quartiles_and_deciles(quantile_column_values, quantile_config)
        
        self.quartile_table = qurtile_table
        self.decile_table = decile_table
        
        return transactions
    
    def __split_to_quartiles_and_deciles(self, 
                                         quantile_table, 
                                         quantile_config):
        
        def edit_for_user_view(quatniles_table: pd.DataFrame, column_name):
            if quatniles_table.empty:
                return
            
            temp_dataframe = quatniles_table.copy()
            temp_dataframe = temp_dataframe.iloc[::-1]
            
            data_config = self.data_configuration
            
            quatniles_table_for_user_view = pd.DataFrame()
            for column in temp_dataframe.columns:
                dtype = data_config[data_config["columnName"] == column].reset_index(drop=True).loc[0, "dtype"]
                if dtype == "int":
                    cur_column_data = temp_dataframe[column].dropna().astype('object')
                    shifted = cur_column_data.shift(-1)
                    shifted.iloc[-1] = cur_column_data.astype(int).iloc[-1]
                    shifted = shifted.astype(int)
                    mask = cur_column_data.astype(int).astype(str) != "0"
                    cur_column_data.loc[mask] = (
                        shifted.loc[mask].astype(str)
                        + ' - ' +
                        cur_column_data.loc[mask].astype(int).astype(str)
                    )
                elif dtype == "float":
                    cur_column_data = temp_dataframe[column].dropna().round(2).astype('object')
                    shifted = cur_column_data.shift(-1)
                    shifted.iloc[-1] = cur_column_data.iloc[-1]
                    shifted = shifted.round(2)
                    mask = cur_column_data.round(2).astype(str) != "0.0"
                    cur_column_data.loc[mask] = (
                        shifted.loc[mask].astype(str)
                        + ' - ' +
                        cur_column_data.loc[mask].round(2).astype(str)
                    )
                quatniles_table_for_user_view[column] = cur_column_data.reset_index(drop=True)
            
            quatniles_table_for_user_view[quatniles_table_for_user_view.isna()] = '-'
            
            n = len(quatniles_table_for_user_view.index)
            index_mapping = {quatniles_table_for_user_view.index[i]: f'{column_name} {i + 1}' for i in range(n)}
            quatniles_table_for_user_view = quatniles_table_for_user_view.rename(index=index_mapping)
            quatniles_table_for_user_view = quatniles_table_for_user_view.reset_index()
            quatniles_table_for_user_view.rename(columns={'index': ''}, inplace=True)
            
            for column in quatniles_table_for_user_view.columns:
                quatniles_table_for_user_view.rename(columns={column: column.replace('_', ' ')},
                        inplace=True)
                
            return quatniles_table_for_user_view
        
        formatted_table = quantile_table.copy()
        quartiles = formatted_table[[column for column in formatted_table.columns if quantile_config.get(column) == 'qua']]
        deciles = quantile_table[[column for column in quantile_table.columns if quantile_config.get(column) == 'dec']]

        quartiles = edit_for_user_view(quartiles, column_name='Квартиль')
        deciles = edit_for_user_view(deciles, 'Дециль')
        
        return quartiles, deciles
    
    def __add_column_name_in_items(self, data):
        edited_column_names = data.copy()
        
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
                                           quantile_values: pd.DataFrame):
        
        def find_quantile_rank(value, quantiles):
            quantiles = sorted(quantiles, reverse=True)
            for i, q in enumerate(quantiles, 1):
                if value > q:
                    return i
            return len(quantiles) + 1
        
        quantile_numbers_data = repo_data.copy()
        
        quantile_values_without_bounds = quantile_values.drop([0, 1])

        number_columns = quantile_numbers_data.select_dtypes(include=[np.number])
        
        for column in number_columns.columns:
            if column in quantile_values_without_bounds.columns:
                column_quantiles_values = quantile_values_without_bounds[column].dropna()
                quantile_numbers_data[column] = quantile_numbers_data[column].apply(
                    find_quantile_rank,
                    quantiles=column_quantiles_values)
        return quantile_numbers_data
    
    def __find_quantiles(self, 
                         data: pd.DataFrame, 
                         quantile_conf: dict[str, str]):
        
        quantiles_by_column = {}
        numeric_columns = data.select_dtypes(include=['number'])
        for column in numeric_columns:
            if column in quantile_conf.keys():
                quantile_type = quantile_conf[column]
                quantiles = self.SEPARATION_TYPES[quantile_type]
                quantiles_by_column[column] = numeric_columns[column].quantile(quantiles)
          
        # Remove quantile duplicates coz we need unique discretization
        quantiles = pd.DataFrame(quantiles_by_column).iloc[::-1].apply(lambda x: x.mask(x.duplicated(), np.nan)).iloc[::-1]
        return quantiles
    
    def _clean_data(self, repo_data, quantile_config):
        clean_data = repo_data.drop_duplicates().copy()
        
        for column in clean_data.columns:
            if (not column in self.data_configuration["columnName"].values 
                or not column in quantile_config 
                or clean_data[column].isnull().all() 
                or (clean_data[column] == 'None').all() 
                or (clean_data[column] == 0).mean() > 0.5):
                
                clean_data.drop(column, axis=1, inplace=True)
            
        return clean_data
    
    def __format_column_names(self, transaction_data):
        def edit_column_names(string):
            string = string.replace('_', '', 1)
            string = string.replace('_', ' ')
            return string
        
        rename_columns_data = transaction_data.copy()
        
        for column in rename_columns_data.columns:
            rename_columns_data.rename(
                columns={column: edit_column_names(column)},
                inplace=True)
        
        while 'None' in rename_columns_data.columns:
            rename_columns_data.drop(columns='None', inplace=True)
            
        if 'Other' in rename_columns_data.columns:
            rename_columns_data.drop(columns='Other', inplace=True)
            
        return rename_columns_data