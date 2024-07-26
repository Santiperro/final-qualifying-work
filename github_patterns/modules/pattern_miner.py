import pandas as pd
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules
from mlxtend.frequent_patterns import fpgrowth
from .exceptions import *


class PatternMiner():
    def mine_patterns(self, 
                      transactions_matrix: pd.DataFrame, 
                      min_supp: float, 
                      min_conf: float,
                      min_lift: float,
                      min_left_elements: int=1, 
                      min_right_elements: int=1,
                      max_left_elements: int=3,
                      max_right_elements: int=3):
        """
        Mine patterns from transaction data using Apriori algorithm and generate association rules.
        
        Parameters:
        - transactions_matrix: pd.DataFrame - Binary matrix of transactions.
        - min_supp: float - Minimum support threshold.
        - min_conf: float - Minimum confidence threshold.
        - min_lift: float - Minimum lift threshold.
        - min_left_elements: int - Minimum number of elements in the antecedent.
        - min_right_elements: int - Minimum number of elements in the consequent.
        - max_left_elements: int - Maximum number of elements in the antecedent.
        - max_right_elements: int - Maximum number of elements in the consequent.
        
        Returns:
        - pd.DataFrame - Dataframe containing the association rules.
        """
        
        def cell_to_string(cell):
            if not isinstance(cell, str):
                try:
                    iter(cell)
                    string = ""
                    for item in cell:
                        string += f"({item}), "
                    string = string[:-2]
                    return string
                except TypeError:
                    pass
            return cell
        
        def check_for_empty_rules(rules):
            if rules.empty:
                raise NoPatternsException("Шаблоны не найдены. Попробуйте изменить параметры поиска")
            
        def drop_unnecessary_columns(dataframe: pd.DataFrame, 
                                     columns: list[str]):
            
            changed_dataframe = dataframe.copy()
            for column in columns:
                if column in changed_dataframe.columns:
                    changed_dataframe = changed_dataframe.drop(column, 
                                                               axis=1)
            return changed_dataframe
                    
        def round_unnecessary_columns(dataframe: pd.DataFrame, 
                                      columns: list[str]):
            
            changed_dataframe = dataframe.copy()
            for column in columns:
                if column in changed_dataframe.columns:
                    changed_dataframe[column] = changed_dataframe[column].round(4)
                    
            return changed_dataframe    
        
        freaquent_itemsets = apriori(transactions_matrix, min_support=min_supp, use_colnames=True)
        check_for_empty_rules(freaquent_itemsets)

        rules = association_rules(freaquent_itemsets, metric='lift', min_threshold=min_lift)
        check_for_empty_rules(rules)
        
        rules = drop_unnecessary_columns(rules, ['antecedent support',
                                         'consequent support',
                                         'leverage',
                                         'conviction',
                                         'zhangs_metric'])
        
        rules = round_unnecessary_columns(rules, 
                                          ['support',
                                           'confidence',
                                           'lift'])

        if min_conf:
            rules = rules[rules["confidence"] >= min_conf]
        
        check_for_empty_rules(rules)
            
        rules = rules[rules['antecedents'].apply(len).between(min_left_elements, max_left_elements)]
        check_for_empty_rules(rules)
        rules = rules[rules['consequents'].apply(len).between(min_right_elements, max_right_elements)]
        check_for_empty_rules(rules)  
        
        rules["antecedents"] = rules["antecedents"].apply(lambda x: cell_to_string(x))
        rules["consequents"] = rules["consequents"].apply(lambda x: cell_to_string(x))
        rules.dropna(inplace=True)
        
        check_for_empty_rules(rules)

        return rules