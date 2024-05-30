import pandas as pd
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules
from .exceptions import *


class PatternMiner():
    def mine_patterns(self, 
                      transactions_matrix, 
                      min_supp, 
                      min_conf,
                      min_lift,
                      min_left_elements=1, 
                      min_right_elements=1,
                      max_left_elements=3,
                      max_right_elements=3):
        
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
        
        freaquent_itemsets = apriori(transactions_matrix, min_support=min_supp, use_colnames=True)
        check_for_empty_rules(freaquent_itemsets)

        # Поиск ассоциативных правил с заданной метрикой и её минимальным значением
        rules = association_rules(freaquent_itemsets, metric='lift', min_threshold=min_lift)
        check_for_empty_rules(rules)

        # Редактирование столбцов
        rules.drop('antecedent support', axis=1, inplace=True)
        rules.drop('consequent support', axis=1, inplace=True)
        rules.drop('leverage', axis=1, inplace=True)
        rules.drop('conviction', axis=1, inplace=True)
        if "zhangs_metric" in rules.columns:
            rules.drop('zhangs_metric', axis=1, inplace=True)
        rules["support"] = rules["support"].round(4)
        rules["confidence"] = rules["confidence"].round(4)
        rules["lift"] = rules["lift"].round(4)

        if min_conf:
            rules = rules[rules["confidence"] >= min_conf]
        
        check_for_empty_rules(rules)
            
        rules["antecedents"] = rules["antecedents"].apply(lambda x: x if len(x) >= min_left_elements else None)
        rules.dropna(inplace=True)
        check_for_empty_rules(rules)
        rules["consequents"] = rules["consequents"].apply(lambda x: x if len(x) >= min_right_elements else None)
        rules.dropna(inplace=True)
        check_for_empty_rules(rules)
            
        rules["antecedents"] = rules["antecedents"].apply(lambda x: x if len(x) <= max_left_elements else None)
        rules.dropna(inplace=True)
        check_for_empty_rules(rules)
        rules["consequents"] = rules["consequents"].apply(lambda x: x if len(x) <= max_right_elements else None)
        rules.dropna(inplace=True)
        check_for_empty_rules(rules)
        
        rules["antecedents"] = rules["antecedents"].apply(lambda x: cell_to_string(x))
        rules["consequents"] = rules["consequents"].apply(lambda x: cell_to_string(x))
        rules.dropna(inplace=True)
        
        check_for_empty_rules(rules)

        return rules