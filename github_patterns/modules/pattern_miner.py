import pandas as pd
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules


class PatternMiner():
    def mine_patterns(self, 
                      transactions_matrix, 
                      min_supp, 
                      min_conf,
                      min_lift,
                      min_left_elements=3, 
                      min_right_elements=1):
        
        def rename_column(name):
            if '_' in name:
                return name[name.index("_") + 1:]
            return name
        
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

        # editing column names
        # for column in transactions_matrix.columns:
        #     transactions_matrix.rename(
        #         columns={column: rename_column(column)},
        #         inplace=True)

        # Поиск частых наборов с параметром минимальной поддержки
        freaquent_itemsets = apriori(transactions_matrix, min_support=min_supp, use_colnames=True)

        # Поиск ассоциативных правил с заданной метрикой и её минимальным значением
        rules = association_rules(freaquent_itemsets, metric='lift', min_threshold=min_lift)

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
            
        rules["antecedents"] = rules["antecedents"].apply(lambda x: x if len(x) >= min_left_elements else None)
        rules["consequents"] = rules["consequents"].apply(lambda x: x if len(x) >= min_right_elements else None)
        rules["antecedents"] = rules["antecedents"].apply(lambda x: cell_to_string(x))
        rules["consequents"] = rules["consequents"].apply(lambda x: cell_to_string(x))
        rules.dropna(inplace=True)

        return rules