import pandas as pd
from core.field_mapper import FieldMapper
from balance_validator import BalanceValidator

# DataFrame simple con solo 'amount'
df = pd.DataFrame({
    'Entry No_': [1,1,2,2],
    'Transaction No_': [1,1,2,2],
    'Amount': [100.0, -100.0, 200.0, -200.0]
})

mapper = FieldMapper()
# Preparar df para la validación
mapper.set_dataframe_for_balance_validation(df)

# Simular que el mapper ya identificó 'Amount' como amount
mapper._used_field_mappings['amount'] = 'Amount'
mapper._confidence_by_column['Amount'] = 0.90

# Instanciar validator
bv = BalanceValidator()

# Candidatos a journal_entry_id
candidates = [('Entry No_', 0.90), ('Transaction No_', 0.85)]
winner = mapper._resolve_journal_entry_id_with_balance(candidates, mapper._dataframe_for_balance, bv)
print("WINNER:", winner)
