
## Tree

```
.
├── [2.7M]  trivago_val_100k.parquet.snappy
└── [ 47M]  validation.parquet.gz

  50M used in 0 directories, 2 files
```

## Processing

```python
# v3
!wget -q --show-progress https://github.com/recohut/datasets/raw/master/trivago/v1/validation.parquet.gz

import pandas as pd
df = pd.read_parquet('validation.parquet.gz')
df.head()

def get_small_dataset(df, maximum_rows=100000):
    """
    Return a dataframe from the original dataset containing a maximum number of rows. The actual total rows
    extracted may vary in order to avoid breaking the last session.
    :param df: dataframe
    :param maximum_rows:
    :return: dataframe
    """
    if len(df) < maximum_rows:
      return df
    # get the last row
    last_row = df.iloc[[maximum_rows]]
    last_session_id = last_row.session_id.values[0]

    # OPTIMIZATION: last_user_id = last_row.user_id.values[0]

    # slice the dataframe from the target row on
    temp_df = df.iloc[maximum_rows:]
    # get the number of remaining interactions of the last session
    # OPTIMIZATION: remaining_rows = temp_df[(temp_df.session_id == last_session_id) & (temp_df.user_id == last_user_id)].shape[0]
    remaining_rows = temp_df[temp_df.session_id == last_session_id].shape[0]
    # slice from the first row to the final index
    return df.iloc[0:maximum_rows+remaining_rows]

# df_small = get_small_dataset(df, maximum_rows=100000)

print(df_small.info())
print(list(df_small.columns))

df_small.to_parquet('trivago_val_100k.parquet.snappy', compression='snappy')

!git add *.parquet.snappy
!git status
```