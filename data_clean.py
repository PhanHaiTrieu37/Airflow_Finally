import pandas as pd
import os

def clean_data():
    
    # df = pd.read_csv('/Workspace/study/BigData/OnCK/PhanHaiTrieu20089981/data/heart.csv')
    df = pd.read_csv('/opt/airflow/data/heart.csv')
    print(df.info()) 
    print(df)
    # * Kiểm tra dữ Liệu bị thiếu
    missing_data = df.isna() # hoặc df.isnull()

    # # Đếm số lượng giá trị bị thiếu trong mỗi cột
    missing_count_per_column = missing_data.sum()

    # # Tổng số lượng giá trị bị thiếu trong toàn bộ DataFrame
    total_missing_count = missing_data.sum().sum()

    # print("Số lượng giá trị bị thiếu trong mỗi cột:")
    print(missing_count_per_column)

    # print("\nTổng số lượng giá trị bị thiếu trong toàn bộ DataFrame:", total_missing_count)
    default_values = {
        int:0,
        float:0.0,
        str: ''
    }

    clean_data = df.fillna(value=default_values)

    # Lọc dữ liệu với high > 24
    filtered_data = df.loc[df['resting_blood_pressure'] > 120]

    # In ra dữ Liệu da lọc
    print(filtered_data)

    now = pd.Timestamp.now()
    year= now.year
    month = now.month
    day = now.day

    data_dir = f'/opt/airflow/data/clean_data/{year}/{month}/{day}'
    # data_dir = f'/Workspace/study/BigData/OnCK/PhanHaiTrieu20089981/data/clean_data/{year}/{month}/{day}'
    os.makedirs(data_dir, exist_ok=True)

    filtered_data.to_csv(f'{data_dir}/stock_price.csv', index= False)
    filtered_data.to_json(f'{data_dir}/stock_price.json', orient='records')

clean_data()