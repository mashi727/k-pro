import pandas as pd
import yfinance as yf
import requests
import os
from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path

# 環境変数を読み込む関数
def load_environment_variables():
    load_dotenv()
    api_key = os.getenv("NOTION_API_TOKEN")
    database_id = os.getenv("DATABASE_ID")
    return api_key, database_id

# Notionのデータベースのエントリを更新する関数
def update_notion_page(api_key, page_id, is_active_value):
    # セレクトプロパティに基づいて値を設定（Trueなら'Select True', Falseなら'Select False'）
    select_value = 'True' if is_active_value else 'False'
    
    update_url = f"https://api.notion.com/v1/pages/{page_id}"
    update_data = {
        "properties": {
            "is_active": {
                "select": {
                    "name": select_value
                }
            }
        }
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    response = requests.patch(update_url, headers=headers, json=update_data)
    if response.status_code == 200:
        print(f"Successfully updated page {page_id} in Notion.")
    else:
        print(f"Failed to update page {page_id} in Notion. Status Code: {response.status_code}, Response: {response.text}")

# 株価データを取得して、NotionのDBを更新する関数
def fetch_and_update_stock_data(api_key, pkl_file_path):
    # portfolio.pklファイルを読み込む
    df = pd.read_pickle(pkl_file_path)

    # 現在の日付と時刻を取得
    now = datetime.now()
    date_time_str = now.strftime("%Y%m%d-%H%M%S")

    # カレントディレクトリにdataフォルダを作成（存在しなければ）
    data_dir = Path.cwd() / 'data'
    data_dir.mkdir(exist_ok=True)

    # ティッカーシンボルごとに1分足、1日足を取得し、成功/失敗をis_activeに反映する
    for index, row in df.iterrows():
        ticker = row['TickerSymbol']
        page_id = row['ID']  # NotionのUUID
        try:
            # 1分足のデータを取得
            data_1min = yf.download(ticker, interval='1m', period='7d')
            # 1日足のデータを取得
            data_1day = yf.download(ticker, interval='1d', period='1y')

            # データが正しく取得できたかをチェック（データが空でないか）
            if not data_1min.empty and not data_1day.empty:
                # ファイル名のフォーマットを指定
                file_name_1min = data_dir / f"{date_time_str}_{ticker}_1min.csv"
                file_name_1day = data_dir / f"{date_time_str}_{ticker}_1day.csv"

                # 取得したデータをCSVファイルに保存
                data_1min.to_csv(file_name_1min)
                data_1day.to_csv(file_name_1day)

                print(f"Saved {file_name_1min} and {file_name_1day}")
                
                # データ取得が成功した場合、is_activeをTrueに設定
                df.at[index, 'is_active'] = 'True'

                # Notionデータベースにis_activeの結果をアップデート（True）
                update_notion_page(api_key, page_id, True)

            else:
                print(f"Failed to retrieve data for {ticker} (no data).")
                # データが空の場合、is_activeをFalseとして更新
                df.at[index, 'is_active'] = 'False'
                update_notion_page(api_key, page_id, False)
        
        except Exception as e:
            # エラーハンドリング：エラーが発生した場合、Falseとしてis_activeを更新
            print(f"Error retrieving data for {ticker}: {e}")
            df.at[index, 'is_active'] = 'False'
            update_notion_page(api_key, page_id, False)

    # 更新されたデータフレームを再びportfolio.pklに保存
    df.to_pickle(pkl_file_path)

    print("Process completed, and portfolio.pkl has been updated.")

# main関数
def main():
    # 環境変数の読み込み
    api_key, _ = load_environment_variables()

    # pickleファイルのパスを指定
    pkl_file_path = 'portfolio.pkl'

    # 株価データを取得し、NotionのDBを更新
    fetch_and_update_stock_data(api_key, pkl_file_path)

# スクリプトが直接実行された場合のみ、main関数を呼び出す
if __name__ == "__main__":
    main()
