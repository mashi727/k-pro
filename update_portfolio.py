import requests
import pandas as pd



import requests
import pandas as pd

def fetch_notion_data(api_key, database_id):
    # デバッグ用にdatabase_idの内容を出力
    if not database_id:
        raise ValueError("Error: 'database_id' is None or empty. Please provide a valid database_id.")

    # APIのエンドポイントとヘッダーの設定
    url = f'https://api.notion.com/v1/databases/{database_id}/query'
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }

    results_list = []
    has_more = True
    next_cursor = None

    while has_more:
        # ペイロードの設定
        payload = {"page_size": 100}
        if next_cursor:
            payload['start_cursor'] = next_cursor

        # Notion APIからデータベースを取得
        response = requests.post(url, headers=headers, json=payload)

        # レスポンスのチェック
        if response.status_code != 200:
            print(f"Error: {response.status_code}, {response.text}")
            return pd.DataFrame()  # 空のDataFrameを返す

        data = response.json()

        # resultsがNoneでないか確認
        if data.get('results') is None:
            print(f"Error: 'results' field is missing in the response. Full response: {data}")
            return pd.DataFrame()

        # JSONから必要な情報を抽出
        for item in data.get('results', []):
            entry = {}
            # 各エントリのユニークIDを取得
            entry['ID'] = item.get('id', None)  # Notionの各エントリのユニークID
            
            # propertiesがNoneでないか確認
            properties = item.get('properties', {})
            if properties is None:
                print(f"Warning: 'properties' field is None for item: {item}")
                continue

            # プロパティの値を抽出
            for prop, prop_data in properties.items():
                if prop_data is None:
                    entry[prop] = None
                    continue

                prop_type = prop_data.get('type')
                if prop_type == 'title':
                    entry[prop] = ''.join([text.get('text', {}).get('content', '') for text in prop_data.get('title', [])])
                elif prop_type == 'rich_text':
                    entry[prop] = ''.join([text.get('text', {}).get('content', '') for text in prop_data.get('rich_text', [])])
                elif prop_type == 'select':
                    # selectプロパティの安全な取得
                    select_data = prop_data.get('select')
                    if select_data and isinstance(select_data, dict):
                        entry[prop] = select_data.get('name', None)
                    else:
                        entry[prop] = None
                elif prop_type == 'multi_select':
                    entry[prop] = ', '.join([sel.get('name', '') for sel in prop_data.get('multi_select', [])])
                elif prop_type == 'number':
                    entry[prop] = prop_data.get('number', None)
                elif prop_type == 'checkbox':
                    entry[prop] = prop_data.get('checkbox', None)
                elif prop_type == 'date':
                    entry[prop] = prop_data.get('date', {}).get('start', None)
                elif prop_type == 'email':
                    entry[prop] = prop_data.get('email', None)
                elif prop_type == 'phone_number':
                    entry[prop] = prop_data.get('phone_number', None)
                elif prop_type == 'url':
                    entry[prop] = prop_data.get('url', None)
                elif prop_type == 'created_time':
                    entry[prop] = prop_data.get('created_time', None)
                elif prop_type == 'last_edited_time':
                    entry[prop] = prop_data.get('last_edited_time', None)
                else:
                    entry[prop] = None  # 必要に応じて他のプロパティタイプを追加

            results_list.append(entry)

        # ページング処理
        has_more = data.get('has_more', False)
        next_cursor = data.get('next_cursor', None)

    # リストからDataFrameを作成
    results_df = pd.DataFrame(results_list)
    return results_df

def main():
    # 使用例
    # Notion APIキーfrom dotenv import load_dotenv
    import os    
    from dotenv import load_dotenv
    load_dotenv()

    api_key = os.getenv("NOTION_API_TOKEN")
    database_id = os.getenv("DATABASE_ID")
    # データ取得
    data_frame = fetch_notion_data(api_key, database_id)

    # 結果の表示
    print(data_frame.to_string(justify='left'))

    # DataFrameをpickle形式で保存
    filename = 'portfolio.pkl'
    data_frame.to_pickle(filename)

if __name__ == "__main__":
    main()
