import pandas as pd
import argparse

def main():
    # argparseでコマンドライン引数を処理
    parser = argparse.ArgumentParser(description='Load and display a DataFrame from a pickle file.')
    parser.add_argument('filename', type=str, help='The path to the pickle file.')

    # 引数のパース
    args = parser.parse_args()

    # データフレームの読み込み
    try:
        df = pd.read_pickle(args.filename)
        print("DataFrame loaded successfully!")
        print(df.to_string(justify='left'))
    except FileNotFoundError:
        print(f"Error: The file '{args.filename}' was not found.")
    except Exception as e:
        print(f"Error: An error occurred while loading the file: {e}")

if __name__ == "__main__":
    main()

