# lsc-check-reservation-count

## 事前準備

* python3.0以上をインストールしてください。
* 当スクリプトを実行するにあたり以下の情報が必要です。
    * SurveyCalendarsテーブルのテーブル名
    * SurveyResultsテーブルのテーブル名
    * 予約数をチェックしたいカレンダーのcategoryId
    * AWSのプロファイル名
* 上記情報の確認方法は下記を参考にしてください。
    * SurveyResults・SurveyCalendarsテーブルのテーブル名：以下のコマンドを実行して確認ができます。
    ```sh
    aws dynamodb list-tables --profile プロファイル名
    ```

    * categoryId：LSCの管理画面でクリップボードにコピーすることができます。
    ![categoryId](https://user-images.githubusercontent.com/63500101/124404198-d38b5200-dd74-11eb-8181-a07fbbb9156b.png)

    * AWSのプロファイル名：以下のファイルで確認ができます。
    ```sh
    ホームディレクトリ/.aws/credentials
    ```

## 実行

aws_check_reservation.pyがあるディレクトリにcdコマンドで移動します。
その後以下のコマンドを実行してください。

```sh
python aws_check_reservation.py --SurveyCalendars [SurveyCalendarsのテーブル名] --SurveyResults [SurveyResultsのテーブル名] --categoryId [categoryId] --profile [AWSのprofile名]
```