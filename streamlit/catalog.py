# 既存のインポートに追加
import streamlit as st
from snowflake.snowpark.context import get_active_session
import plotly.express as px
import re
import ast
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

# ページ設定：幅広レイアウトを使用
st.set_page_config(layout="wide")

# 現在のSnowflakeセッションを取得
session = get_active_session()

# データベース一覧を取得する関数（キャッシュ付き）
@st.cache_data
def get_databases():
    try:
        # データベース一覧をSnowflakeから取得
        databases = session.sql("""
            SELECT database_name
            FROM snowflake.account_usage.databases
            WHERE deleted IS NULL
            ORDER BY database_name
        """).toPandas()
        
        # '<Select>'オプションを先頭に追加
        select_option = pd.DataFrame({'DATABASE_NAME': ['<Select>']})
        return pd.concat([select_option, databases], ignore_index=True)
    except Exception as e:
        st.error(f"データベース一覧の取得中にエラーが発生しました: {str(e)}")
        return pd.DataFrame({'DATABASE_NAME': ['<Select>']})

# 全データベースのテーブルカタログを取得する関数（キャッシュ付き）
@st.cache_data
def get_all_table_catalogs():
    try:
        all_catalogs = []
        # '<Select>'を除外したデータベース一覧を取得
        databases = session.sql("""
            SELECT database_name
            FROM snowflake.account_usage.databases
            WHERE deleted IS NULL
            ORDER BY database_name
        """).toPandas()
        
        for _, row in databases.iterrows():
            database_name = row['DATABASE_NAME']
            # 既存のget_table_catalog関数を使用
            catalog = get_table_catalog(database_name)
            if not catalog.empty:
                all_catalogs.append(catalog)
        
        if all_catalogs:
            return pd.concat(all_catalogs, ignore_index=True)
        return pd.DataFrame()
    except Exception as e:
        st.error(f"テーブルカタログの取得中にエラーが発生しました: {str(e)}")
        return pd.DataFrame()

# 全データベースの利用統計を取得する関数（キャッシュ付き）
@st.cache_data
def get_all_usage_stats():
    try:
        all_stats = []
        # '<Select>'を除外したデータベース一覧を取得
        databases = session.sql("""
            SELECT database_name
            FROM snowflake.account_usage.databases
            WHERE deleted IS NULL
            ORDER BY database_name
        """).toPandas()
        
        for _, row in databases.iterrows():
            database_name = row['DATABASE_NAME']
            # 既存のget_table_usage_stats関数を使用
            stats = get_table_usage_stats(database_name)
            if not stats.empty:
                all_stats.append(stats)
        
        if all_stats:
            return pd.concat(all_stats, ignore_index=True)
        return pd.DataFrame()
    except Exception as e:
        st.error(f"利用統計の取得中にエラーが発生しました: {str(e)}")
        return pd.DataFrame()

# テーブルカタログを取得する関数
def get_table_catalog(databasename):
    try:
        df = session.sql(f"""
            SELECT DISTINCT 
                COMMENT, 
                table_catalog, 
                table_schema, 
                table_name, 
                table_owner, 
                row_count 
            FROM {databasename}.information_schema.tables 
            WHERE table_schema != 'INFORMATION_SCHEMA'
        """)
        return df.toPandas()
    except Exception as e:
        st.error(f"テーブルカタログの取得中にエラーが発生しました: {str(e)}")
        return pd.DataFrame()

# テーブルの統計情報を取得する関数（キャッシュ付き）
@st.cache_data()
def get_table_stats(database_name, schema_name, table_name):
    try:
        stats = session.sql(f"""
            SELECT 
                'Last Updated' as metric,
                TO_CHAR(MAX(LAST_ALTERED), 'YYYY-MM-DD HH24:MI:SS') as value
            FROM {database_name}.information_schema.tables 
            WHERE table_name = '{table_name}'
            AND table_schema = '{schema_name}'
            UNION ALL
            SELECT 
                'Created On',
                TO_CHAR(MIN(CREATED), 'YYYY-MM-DD HH24:MI:SS')
            FROM {database_name}.information_schema.tables 
            WHERE table_name = '{table_name}'
            AND table_schema = '{schema_name}'
            UNION ALL
            SELECT 
                'Storage Size (Bytes)',
                TO_CHAR(SUM(bytes), '999,999,999,999')
            FROM {database_name}.information_schema.tables 
            WHERE table_name = '{table_name}'
            AND table_schema = '{schema_name}'
        """)
        return stats.toPandas()
    except Exception as e:
        st.error(f"テーブル統計の取得中にエラーが発生しました: {str(e)}")
        return pd.DataFrame()

# テーブルの行数を取得する関数（キャッシュ付き）
@st.cache_data()
def get_count(tablename):
    try:
        df = session.sql(f"SELECT COUNT(*) as count_rows FROM {tablename}")
        df = df.toPandas()
        count = df['COUNT_ROWS'].values[0]
        return format(count, ',')
    except Exception as e:
        st.error(f"行数の取得中にエラーが発生しました: {str(e)}")
        return "N/A"

# カラム情報を取得する関数（キャッシュ付き）
@st.cache_data()
def get_column_data(tablename):
    df = session.sql("""
        select 
            TABLE_NAME, 
            COLUMN_NAME, 
            COMMENT 
        from information_schema.columns 
        where table_name = '""" + tablename + "'")
    return df.toPandas()


def get_response(session, prompt):
    # cortex.completeはroleがuserでないと動作しないので注意
    response = session.sql(f'''
    SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet',
        {prompt},
        {{
            'temperature': 0,
            'top_p': 0
        }});
        ''').to_pandas().iloc[0,0]
    # レスポンスを辞書型に変換
    response = ast.literal_eval(response)
    response = response["choices"][0]["messages"]
    return response

def get_table_context(table_name, column_data):
    context = f"""
        テーブル名は<tableName>"{str(table_name)}"</tableName>です。
        SQLのサンプルクエリはこちらです。<サンプルクエリ> select * from {str(table_name)} </サンプルクエリ> 

        また、対象のテーブルが持つ列情報は<columns>"{column_data}"</columns>です。 
    """
    return context

def get_system_prompt(table_name, column_data):
    table_context = get_table_context(table_name=table_name, column_data=column_data)
    return GEN_SQL.format(context=table_context)

GEN_SQL = """
あなたはSnowflake SQL エキスパートとして行動します。質問の回答は日本語でお願いします。
テーブルが与えられるので、テーブル名は <tableName> タグ内にあり、列は <columns> タグ内にあるので確認してください。
テーブルの概要は以下を参考にしてください。

{context}

このテーブルの概要を説明し、このテーブルの各行にあるデータにどのような相関や特徴があるかを説明してください。
また列を確認し利用可能な指標を数行で共有し、箇条書きを使用して分析例を3つを必ず挙げてください。
またなぜその分析例が効果的なのかも詳細に説明し、サンプルのSQLを生成してください。
"""

def get_cosine_similarity():
    """マーケットプレイスとの類似度検索"""
    search_results = session.sql(f"""
        SELECT 
            market.TITLE, 
            market.DESCRIPTION, 
            VECTOR_COSINE_SIMILARITY(catalog.embeddings, market.embeddings) as similarity
        FROM 
            DATA_CATALOG.TABLE_CATALOG.TABLE_CATALOG catalog, 
            DATA_CATALOG.TABLE_CATALOG.MARKETPLACE_EMBEDDING_LISTINGS market
        ORDER BY 
            similarity DESC
        LIMIT 10
        """).collect()
    return search_results


# テーブルの利用統計を取得する関数（キャッシュ付き）
@st.cache_data()
def get_table_usage_stats(database_name):
    """テーブルの詳細な利用統計を取得する関数"""
    try:
        usage_stats = session.sql(f"""
            WITH parsed_objects AS (
                SELECT 
                    query_id,
                    query_start_time,
                    f.value:objectDomain::STRING as obj_domain,
                    f.value:objectName::STRING as obj_name
                FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY,
                TABLE(FLATTEN(direct_objects_accessed)) f
                WHERE QUERY_START_TIME >= DATEADD(month, -3, CURRENT_TIMESTAMP())
                AND f.value:objectName::STRING LIKE '{database_name}.%'
            )
            SELECT 
                TO_VARCHAR(DATE(query_start_time)) as ACCESS_DATE,
                DAYNAME(query_start_time) as DAY_OF_WEEK,
                HOUR(query_start_time) as HOUR_OF_DAY,
                obj_name as TABLE_FULL_NAME,
                COUNT(DISTINCT query_id) as ACCESS_COUNT
            FROM parsed_objects
            WHERE obj_domain = 'Table'
            GROUP BY ACCESS_DATE, DAY_OF_WEEK, HOUR_OF_DAY, TABLE_FULL_NAME
            ORDER BY ACCESS_DATE
        """).toPandas()
        
        # 列名を小文字に統一
        usage_stats.columns = usage_stats.columns.str.lower()
        return usage_stats
    except Exception as e:
        st.error(f"テーブル利用統計の取得中にエラーが発生しました: {str(e)}")
        # エラー時は空のDataFrameを返す
        return pd.DataFrame(columns=[
            'access_date', 'day_of_week', 'hour_of_day', 
            'table_full_name', 'access_count'
        ])

# 利用統計の可視化を行う関数
def display_usage_analytics(usage_stats, table_name=None):
    """利用統計の可視化を行う関数"""
    if usage_stats.empty:
        st.warning("利用統計データがありません")
        return
        
    # 必要なカラムが存在することを確認
    required_columns = ['access_date', 'day_of_week', 'hour_of_day', 'table_full_name', 'access_count']
    missing_columns = [col for col in required_columns if col not in usage_stats.columns]
    
    if missing_columns:
        st.error(f"必要なカラムが見つかりません: {', '.join(missing_columns)}")
        return

    # 特定のテーブルのデータをフィルタリング
    if table_name:
        usage_stats = usage_stats[usage_stats['table_full_name'] == table_name]

    # 利用統計の可視化
    st.subheader("📊 利用統計分析")
    
    col1, col2 = st.columns(2)
    with col1:
        # 日次での利用推移
        daily_usage = usage_stats.groupby('access_date')['access_count'].sum().reset_index()
        fig_daily = px.line(daily_usage, 
                           x='access_date', 
                           y='access_count',
                           title='日次アクセス推移',
                           labels={'access_date': '日付', 'access_count': 'アクセス数'})
        st.plotly_chart(fig_daily, use_container_width=True)

    with col2:
        # 時間帯別の利用傾向
        hourly_usage = usage_stats.groupby('hour_of_day')['access_count'].sum().reset_index()
        fig_hourly = px.bar(hourly_usage, 
                           x='hour_of_day', 
                           y='access_count',
                           title='時間帯別アクセス傾向',
                           labels={'hour_of_day': '時間', 'access_count': 'アクセス数'})
        st.plotly_chart(fig_hourly, use_container_width=True)

    # 利用パターンの分析
    st.subheader("📊 利用パターンの分析")
    col3, col4 = st.columns(2)
    
    with col3:
        # よく利用されるテーブルのランキング
        if not table_name:  # 全体表示の場合のみ表示
            table_ranking = usage_stats.groupby('table_full_name')['access_count'].sum() \
                                     .sort_values(ascending=False).head(10)
            fig_ranking = px.bar(table_ranking,
                                title='よく利用されるテーブル TOP10',
                                labels={'table_full_name': 'テーブル名', 'value': 'アクセス数'})
            st.plotly_chart(fig_ranking, use_container_width=True)

    with col4:
        # 利用パターンの分析とアドバイス
        peak_hour = hourly_usage.loc[hourly_usage['access_count'].idxmax()]
        st.info(f"🕒 最もアクセスが多い時間帯: {int(peak_hour['hour_of_day'])}時 ({int(peak_hour['access_count'])}回)")
        
        # トレンド分析
        recent_trend = daily_usage.tail(7)['access_count'].mean()
        overall_trend = daily_usage['access_count'].mean()
        trend_diff = ((recent_trend - overall_trend) / overall_trend) * 100
        
        if abs(trend_diff) > 10:
            if trend_diff > 0:
                st.success(f"📈 直近1週間のアクセス数が平均より{trend_diff:.1f}%増加しています")
            else:
                st.warning(f"📉 直近1週間のアクセス数が平均より{abs(trend_diff):.1f}%減少しています")

# データ探索のための質問とカテゴリを生成
@st.cache_data
def generate_discovery_questions():
    return [
        {"category": "売上・財務", "question": "売上に関するデータ", "keywords": ["revenue", "sales", "income", "profit", "売上", "収益", "金額", "単価"]},
        {"category": "顧客", "question": "顧客データ", "keywords": ["customer", "client", "user", "顧客", "ユーザー", "会員"]},
        {"category": "製品", "question": "製品情報", "keywords": ["product", "item", "inventory", "製品", "商品", "在庫"]},
        {"category": "マーケティング", "question": "マーケティングデータ", "keywords": ["marketing", "campaign", "advertisement", "広告", "キャンペーン"]},
        {"category": "取引", "question": "取引データ", "keywords": ["transaction", "payment", "order", "取引", "注文", "支払"]},
        {"category": "時系列", "question": "時系列データ", "keywords": ["daily", "monthly", "yearly", "日次", "月次", "年次", "推移"]},
        {"category": "地域", "question": "地域別のデータ", "keywords": ["region", "area", "location", "地域", "都道府県", "市区町村"]},
        {"category": "組織", "question": "組織・部門別のデータ", "keywords": ["department", "division", "organization", "部門", "組織", "部署"]},
    ]



# キーワードに基づいてテーブルをフィルタリング
def filter_tables_by_keywords(table_catalog, keywords):
    filtered_tables = []
    for _, row in table_catalog.iterrows():
        comment = str(row['COMMENT']).lower() if pd.notna(row['COMMENT']) else ""
        table_name = str(row['TABLE_NAME']).lower()
        
        if any(keyword.lower() in comment or keyword.lower() in table_name for keyword in keywords):
            filtered_tables.append(row)
    
    return pd.DataFrame(filtered_tables)

# 人気のテーブルを取得
def get_popular_tables(usage_stats, table_catalog, limit=5):
    if usage_stats.empty:
        return pd.DataFrame()
    
    popular_tables = usage_stats.groupby('table_full_name')['access_count'].sum() \
                              .sort_values(ascending=False) \
                              .head(limit)
    return popular_tables

# おすすめのテーブルを表示
def display_recommended_tables(table_catalog, usage_stats):
    if table_catalog.empty or usage_stats.empty:
        st.info("テーブルの利用統計情報がありません")
        return
        
    st.markdown("### 💡 おすすめのテーブル")
    
    # 人気のテーブル
    popular_tables = get_popular_tables(usage_stats, table_catalog)
    if not popular_tables.empty:
        st.markdown("#### 👥 よく使用されているテーブル")
        for table_name, access_count in popular_tables.items():
            # テーブル名から各部分を抽出
            table_parts = table_name.split('.')
            if len(table_parts) >= 3:
                catalog = table_parts[0]
                schema = table_parts[1]
                name = table_parts[2]
                
                # テーブル情報を検索
                table_match = table_catalog[
                    (table_catalog['TABLE_CATALOG'] == catalog) &
                    (table_catalog['TABLE_SCHEMA'] == schema) &
                    (table_catalog['TABLE_NAME'] == name)
                ]
                
                if not table_match.empty:
                    table_info = table_match.iloc[0]
                    with st.expander(f"**{name}** (アクセス数: {access_count})", expanded=False):
                        # テーブルの説明文がある場合は表示
                        if pd.notna(table_info['COMMENT']):
                            st.write(table_info['COMMENT'])
                        else:
                            st.write("説明文はありません")
                            
                        # メタデータの表示
                        col1, col2 = st.columns(2)
                        with col1:
                            st.metric("スキーマ", table_info['TABLE_SCHEMA'])
                        with col2:
                            if pd.notna(table_info['ROW_COUNT']):
                                st.metric("行数", format(table_info['ROW_COUNT'], ','))
                        
                        # 詳細ボタン
                        if st.button("詳細を見る", key=f"popular_{table_name}"):
                            st.session_state.selected_table = table_name
                else:
                    st.warning(f"テーブル {name} の詳細情報が見つかりませんでした")
            else:
                st.warning(f"テーブル名 {table_name} の形式が正しくありません")

# 新しい関数: テーブル検索機能
def search_tables(table_catalog, search_term):
    if not search_term:
        return table_catalog
    
    search_term = search_term.lower()
    filtered = table_catalog[
        table_catalog['TABLE_NAME'].str.lower().str.contains(search_term) |
        table_catalog['COMMENT'].str.lower().str.contains(search_term, na=False)
    ]
    return filtered

# メインアプリケーション
st.title("Snowflake データカタログ ❄️")
st.subheader(f"ようこそ  :blue[{str(st.experimental_user.user_name)}] さん")

# タブの作成
tab1, tab2 = st.tabs(["📊 特定の DB から詳細を分析", "🔍 キーワードからデータを探す"])

with tab1:
    # 既存のデータベース選択UI
    df_databases = get_databases()
    filter_database = st.selectbox('データベースを選択してください', df_databases['DATABASE_NAME'])
    
    if not '<Select>' in filter_database:
        database_name = filter_database.split(' ')[0].replace('(','').replace(')','')
        
        with st.spinner('テーブルデータを分析中'):
            # 既存のテーブル一覧表示コード...
            # (以下、既存のコードをそのまま維持)
            table_catalog = get_table_catalog(database_name)
            usage_stats = get_table_usage_stats(database_name)

            # 全体の利用統計を表示
            with st.expander("データベース全体の利用統計", expanded=False):
                display_usage_analytics(usage_stats)
            
            # 4列レイアウトでテーブルを表示
            st.header("📑 テーブル一覧")
            col1, col2, col3, col4 = st.columns(4)
            
            for index, row in table_catalog.iterrows():
                current_col = [col1, col2, col3, col4][index % 4]
                with current_col:
                    with st.expander("**"+row['TABLE_NAME']+"**", expanded=True):
                        full_table_name = f"{row['TABLE_CATALOG']}.{row['TABLE_SCHEMA']}.{row['TABLE_NAME']}"
                        
                        st.write(row['COMMENT'])
                        
                        if not usage_stats.empty:
                            table_access = usage_stats[
                                usage_stats['table_full_name'] == full_table_name
                            ]['access_count'].sum()
                            
                            st.markdown(
                                f"""
                                <div style='
                                    background-color: #eef1f6;
                                    padding: 8px 15px;
                                    border-radius: 5px;
                                    margin: 10px 0;
                                    display: inline-block;
                                    border: 1px solid #e0e4eb;
                                '>
                                    <span style='font-size: 0.9em; color: #666;'>👥 過去3ヶ月のアクセス数:</span>
                                    <span style='font-size: 1.1em; font-weight: bold; margin-left: 8px; color: #2c3e50;'>{table_access}</span>
                                </div>
                                """,
                                unsafe_allow_html=True
                            )
                        
                        key_details = full_table_name
                        get_data_details = st.button("詳細", key=key_details, type="primary")
                        
                # 詳細情報の表示
                if get_data_details:
                    st.session_state.messages = []
                    
                    count_rows = get_count(key_details)
                    table_parts = key_details.split('.')
                    database_name = table_parts[0]
                    schema_name = table_parts[1]
                    table_name = table_parts[2]

                    with st.expander(str(key_details) + " の概要", expanded=True):
                        st.success("レコード数 : " + str(count_rows))

                        st.info("📊 テーブル統計情報")
                        stats_df = get_table_stats(database_name, schema_name, table_name)
                        if not stats_df.empty:
                            st.dataframe(stats_df, use_container_width=True)

                        st.info("テーブル内のカラム名と説明")
                        sql = session.sql(f"select * from {key_details} limit 10")
                        st.dataframe(sql, use_container_width=True)

                    with st.expander("LLMを使ったテーブルの詳細分析"):
                        column_data = get_column_data(table_name)
                        prompt = get_system_prompt(table_name, column_data)
                        st.session_state.messages.append({"role": 'user', "content": prompt})

                        response = get_response(session, st.session_state.messages)
                        st.session_state.messages.append({"role": "assistant", "content": response})
                        st.markdown(response)

                    with st.expander("マーケットプレイスで役立ちそうなデータ上位10件"):
                        results = get_cosine_similarity()
                        st.dataframe(results, use_container_width=True)

with tab2:
    st.markdown("### キーワードからデータを探す")
    
    # 検索のヒント表示
    with st.expander("検索のヒント", expanded=False):
        st.markdown("""
        #### 効果的な検索のコツ
        - **具体的なキーワード**: 「売上」「顧客」など具体的な単語で検索
        - **複数のキーワード**: スペース区切りで複数のキーワードを入力可能
        - **日本語/英語**: 日本語と英語どちらでも検索可能
        
        #### よく使われる検索キーワード例
        - 売上関連: revenue, sales, 売上, 収益
        - 顧客関連: customer, user, 顧客, ユーザー
        - 製品関連: product, item, 製品, 商品
        - 期間関連: daily, monthly, 日次, 月次
        """)
    
    # 検索バー
    search_term = st.text_input("キーワードで検索", placeholder="テーブル名や説明文で検索...")
    
    # データ探索のための質問
    st.markdown("### どんなデータをお探しですか？")
    questions = generate_discovery_questions()
    selected_purposes = []
    
    cols = st.columns(3)
    for i, q in enumerate(questions):
        with cols[i % 3]:
            if st.checkbox(q["question"]):
                selected_purposes.extend(q["keywords"])
    
    with st.spinner('テーブルデータを分析中...'):
        # 全データベースからテーブル情報を取得
        table_catalog = get_all_table_catalogs()
        usage_stats = get_all_usage_stats()
        
        # 検索結果とフィルタリング結果の統合
        filtered_catalog = table_catalog
        if search_term:
            filtered_catalog = search_tables(filtered_catalog, search_term)
        if selected_purposes:
            filtered_catalog = filter_tables_by_keywords(filtered_catalog, selected_purposes)
        
        if len(filtered_catalog) > 0:
            st.markdown(f"### 検索結果: {len(filtered_catalog)}件のテーブルが見つかりました")
            
            # テーブル一覧の表示
            for _, row in filtered_catalog.iterrows():
                with st.expander(f"**{row['TABLE_CATALOG']}.{row['TABLE_SCHEMA']}.{row['TABLE_NAME']}**", expanded=False):
                    st.write(row['COMMENT'])
                    full_table_name = f"{row['TABLE_CATALOG']}.{row['TABLE_SCHEMA']}.{row['TABLE_NAME']}"
                    
                    if not usage_stats.empty:
                        table_access = usage_stats[
                            usage_stats['table_full_name'] == full_table_name
                        ]['access_count'].sum()
                        st.metric("👥 過去3ヶ月のアクセス数", table_access)
                    
        else:
            st.info("条件に一致するテーブルが見つかりませんでした。")
        
        # おすすめのテーブル表示
        if not search_term and not selected_purposes:
            display_recommended_tables(table_catalog, usage_stats)