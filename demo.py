import streamlit as st
import duckdb

@st.cache_resource
def get_connection():
    connection = duckdb.connect(":memory:") #, read_only=True)

    initial_sql = f"""
    INSTALL iceberg;
    LOAD iceberg;
    CREATE SECRET (
        TYPE S3,
        KEY_ID '{st.secrets.s3.access_key_id}',
        SECRET '{st.secrets.s3.secret}',
        REGION '{st.secrets.s3.region}'
    );
    """

    connection.execute(initial_sql)

    return connection

def convert_s3_path(original_path: str) -> str:
    # パスの最後のmetadataディレクトリ以降の部分を抽出
    parts = original_path.split('/')
    metadata_index = parts.index('metadata')
    result_path = 's3://.../' + '/'.join(parts[metadata_index:])
    return result_path

@st.cache_data
def get_metadata_list(_connection):
    metadata_list = _connection.execute(f"SELECT * FROM glob('{st.secrets.s3.glob_path}');")
    metadata_df = metadata_list.fetchdf()
    # メタデータファイルの一部をマスクする
    metadata_df["masked_path"] = metadata_df["file"].apply(convert_s3_path)
    return metadata_df 

@st.cache_data
def query_data(_connection, metadata_path: str):
    result = _connection.execute(f"SELECT COUNT(*) AS CNT FROM iceberg_scan('{metadata_path}');")
    result_df = result.fetchdf()
    cnt = result_df.iloc[0]["CNT"]
    result = _connection.execute(f"SELECT * FROM iceberg_scan('{metadata_path}') LIMIT 100;")
    return cnt, result.fetchdf()

connection = get_connection()

st.title("Apache Iceberg Japan Meetup :snowflake:")

if st.button("Reload metadata :material/refresh:"):
    get_metadata_list.clear()

# メタデータファイル一覧を取得
metadata_df = get_metadata_list(connection)

# 表示するメタデータファイル一覧はマスクしたもののみとする
show_df = metadata_df[["masked_path"]]
selection = st.dataframe(show_df, key="selected_metadata", on_select="rerun", selection_mode=["single-row"])
selected_metadata = None
if len(selection["selection"]["rows"]) > 0:
    idx = selection["selection"]["rows"][0]
    selected_metadata = metadata_df.iloc[idx]["file"]
else:
    st.stop()

st.write(f"Selected metadata file: {convert_s3_path(selected_metadata)}")

cnt, result_df = query_data(connection, selected_metadata)
st.metric("行数", cnt)
st.dataframe(result_df)
