import streamlit as st
import pandas as pd
import uuid
from supabase import create_client

# ── 한글 ↔ DB 컬럼 매핑 ──
COL_KR_TO_DB = {
    '분석 기간': 'analysis_period',
    '광고그룹 이름': 'ad_group_name',
    'keyword': 'keyword',
    '총비용': 'total_cost',
    '평균CPC': 'avg_cpc',
    '클릭수': 'clicks',
    'nt 클릭수': 'nt_clicks',
    '결제수': 'orders',
    '결제금액': 'order_amount',
    '결제금액(+14일기여도추정)': 'order_amount_14d',
    '전환율(%)': 'conversion_rate',
    'ROAS(%)': 'roas',
    'ROAS_14일(%)': 'roas_14d',
    '유형': 'ad_type',
    '메모': 'memo',
}
COL_DB_TO_KR = {v: k for k, v in COL_KR_TO_DB.items()}


@st.cache_resource(ttl=3600)
def init_supabase():
    url = st.secrets["supabase"]["url"]
    key = st.secrets["supabase"]["key"]
    return create_client(url, key)


def get_user_id():
    params = st.query_params
    if "uid" not in params:
        new_uid = str(uuid.uuid4())[:8]
        st.query_params["uid"] = new_uid
    return params["uid"]


def _df_to_records(df, user_id, ad_type, memo_dict=None):
    """DataFrame을 Supabase insert용 dict 리스트로 변환."""
    records = []
    seen_keys = set()
    for _, r in df.iterrows():
        kw = str(r.get('keyword', '')) if pd.notna(r.get('keyword')) else ''
        # keyword가 비어있으면 광고그룹 이름을 keyword로 사용
        if not kw.strip():
            kw = str(r.get('광고그룹 이름', ''))
        period = str(r.get('분석 기간', ''))
        # 중복 키 방지
        ukey = (period, kw, ad_type)
        if ukey in seen_keys:
            continue
        seen_keys.add(ukey)
        rec = {
            'user_id': user_id,
            'analysis_period': period,
            'ad_group_name': str(r.get('광고그룹 이름', '')),
            'keyword': kw,
            'total_cost': int(r.get('총비용', 0)),
            'avg_cpc': int(r.get('평균CPC', 0)),
            'clicks': int(r.get('클릭수', 0)),
            'nt_clicks': int(r.get('nt 클릭수', 0)),
            'orders': int(r.get('결제수', 0)),
            'order_amount': int(r.get('결제금액', 0)),
            'order_amount_14d': float(r.get('결제금액(+14일기여도추정)', 0)),
            'conversion_rate': float(r.get('전환율(%)', 0)),
            'roas': float(r.get('ROAS(%)', 0)),
            'roas_14d': float(r.get('ROAS_14일(%)', 0)),
            'ad_type': ad_type,
            'memo': '',
        }
        if memo_dict:
            key = (rec['analysis_period'], rec['keyword'])
            rec['memo'] = memo_dict.get(key, '')
        records.append(rec)
    return records


def _rows_to_df(rows):
    """Supabase 응답을 한글 컬럼명 DataFrame으로 변환."""
    if not rows:
        return None
    df = pd.DataFrame(rows)
    # DB 컬럼 → 한글 컬럼 이름 변환
    rename = {k: v for k, v in COL_DB_TO_KR.items() if k in df.columns}
    df = df.rename(columns=rename)
    # 불필요 컬럼 제거
    for col in ['id', 'user_id', 'created_at']:
        if col in df.columns:
            df = df.drop(columns=[col])
    return df


# ── 데이터 CRUD ──

def db_save_weekly(user_id, df, ad_type, memo_dict=None):
    """주간 데이터 저장 (UPSERT)."""
    sb = init_supabase()
    records = _df_to_records(df, user_id, ad_type, memo_dict)

    # 기존 메모 보존: memo_dict가 없으면 기존 DB의 메모를 유지
    if memo_dict is None:
        period = records[0]['analysis_period'] if records else ''
        existing = sb.table('weekly_history').select('keyword,memo').eq(
            'user_id', user_id
        ).eq('analysis_period', period).eq('ad_type', ad_type).execute()
        old_memos = {r['keyword']: r['memo'] for r in existing.data if r.get('memo')}
        for rec in records:
            if rec['keyword'] in old_memos:
                rec['memo'] = old_memos[rec['keyword']]

    sb.table('weekly_history').upsert(
        records, on_conflict='user_id,analysis_period,keyword,ad_type'
    ).execute()


def db_load_history(user_id):
    """사용자의 전체 주간 이력 로드."""
    sb = init_supabase()
    result = sb.table('weekly_history').select('*').eq('user_id', user_id).execute()
    df = _rows_to_df(result.data)
    if df is not None:
        # 호환성 처리
        if 'nt 클릭수' not in df.columns:
            df['nt 클릭수'] = 0
        if '전환율(%)' not in df.columns:
            df['전환율(%)'] = df.apply(
                lambda r: (r['결제수'] / r['클릭수'] * 100) if r['클릭수'] > 0 else 0, axis=1
            )
        if '메모' not in df.columns:
            df['메모'] = ''
        df['메모'] = df['메모'].fillna('')
    return df


def db_update_memo(user_id, period, keyword, ad_type, new_memo_text):
    """기존 메모에 새 메모 추가."""
    from datetime import datetime
    MEMO_SEP = "||"

    def _append(existing, text):
        ts = datetime.now().strftime("%m/%d %H:%M")
        entry = f"[{ts}] {text}"
        parts = [m.strip() for m in str(existing).split(MEMO_SEP) if m.strip()] if existing and str(existing).strip() else []
        parts.append(entry)
        return MEMO_SEP.join(parts)

    sb = init_supabase()
    result = sb.table('weekly_history').select('memo').eq(
        'user_id', user_id
    ).eq('analysis_period', period).eq('keyword', keyword).eq('ad_type', ad_type).execute()
    if result.data:
        old_memo = result.data[0].get('memo', '')
        new_val = _append(old_memo, new_memo_text)
        sb.table('weekly_history').update({'memo': new_val}).eq(
            'user_id', user_id
        ).eq('analysis_period', period).eq('keyword', keyword).eq('ad_type', ad_type).execute()
        return True
    return False


def db_set_memo(user_id, period, keyword, ad_type, memo_str):
    """메모를 직접 설정 (삭제용)."""
    sb = init_supabase()
    sb.table('weekly_history').update({'memo': memo_str}).eq(
        'user_id', user_id
    ).eq('analysis_period', period).eq('keyword', keyword).eq('ad_type', ad_type).execute()
    return True


# ── 키워드 매핑 CRUD ──

def db_load_keyword_mappings(user_id):
    """사용자의 키워드 매핑 전체 로드. {(ad_group_name, ad_type): mapped_nt_keyword}"""
    sb = init_supabase()
    result = sb.table('keyword_mappings').select(
        'ad_group_name,ad_type,mapped_nt_keyword'
    ).eq('user_id', user_id).execute()
    return {(r['ad_group_name'], r['ad_type']): r['mapped_nt_keyword'] for r in result.data}


def db_save_keyword_mapping(user_id, ad_group_name, ad_type, mapped_nt_keyword):
    """키워드 매핑 1건 저장 (UPSERT)."""
    sb = init_supabase()
    sb.table('keyword_mappings').upsert({
        'user_id': user_id,
        'ad_group_name': ad_group_name,
        'ad_type': ad_type,
        'mapped_nt_keyword': mapped_nt_keyword,
    }, on_conflict='user_id,ad_group_name,ad_type').execute()


def db_delete_keyword_mapping(user_id, ad_group_name, ad_type):
    """키워드 매핑 삭제."""
    sb = init_supabase()
    sb.table('keyword_mappings').delete().eq(
        'user_id', user_id
    ).eq('ad_group_name', ad_group_name).eq('ad_type', ad_type).execute()
