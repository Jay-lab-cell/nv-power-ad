import streamlit as st
import pandas as pd
import uuid
from supabase import create_client

try:
    from httpx import ConnectError as _HttpxConnectError, ConnectTimeout as _HttpxConnectTimeout
    _CONN_ERRORS = (_HttpxConnectError, _HttpxConnectTimeout, OSError)
except Exception:
    _CONN_ERRORS = (OSError,)


def _show_db_error(e):
    msg = str(e) or type(e).__name__
    if not st.session_state.get('_db_error_shown'):
        st.warning(
            "⚠️ Supabase에 연결할 수 없습니다. "
            "Streamlit Cloud의 Secrets에서 `supabase.url`, `supabase.key`를 확인하거나, "
            "Supabase 프로젝트가 일시중지 상태인지 확인해주세요.\n\n"
            f"상세: `{msg[:200]}`"
        )
        st.session_state['_db_error_shown'] = True

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
    try:
        url = st.secrets["supabase"]["url"]
        key = st.secrets["supabase"]["key"]
    except Exception as e:
        _show_db_error(f"Secrets 누락: {e}")
        return None
    try:
        return create_client(url, key)
    except Exception as e:
        _show_db_error(e)
        return None


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
    if sb is None:
        return
    records = _df_to_records(df, user_id, ad_type, memo_dict)

    try:
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
    except Exception as e:
        _show_db_error(e)


def db_load_history(user_id):
    """사용자의 전체 주간 이력 로드."""
    sb = init_supabase()
    if sb is None:
        return None
    try:
        result = sb.table('weekly_history').select('*').eq('user_id', user_id).execute()
    except Exception as e:
        _show_db_error(e)
        return None
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


def db_load_period_conversions(user_id, period, ad_type):
    """특정 기간·광고유형의 과거 저장된 전환 지표를 광고그룹 이름 키로 반환.
    반환: {ad_group_name: {nt_clicks, orders, order_amount, order_amount_14d}}
    """
    sb = init_supabase()
    if sb is None:
        return {}
    try:
        res = sb.table('weekly_history').select(
            'ad_group_name,nt_clicks,orders,order_amount,order_amount_14d'
        ).eq('user_id', user_id).eq('analysis_period', period).eq('ad_type', ad_type).execute()
    except Exception as e:
        _show_db_error(e)
        return {}
    out = {}
    for r in res.data or []:
        name = r.get('ad_group_name')
        if not name:
            continue
        out[name] = {
            'nt_clicks': int(r.get('nt_clicks') or 0),
            'orders': int(r.get('orders') or 0),
            'order_amount': float(r.get('order_amount') or 0),
            'order_amount_14d': float(r.get('order_amount_14d') or 0),
        }
    return out


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
    if sb is None:
        return False
    try:
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
    except Exception as e:
        _show_db_error(e)
    return False


def db_set_memo(user_id, period, keyword, ad_type, memo_str):
    """메모를 직접 설정 (삭제용)."""
    sb = init_supabase()
    if sb is None:
        return False
    try:
        sb.table('weekly_history').update({'memo': memo_str}).eq(
            'user_id', user_id
        ).eq('analysis_period', period).eq('keyword', keyword).eq('ad_type', ad_type).execute()
        return True
    except Exception as e:
        _show_db_error(e)
        return False


# ── 키워드 매핑 CRUD ──

def db_load_keyword_mappings(user_id):
    """사용자의 키워드 매핑 전체 로드.
    반환: {(ad_group_name, ad_type): {'medium': [...], 'keyword': [...]}}
    """
    sb = init_supabase()
    mappings = {}
    if sb is None:
        return mappings
    try:
        result = sb.table('keyword_mappings').select(
            'ad_group_name,ad_type,mapped_nt_keyword,mapped_nt_medium'
        ).eq('user_id', user_id).execute()
    except Exception as e:
        _show_db_error(e)
        return mappings
    for r in result.data:
        kw_raw = r.get('mapped_nt_keyword') or ''
        med_raw = r.get('mapped_nt_medium') or ''
        kw_list = [k.strip() for k in kw_raw.split(',') if k.strip()]
        med_list = [m.strip() for m in med_raw.split(',') if m.strip()]
        mappings[(r['ad_group_name'], r['ad_type'])] = {
            'medium': med_list,
            'keyword': kw_list,
        }
    return mappings


def db_save_keyword_mapping(user_id, ad_group_name, ad_type, medium_list, keyword_list):
    """매핑 1건 저장 (UPSERT). medium_list, keyword_list: 리스트."""
    sb = init_supabase()
    if sb is None:
        return
    try:
        sb.table('keyword_mappings').upsert({
            'user_id': user_id,
            'ad_group_name': ad_group_name,
            'ad_type': ad_type,
            'mapped_nt_medium': ','.join(medium_list) if medium_list else '',
            'mapped_nt_keyword': ','.join(keyword_list) if keyword_list else '',
        }, on_conflict='user_id,ad_group_name,ad_type').execute()
    except Exception as e:
        _show_db_error(e)


def db_delete_keyword_mapping(user_id, ad_group_name, ad_type):
    """키워드 매핑 삭제."""
    sb = init_supabase()
    if sb is None:
        return
    try:
        sb.table('keyword_mappings').delete().eq(
            'user_id', user_id
        ).eq('ad_group_name', ad_group_name).eq('ad_type', ad_type).execute()
    except Exception as e:
        _show_db_error(e)


# ── 사용자 설정 (user_settings) ──

def db_get_setting(user_id, key, default=None):
    sb = init_supabase()
    if sb is None:
        return default
    try:
        r = sb.table('user_settings').select('setting_value').eq(
            'user_id', user_id).eq('setting_key', key).execute()
    except Exception as e:
        _show_db_error(e)
        return default
    if not r.data:
        return default
    return r.data[0].get('setting_value', default)


def db_set_setting(user_id, key, value):
    sb = init_supabase()
    if sb is None:
        return
    try:
        sb.table('user_settings').upsert({
            'user_id': user_id,
            'setting_key': key,
            'setting_value': value,
        }, on_conflict='user_id,setting_key').execute()
    except Exception as e:
        _show_db_error(e)


# ── 숨김 광고그룹 (hidden_adgroups) ──

def db_load_hidden_adgroups(user_id):
    """사용자가 숨김 처리한 광고그룹 ID 집합."""
    sb = init_supabase()
    if sb is None:
        return set()
    try:
        r = sb.table('hidden_adgroups').select('adgroup_id').eq('user_id', user_id).execute()
    except Exception as e:
        _show_db_error(e)
        return set()
    return {row['adgroup_id'] for row in (r.data or [])}


def db_hide_adgroup(user_id, adgroup_id, ad_group_name='', ad_type=''):
    sb = init_supabase()
    if sb is None:
        return
    try:
        sb.table('hidden_adgroups').upsert({
            'user_id': user_id,
            'adgroup_id': adgroup_id,
            'ad_group_name': ad_group_name,
            'ad_type': ad_type,
        }, on_conflict='user_id,adgroup_id').execute()
    except Exception as e:
        _show_db_error(e)


def db_unhide_adgroup(user_id, adgroup_id):
    sb = init_supabase()
    if sb is None:
        return
    try:
        sb.table('hidden_adgroups').delete().eq(
            'user_id', user_id).eq('adgroup_id', adgroup_id).execute()
    except Exception as e:
        _show_db_error(e)


def db_load_hidden_detail(user_id):
    """숨김 리스트 전체 상세 (복원 UI용)."""
    sb = init_supabase()
    if sb is None:
        return []
    try:
        r = sb.table('hidden_adgroups').select(
            'adgroup_id,ad_group_name,ad_type,created_at'
        ).eq('user_id', user_id).order('created_at', desc=True).execute()
    except Exception as e:
        _show_db_error(e)
        return []
    return r.data or []
