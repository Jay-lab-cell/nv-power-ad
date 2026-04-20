import streamlit as st
import pandas as pd
import altair as alt
import re
from datetime import date, timedelta, datetime

from db import (get_user_id, db_save_weekly, db_load_history, db_update_memo, db_set_memo,
                db_load_keyword_mappings, db_save_keyword_mapping, db_delete_keyword_mapping,
                db_load_period_conversions, db_get_setting, db_set_setting,
                db_load_hidden_adgroups, db_hide_adgroup, db_unhide_adgroup, db_load_hidden_detail)
from naver_sa_api import fetch_ad_data, list_campaigns

st.set_page_config(page_title="네이버 광고 ROAS 분석", layout="wide", initial_sidebar_state="expanded")
st.title("네이버 파워링크 & 파워컨텐츠 ROAS 분석")

# ── 사용자 식별 ──
user_id = get_user_id()
# 사용자 전환 감지 → API 관련 캐시 무효화
if st.session_state.get('_last_uid') != user_id:
    for k in ('api_campaigns_df', 'api_ad_cache'):
        st.session_state.pop(k, None)
    st.session_state['_last_uid'] = user_id
with st.sidebar:
    st.caption(f"👤 현재 사용자: `{user_id}`")
    switch_uid = st.text_input("사용자 전환", placeholder="예: admin", key="switch_uid_input")
    if st.button("전환", key="switch_uid_btn") and switch_uid.strip():
        st.query_params["uid"] = switch_uid.strip()
        for k in ('api_campaigns_df', 'api_ad_cache'):
            st.session_state.pop(k, None)
        st.rerun()

# ── Sticky 헤더 CSS ──
st.markdown("""
<style>
    div[data-testid="stVerticalBlockBorderWrapper"] {
        position: sticky;
        top: 3.5rem;
        z-index: 999;
        background-color: white;
    }
</style>
""", unsafe_allow_html=True)

MEMO_SEP = "||"


# ──────────────────── 메모 유틸 함수 ────────────────────

def get_memo_list(memo_val):
    """메모 문자열 → 리스트. 빈 값이면 빈 리스트."""
    if pd.isna(memo_val) or str(memo_val).strip() == '':
        return []
    return [m.strip() for m in str(memo_val).split(MEMO_SEP) if m.strip()]


def get_memo_count(memo_val):
    return len(get_memo_list(memo_val))


def format_memo_count(memo_val):
    """표시용: 메모 없으면 '', 있으면 '+N'"""
    cnt = get_memo_count(memo_val)
    return f"+{cnt}" if cnt > 0 else ""


def append_memo(existing_memo, new_text):
    """기존 메모 문자열에 새 메모 추가. 타임스탬프 포함."""
    ts = datetime.now().strftime("%m/%d %H:%M")
    entry = f"[{ts}] {new_text}"
    memos = get_memo_list(existing_memo)
    memos.append(entry)
    return MEMO_SEP.join(memos)


def delete_memo(existing_memo, index):
    """메모 리스트에서 index번째 메모 삭제. 새 메모 문자열 반환."""
    memos = get_memo_list(existing_memo)
    if 0 <= index < len(memos):
        memos.pop(index)
    return MEMO_SEP.join(memos) if memos else ''


def add_memo_column(df, memo_source, period_col='분석 기간', keyword_col='keyword'):
    """DataFrame에 메모 열 추가. memo_source는 {(period,keyword): memo_str} dict."""
    df = df.copy()
    df['메모'] = df.apply(
        lambda r: format_memo_count(memo_source.get((r[period_col], r[keyword_col]), '')),
        axis=1
    )
    return df


def build_memo_dict(df):
    """DataFrame에서 (분석 기간, keyword) → 메모 문자열 dict 구성."""
    memo_dict = {}
    if '메모' in df.columns:
        for _, r in df.iterrows():
            key = (r['분석 기간'], r['keyword'])
            if pd.notna(r['메모']) and str(r['메모']).strip():
                memo_dict[key] = str(r['메모'])
    return memo_dict


# ──────────────────── 유틸 함수 ────────────────────

def extract_keyword(name):
    match = re.search(r'\(([^)]+)\)', str(name))
    if match:
        kw = match.group(1)
        if kw.startswith('pl_'):
            kw = kw[3:]
        return kw
    if '_' in str(name):
        kw = str(name).split('_', 1)[1]
        kw = kw.split(' - ')[0].strip()
        return kw
    return None


def clean_cost(val):
    if pd.isna(val):
        return 0
    s = str(val).replace(',', '').replace('원', '').strip()
    try:
        return int(s)
    except ValueError:
        return 0


def _find_col(df, candidates):
    """컬럼명 자동 탐색: candidates 키워드가 포함된 컬럼 반환."""
    for c in df.columns:
        for kw in candidates:
            if kw in str(c):
                return c
    return None


def process_ad_data(ad_df, keyword_mappings=None):
    # '상태' 컬럼이 있으면 필터, 없으면 전체 사용
    if '상태' in ad_df.columns:
        ad_df = ad_df[ad_df['상태'].notna()].copy()
    else:
        ad_df = ad_df.copy()
    # 총비용 컬럼 자동 탐색
    cost_col = _find_col(ad_df, ['총비용'])
    if cost_col and cost_col != '총비용':
        ad_df['총비용'] = ad_df[cost_col].apply(clean_cost)
    elif cost_col:
        ad_df['총비용'] = ad_df['총비용'].apply(clean_cost)
    else:
        ad_df['총비용'] = 0
    # 클릭수 컬럼 자동 탐색
    if '클릭수' not in ad_df.columns:
        click_col = _find_col(ad_df, ['클릭수', '클릭'])
        if click_col:
            ad_df['클릭수'] = ad_df[click_col].apply(lambda x: int(str(x).replace(',', '')) if pd.notna(x) else 0)
        else:
            ad_df['클릭수'] = 0
    ad_df['keyword'] = ad_df['광고그룹 이름'].apply(extract_keyword)
    # 수동 매핑 적용 (저장된 매핑이 자동 추출보다 우선)
    if keyword_mappings:
        ad_df['keyword'] = ad_df.apply(
            lambda r: keyword_mappings.get(r['광고그룹 이름'], r['keyword']),
            axis=1
        )
    return ad_df


def find_unmatched(ad_df, conv_grouped):
    """전환 데이터와 매칭되지 않는 광고그룹 찾기 (총비용 > 0인 행만)."""
    if conv_grouped is None or len(conv_grouped) == 0:
        return pd.DataFrame()
    available_nt = set(conv_grouped['nt_keyword'].dropna().unique())
    unmatched = ad_df[
        (~ad_df['keyword'].isin(available_nt)) & (ad_df['총비용'] > 0)
    ].copy()
    return unmatched[['광고그룹 이름', 'keyword', '총비용']].drop_duplicates(subset='광고그룹 이름')


def process_conversion(conv_df, medium, match_by='medium'):
    if match_by == 'medium':
        filtered = conv_df[conv_df['nt_medium'] == medium].copy()
        if len(filtered) == 0:
            filtered = conv_df.copy()
    else:
        filtered = conv_df.copy()
    agg_dict = {
        '결제수': 'sum',
        '결제금액': 'sum',
        '결제금액(+14일기여도추정)': 'sum',
    }
    if '유입수' in filtered.columns:
        agg_dict['유입수'] = 'sum'
    grouped = filtered.groupby('nt_keyword', as_index=False).agg(agg_dict)
    if '유입수' in grouped.columns:
        grouped.rename(columns={'유입수': 'nt 클릭수'}, inplace=True)
    else:
        grouped['nt 클릭수'] = 0
    return grouped


def build_conv_by_mapping(conv_df, ad_df, all_mappings, ad_type_label):
    """매핑 기반 전환 데이터 합산.
    all_mappings: {(ag_name, ad_type): {'medium': [...], 'keyword': [...]}}
    medium + keyword 동시 필터 지원. 각 광고그룹별로 독립 필터 적용.
    반환: merge_and_calc에 사용할 conv_grouped (nt_keyword 열 기준)
    """
    EMPTY = pd.DataFrame(columns=['nt_keyword', '결제수', '결제금액', '결제금액(+14일기여도추정)', 'nt 클릭수'])
    if conv_df is None or len(conv_df) == 0:
        return EMPTY

    agg_cols = ['결제수', '결제금액', '결제금액(+14일기여도추정)']
    has_inflow = '유입수' in conv_df.columns

    rows = []
    for _, ad_row in ad_df.drop_duplicates(subset='광고그룹 이름').iterrows():
        ag_name = ad_row['광고그룹 이름']
        kw = ad_row['keyword']
        mapping = all_mappings.get((ag_name, ad_type_label), {})
        med_list = mapping.get('medium', [])
        kw_list = mapping.get('keyword', [])
        if not med_list and not kw_list:
            continue  # 매핑 없으면 기본 동작에 맡김
        filtered = conv_df.copy()
        if med_list:
            filtered = filtered[filtered['nt_medium'].isin(med_list)]
        if kw_list:
            filtered = filtered[filtered['nt_keyword'].isin(kw_list)]
        if len(filtered) == 0:
            continue
        row = {'nt_keyword': kw}
        for col in agg_cols:
            row[col] = filtered[col].sum() if col in filtered.columns else 0
        row['nt 클릭수'] = filtered['유입수'].sum() if has_inflow else 0
        rows.append(row)

    return pd.DataFrame(rows) if rows else EMPTY


def build_conv_grouped(conv_df, ad_df, all_mappings, ad_type_label, default_medium):
    """전환 데이터 집계: 매핑이 있는 광고그룹은 개별 필터 적용, 나머지는 기본 medium으로 처리."""
    EMPTY = pd.DataFrame(columns=['nt_keyword', '결제수', '결제금액', '결제금액(+14일기여도추정)', 'nt 클릭수'])
    if conv_df is None:
        return EMPTY

    # 매핑 있는 광고그룹 → 개별 필터로 집계
    mapped_rows = build_conv_by_mapping(conv_df, ad_df, all_mappings, ad_type_label)
    mapped_kws = set(mapped_rows['nt_keyword'].tolist()) if len(mapped_rows) > 0 else set()

    # 매핑 없는 광고그룹 → 기본 medium으로 처리
    unmapped_ad_groups = set(ad_df['광고그룹 이름'].unique()) - {
        ag for (ag, at) in all_mappings if at == ad_type_label
        and (all_mappings[(ag, at)].get('medium') or all_mappings[(ag, at)].get('keyword'))
    }
    unmapped_kws = set(ad_df[ad_df['광고그룹 이름'].isin(unmapped_ad_groups)]['keyword'].dropna())

    default_conv = process_conversion(conv_df, default_medium)
    # 이미 수동 매핑된 keyword와 겹치면 제거
    if len(default_conv) > 0 and mapped_kws:
        default_conv = default_conv[~default_conv['nt_keyword'].isin(mapped_kws)]

    if len(mapped_rows) > 0 and len(default_conv) > 0:
        return pd.concat([mapped_rows, default_conv], ignore_index=True)
    elif len(mapped_rows) > 0:
        return mapped_rows
    else:
        return default_conv


def apply_saved_conversions(result_df, saved_map):
    """result_df의 결제수·결제금액이 0인 행을 DB 저장값으로 보강.
    saved_map: {ad_group_name: {nt_clicks, orders, order_amount, order_amount_14d}}
    ROAS/전환율은 재계산.
    """
    if result_df is None or len(result_df) == 0 or not saved_map:
        return result_df
    df = result_df.copy()
    for i, row in df.iterrows():
        ag = row['광고그룹 이름']
        saved = saved_map.get(ag)
        if not saved:
            continue
        # 기존 값이 있으면(엑셀 우선) 건너뜀
        if row['결제수'] > 0 or row['결제금액'] > 0:
            continue
        df.at[i, 'nt 클릭수'] = saved['nt_clicks']
        df.at[i, '결제수'] = saved['orders']
        df.at[i, '결제금액'] = saved['order_amount']
        df.at[i, '결제금액(+14일기여도추정)'] = saved['order_amount_14d']
        clicks = row['클릭수']
        cost = row['총비용']
        df.at[i, '전환율(%)'] = (saved['orders'] / clicks * 100) if clicks > 0 else 0
        df.at[i, 'ROAS(%)'] = (saved['order_amount'] / cost * 100) if cost > 0 else 0
        df.at[i, 'ROAS_14일(%)'] = (saved['order_amount_14d'] / cost * 100) if cost > 0 else 0
    return df


def merge_and_calc(ad_df, conv_grouped, period_str):
    merged = ad_df.merge(conv_grouped, left_on='keyword', right_on='nt_keyword', how='left')
    for col in ['결제수', '결제금액', '결제금액(+14일기여도추정)', 'nt 클릭수']:
        merged[col] = merged[col].fillna(0)

    merged['평균CPC'] = merged.apply(
        lambda r: int(r['총비용'] / r['클릭수']) if r['클릭수'] > 0 else 0, axis=1
    )
    merged['전환율(%)'] = merged.apply(
        lambda r: (r['결제수'] / r['클릭수'] * 100) if r['클릭수'] > 0 else 0, axis=1
    )
    merged['ROAS(%)'] = merged.apply(
        lambda r: (r['결제금액'] / r['총비용'] * 100) if r['총비용'] > 0 else 0, axis=1
    )
    merged['ROAS_14일(%)'] = merged.apply(
        lambda r: (r['결제금액(+14일기여도추정)'] / r['총비용'] * 100) if r['총비용'] > 0 else 0, axis=1
    )
    merged['분석 기간'] = period_str
    merged = merged.sort_values('총비용', ascending=False).reset_index(drop=True)

    keep_cols = ['분석 기간', '광고그룹 이름', 'keyword', '총비용', '평균CPC',
                 '클릭수', 'nt 클릭수', '결제수', '결제금액', '결제금액(+14일기여도추정)',
                 '전환율(%)', 'ROAS(%)', 'ROAS_14일(%)']
    if '광고그룹 ID' in merged.columns:
        keep_cols.insert(2, '광고그룹 ID')
    result = merged[keep_cols].copy()
    return result


def format_result(df):
    formatted = df.copy()
    formatted['총비용'] = formatted['총비용'].apply(lambda x: f"{int(x):,}")
    formatted['클릭수'] = formatted['클릭수'].apply(lambda x: f"{int(x):,}")
    formatted['평균CPC'] = formatted['평균CPC'].apply(lambda x: f"{int(x):,}원")
    formatted['nt 클릭수'] = formatted['nt 클릭수'].apply(lambda x: f"{int(x):,}")
    formatted['결제수'] = formatted['결제수'].apply(lambda x: f"{int(x):,}")
    formatted['결제금액'] = formatted['결제금액'].apply(lambda x: f"{int(x):,}")
    formatted['결제금액(+14일기여도추정)'] = formatted['결제금액(+14일기여도추정)'].apply(lambda x: f"{int(x):,}")
    formatted['전환율(%)'] = formatted['전환율(%)'].apply(lambda x: f"{x:.2f}%")
    formatted['ROAS(%)'] = formatted['ROAS(%)'].apply(lambda x: f"{x:.2f}%")
    formatted['ROAS_14일(%)'] = formatted['ROAS_14일(%)'].apply(lambda x: f"{x:.2f}%")
    return formatted


def highlight_low_roas(df, target_roas):
    styles = pd.DataFrame('', index=df.index, columns=df.columns)
    for col in ['ROAS(%)', 'ROAS_14일(%)']:
        if col in df.columns:
            vals = df[col].apply(lambda x: float(str(x).replace('%', '').replace(',', '')) if pd.notna(x) else 0)
            styles[col] = vals.apply(
                lambda v: 'color: red; font-weight: bold' if v < target_roas else ''
            )
    return styles


# ──────────────────── Altair 차트 ────────────────────

def _parse_period_dates(period_str):
    """'2026.03.03 ~ 2026.03.09' → (date(2026,3,3), date(2026,3,9))"""
    try:
        parts = str(period_str).split(' ~ ')
        d1 = date(*[int(x) for x in parts[0].strip().split('.')])
        d2 = date(*[int(x) for x in parts[1].strip().split('.')])
        return d1, d2
    except Exception:
        return None, None


def filter_history_by_dates(df, sel_start, sel_end):
    """분석 기간이 선택 날짜 범위와 겹치는 행만 필터링"""
    mask = []
    for period in df['분석 기간']:
        p_start, p_end = _parse_period_dates(period)
        if p_start is not None and p_end is not None:
            # 두 기간이 겹치는지 확인
            mask.append(p_start <= sel_end and p_end >= sel_start)
        else:
            mask.append(False)
    return df[mask].copy()


def _sorted_periods(data):
    """분석 기간을 시간순(과거→현재)으로 정렬한 리스트 반환"""
    periods = data['분석 기간'].unique().tolist()
    periods.sort()  # "YYYY.MM.DD ~ ..." 형식이라 문자열 정렬 = 시간순
    return periods


def _build_chart_memo_data(chart_data, ad_type):
    """차트 위에 표시할 메모 마커 데이터 생성."""
    rows = []
    for _, r in chart_data.iterrows():
        memo_val = r.get('메모', '')
        if pd.notna(memo_val) and str(memo_val).strip():
            memo_list = get_memo_list(memo_val)
            if memo_list:
                last_memo = memo_list[-1]
                preview = last_memo[:40] + '...' if len(last_memo) > 40 else last_memo
                rows.append({
                    '분석 기간': r['분석 기간'],
                    'keyword': r['keyword'],
                    'memo_count': len(memo_list),
                    'memo_preview': preview,
                    'ROAS(%)': r.get('ROAS(%)', 0),
                    '총비용': r.get('총비용', 0),
                    '평균CPC': r.get('평균CPC', 0),
                })
    return pd.DataFrame(rows) if rows else None


def make_line_chart(data, y_col, title, memo_data=None):
    data = data.copy()
    data[y_col] = data[y_col].round(0).astype(int)
    period_order = _sorted_periods(data)

    base = alt.Chart(data).encode(
        x=alt.X('분석 기간:N', title='분석 기간', sort=period_order),
        y=alt.Y(f'{y_col}:Q', title=title),
        color=alt.Color('keyword:N', title='키워드'),
        tooltip=[
            alt.Tooltip('분석 기간:N', title='기간'),
            alt.Tooltip('keyword:N', title='키워드'),
            alt.Tooltip(f'{y_col}:Q', title=title, format=',d'),
        ]
    )

    line = base.mark_line(point=True)

    text = base.mark_text(
        align='center', dy=-12, fontSize=11, fontWeight='bold'
    ).encode(
        text='keyword:N'
    )

    chart = (line + text).properties(height=350).interactive()

    # 메모 마커 오버레이
    if memo_data is not None and len(memo_data) > 0 and y_col in memo_data.columns:
        md = memo_data.copy()
        md[y_col] = md[y_col].round(0).astype(int)
        memo_points = alt.Chart(md).mark_point(
            shape='triangle-up', size=200, filled=True, color='red'
        ).encode(
            x=alt.X('분석 기간:N', sort=period_order),
            y=alt.Y(f'{y_col}:Q'),
            tooltip=[
                alt.Tooltip('keyword:N', title='키워드'),
                alt.Tooltip('memo_count:Q', title='메모 수'),
                alt.Tooltip('memo_preview:N', title='최근 메모'),
            ]
        )
        memo_label = alt.Chart(md).mark_text(
            dy=-20, fontSize=10, color='red', fontWeight='bold'
        ).encode(
            x=alt.X('분석 기간:N', sort=period_order),
            y=alt.Y(f'{y_col}:Q'),
            text=alt.Text('memo_count:Q'),
        )
        chart = chart + memo_points + memo_label

    return chart


def make_bar_chart(data, y_col, title, memo_data=None):
    data = data.copy()
    data[y_col] = data[y_col].round(0).astype(int)
    period_order = _sorted_periods(data)

    base = alt.Chart(data).encode(
        x=alt.X('분석 기간:N', title='분석 기간', sort=period_order),
        y=alt.Y(f'{y_col}:Q', title=title),
        color=alt.Color('keyword:N', title='키워드'),
        tooltip=[
            alt.Tooltip('분석 기간:N', title='기간'),
            alt.Tooltip('keyword:N', title='키워드'),
            alt.Tooltip(f'{y_col}:Q', title=title, format=',d'),
        ],
        xOffset='keyword:N',
    )

    bar = base.mark_bar()

    text = base.mark_text(
        align='center', dy=-8, fontSize=10, fontWeight='bold'
    ).encode(
        text='keyword:N'
    )

    chart = (bar + text).properties(height=350).interactive()

    # 메모 마커 오버레이
    if memo_data is not None and len(memo_data) > 0 and y_col in memo_data.columns:
        md = memo_data.copy()
        md[y_col] = md[y_col].round(0).astype(int)
        memo_points = alt.Chart(md).mark_point(
            shape='triangle-up', size=200, filled=True, color='red'
        ).encode(
            x=alt.X('분석 기간:N', sort=period_order),
            y=alt.Y(f'{y_col}:Q'),
            tooltip=[
                alt.Tooltip('keyword:N', title='키워드'),
                alt.Tooltip('memo_count:Q', title='메모 수'),
                alt.Tooltip('memo_preview:N', title='최근 메모'),
            ]
        )
        memo_label = alt.Chart(md).mark_text(
            dy=-20, fontSize=10, color='red', fontWeight='bold'
        ).encode(
            x=alt.X('분석 기간:N', sort=period_order),
            y=alt.Y(f'{y_col}:Q'),
            text=alt.Text('memo_count:Q'),
        )
        chart = chart + memo_points + memo_label

    return chart


# ──────────────────── 파일 자동 매칭 ────────────────────

def load_file(uploaded_file):
    if uploaded_file.name.endswith('.csv'):
        return pd.read_csv(uploaded_file)
    return pd.read_excel(uploaded_file)


def classify_files(uploaded_files):
    conv_df = None
    power_ad_df = None
    powerlink_df = None
    match_info = {}

    loaded = []
    for f in uploaded_files:
        df = load_file(f)
        loaded.append((f.name, df))

    for name, df in loaded:
        cols = set(df.columns)

        if 'nt_medium' in cols and 'nt_keyword' in cols:
            conv_df = df
            match_info['전환 리포트'] = name
            continue

        if '광고그룹 ID' in cols and '상태' in cols:
            clean = df[df['상태'].notna()]
            ids = clean['광고그룹 ID'].dropna().astype(str)

            codes = set()
            for gid in ids:
                m = re.search(r'grp-a001-(\d+)-', gid)
                if m:
                    codes.add(m.group(1))

            if '03' in codes:
                power_ad_df = df
                match_info['파워컨텐츠'] = name
            elif '01' in codes:
                powerlink_df = df
                match_info['파워링크'] = name
            else:
                lower = name.lower()
                if any(k in lower for k in ['powerlink', '파워링크', 'pl_ad']):
                    powerlink_df = df
                    match_info['파워링크'] = name
                elif power_ad_df is None:
                    power_ad_df = df
                    match_info['파워컨텐츠'] = name
                else:
                    powerlink_df = df
                    match_info['파워링크'] = name

    return conv_df, power_ad_df, powerlink_df, match_info


# ──────────────────── 주간 이력 저장/로드 (Supabase) ────────────────────

def save_weekly(df, ad_type, memo_dict=None):
    db_save_weekly(user_id, df, ad_type, memo_dict)


def update_memo_in_csv(period, keyword, ad_type, new_memo_text):
    return db_update_memo(user_id, period, keyword, ad_type, new_memo_text)


def set_memo_in_csv(period, keyword, ad_type, memo_str):
    return db_set_memo(user_id, period, keyword, ad_type, memo_str)


def load_history():
    return db_load_history(user_id)


def format_history(df):
    formatted = df.copy()
    # NaN 안전 처리
    for col in ['총비용', '클릭수', '평균CPC', 'nt 클릭수', '결제수', '결제금액',
                '결제금액(+14일기여도추정)', '전환율(%)', 'ROAS(%)', 'ROAS_14일(%)']:
        if col in formatted.columns:
            formatted[col] = formatted[col].fillna(0)
    formatted['총비용'] = formatted['총비용'].apply(lambda x: f"{int(x):,}")
    formatted['클릭수'] = formatted['클릭수'].apply(lambda x: f"{int(x):,}")
    formatted['평균CPC'] = formatted['평균CPC'].apply(lambda x: f"{int(x):,}원")
    if 'nt 클릭수' in formatted.columns:
        formatted['nt 클릭수'] = formatted['nt 클릭수'].apply(lambda x: f"{int(x):,}")
    formatted['결제수'] = formatted['결제수'].apply(lambda x: f"{int(x):,}")
    formatted['결제금액'] = formatted['결제금액'].apply(lambda x: f"{int(x):,}")
    if '결제금액(+14일기여도추정)' in formatted.columns:
        formatted['결제금액(+14일기여도추정)'] = formatted['결제금액(+14일기여도추정)'].apply(lambda x: f"{int(x):,}")
    if '전환율(%)' in formatted.columns:
        formatted['전환율(%)'] = formatted['전환율(%)'].apply(lambda x: f"{x:.2f}%")
    formatted['ROAS(%)'] = formatted['ROAS(%)'].apply(lambda x: f"{int(round(x)):,}%")
    if 'ROAS_14일(%)' in formatted.columns:
        formatted['ROAS_14일(%)'] = formatted['ROAS_14일(%)'].apply(lambda x: f"{int(round(x)):,}%")
    return formatted


# ──────────────────── UI ────────────────────

st.subheader("파일 업로드 및 분석 기간 설정")

# 날짜 세션 상태 초기화: 종료일 = 어제, 시작일 = 어제 - 6일
if 'date_start' not in st.session_state:
    yesterday = date.today() - timedelta(days=1)
    st.session_state.date_start = yesterday - timedelta(days=6)
    st.session_state.date_end = yesterday


# 7일 이동 콜백
def go_prev_week():
    st.session_state.date_start -= timedelta(days=7)
    st.session_state.date_end -= timedelta(days=7)


def go_next_week():
    st.session_state.date_start += timedelta(days=7)
    st.session_state.date_end += timedelta(days=7)


with st.container(border=True):
    btn_col1, btn_col2, _, _, _ = st.columns([1, 1, 2, 1, 1])
    with btn_col1:
        st.button("◀ 이전 7일", on_click=go_prev_week)
    with btn_col2:
        st.button("이후 7일 ▶", on_click=go_next_week)

    col_date1, col_date2, col_roas = st.columns([1, 1, 1])
    with col_date1:
        start_date = st.date_input("시작일", key="date_start")
    with col_date2:
        end_date = st.date_input("종료일", key="date_end")
    with col_roas:
        target_roas = st.number_input("목표 ROAS(%)", min_value=0, value=150, step=10)

data_source = st.radio(
    "데이터 소스",
    ["🔄 API 자동 (네이버 검색광고)", "📂 엑셀 업로드"],
    horizontal=True,
    key="data_source",
)

conv_df = None
power_ad_df = None
powerlink_df = None
match_info = {}

if data_source.startswith("🔄"):
    # ── API 모드 ──
    # 캠페인 목록 로드 (세션 캐시)
    if 'api_campaigns_df' not in st.session_state:
        try:
            st.session_state['api_campaigns_df'] = list_campaigns()
        except Exception as e:
            st.error(f"❌ 캠페인 목록 로드 실패: {e}")
            st.stop()
    camp_df = st.session_state['api_campaigns_df']
    camp_pl_pc = camp_df[camp_df['유형'].isin(['파워링크', '파워컨텐츠'])].copy()

    # 저장된 선택 캠페인 ID 로드 (없으면 '논문 파워컨텐츠', 'PL_논문 파링' 기본)
    saved_camp_ids = db_get_setting(user_id, 'selected_campaign_ids', None)
    if saved_camp_ids is None:
        default_names = ['논문 파워컨텐츠', 'PL_논문 파링']
        saved_camp_ids = camp_pl_pc[camp_pl_pc['campaign_name'].isin(default_names)]['campaign_id'].tolist()

    # id ↔ label 매핑
    camp_label_map = {r['campaign_id']: f"[{r['유형']}] {r['campaign_name']}" for _, r in camp_pl_pc.iterrows()}
    selected_camp_ids = st.multiselect(
        "📌 조회할 캠페인",
        options=camp_pl_pc['campaign_id'].tolist(),
        default=[cid for cid in saved_camp_ids if cid in camp_pl_pc['campaign_id'].values],
        format_func=lambda cid: camp_label_map.get(cid, cid),
        key="selected_campaigns",
    )
    # 선택 변경 시 DB에 즉시 저장 (+ 캐시 무효화)
    if selected_camp_ids != saved_camp_ids:
        db_set_setting(user_id, 'selected_campaign_ids', selected_camp_ids)
        st.session_state.pop('api_ad_cache', None)

    col_btn, col_info = st.columns([1, 3])
    with col_btn:
        refresh = st.button("🔄 광고데이터 새로고침")
    with col_info:
        st.caption(f"기간: {start_date} ~ {end_date} · 캠페인 {len(selected_camp_ids)}개 선택")

    cache_key = (start_date.isoformat(), end_date.isoformat(), tuple(sorted(selected_camp_ids)))
    if 'api_ad_cache' not in st.session_state:
        st.session_state['api_ad_cache'] = {}

    if selected_camp_ids and (refresh or cache_key in st.session_state['api_ad_cache']):
        if refresh or cache_key not in st.session_state['api_ad_cache']:
            try:
                with st.spinner("네이버 검색광고 API 호출 중..."):
                    pl_df, pc_df = fetch_ad_data(start_date, end_date, campaign_ids=selected_camp_ids)
                st.session_state['api_ad_cache'][cache_key] = (pl_df, pc_df)
            except Exception as e:
                st.error(f"❌ API 호출 실패: {e}")
                st.caption("💡 .env의 NAVER_SA_* 값을 확인하거나, '엑셀 업로드' 모드로 전환하세요.")
                st.stop()
        powerlink_df, power_ad_df = st.session_state['api_ad_cache'][cache_key]
        cols = st.columns(2)
        with cols[0]:
            st.success(f"✅ 파워링크: {len(powerlink_df)}개 광고그룹")
        with cols[1]:
            st.success(f"✅ 파워컨텐츠: {len(power_ad_df)}개 광고그룹")
        match_info = {'파워링크': 'API', '파워컨텐츠': 'API'}
    elif not selected_camp_ids:
        st.warning("⚠️ 조회할 캠페인을 1개 이상 선택하세요.")

    conv_files = st.file_uploader(
        "전환 리포트 업로드 (여러 개 가능)",
        type=["xlsx", "csv"],
        accept_multiple_files=True,
        key="api_conv_upload",
    )
    if conv_files:
        dfs = [load_file(f) for f in conv_files]
        conv_df = pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]
        match_info['전환 리포트'] = ", ".join(f.name for f in conv_files)

    uploaded_files = list(conv_files) if conv_files else []
else:
    uploaded_files = st.file_uploader(
        "파일을 드래그하여 한번에 업로드 (파워컨텐츠 + 파워링크 + 전환 리포트)",
        type=["xlsx", "csv"],
        accept_multiple_files=True
    )
    if uploaded_files:
        conv_df, power_ad_df, powerlink_df, match_info = classify_files(uploaded_files)

if (powerlink_df is not None) or (power_ad_df is not None) or (uploaded_files and len(uploaded_files) >= 1):

    # 식별된 파일 표시
    detected_labels = [l for l in ['전환 리포트', '파워컨텐츠', '파워링크'] if match_info.get(l)]
    if detected_labels:
        cols = st.columns(len(detected_labels))
        for i, label in enumerate(detected_labels):
            with cols[i]:
                st.success(f"✅ {label}: {match_info[label]}")
    if conv_df is None:
        st.caption("ℹ️ 전환 리포트 미업로드 — 전환 관련 지표(결제수, ROAS 등)는 0으로 표시됩니다.")

    # 광고 파일이 하나라도 있으면 분석 진행
    has_ad = power_ad_df is not None or powerlink_df is not None
    if has_ad:
        period_str = f"{start_date.strftime('%Y.%m.%d')} ~ {end_date.strftime('%Y.%m.%d')}"

        # 저장된 매핑 로드
        all_mappings = db_load_keyword_mappings(user_id)

        result_powercont = None
        result_powerlink = None
        unmatched_pc = pd.DataFrame()
        unmatched_pl = pd.DataFrame()
        power_ad_clean = None
        powerlink_clean = None

        # API 모드에서 전환 엑셀 없으면 DB 저장값 폴백 (광고그룹 이름 단위)
        use_db_fallback = (conv_df is None)
        saved_pc = db_load_period_conversions(user_id, period_str, '파워컨텐츠') if use_db_fallback else {}
        saved_pl = db_load_period_conversions(user_id, period_str, '파워링크') if use_db_fallback else {}

        if power_ad_df is not None:
            power_ad_clean = process_ad_data(power_ad_df)
            conv_powercont = build_conv_grouped(conv_df, power_ad_clean, all_mappings, '파워컨텐츠', 'powercont')
            unmatched_pc = find_unmatched(power_ad_clean, conv_powercont)
            result_powercont = merge_and_calc(power_ad_clean, conv_powercont, period_str)
            if saved_pc:
                result_powercont = apply_saved_conversions(result_powercont, saved_pc)

        if powerlink_df is not None:
            powerlink_clean = process_ad_data(powerlink_df)
            conv_pl = build_conv_grouped(conv_df, powerlink_clean, all_mappings, '파워링크', 'pl')
            unmatched_pl = find_unmatched(powerlink_clean, conv_pl)
            result_powerlink = merge_and_calc(powerlink_clean, conv_pl, period_str)
            if saved_pl:
                result_powerlink = apply_saved_conversions(result_powerlink, saved_pl)

        if use_db_fallback:
            if saved_pc or saved_pl:
                st.info(f"ℹ️ 전환 엑셀 미업로드 — DB의 저장된 전환값으로 자동 채움 (파워컨텐츠 {len(saved_pc)}건, 파워링크 {len(saved_pl)}건, user={user_id}, period={period_str})")
            else:
                st.warning(
                    f"⚠️ 전환 엑셀 없음 + DB에도 [{period_str}] 기간의 저장된 전환값 없음 "
                    f"(user=`{user_id}`) — 결제/ROAS는 0으로 표시됨. "
                    f"{'URL에 `?uid=admin`을 붙여 접속하세요.' if user_id != 'admin' and user_id != 'leejay' else '다른 기간으로 이동하거나 전환 리포트 엑셀을 업로드하세요.'}"
                )

        # ── 키워드 매핑 UI (항상 표시) ──
        total_unmatched = len(unmatched_pc) + len(unmatched_pl)
        label = f"🔗 키워드 매핑 ({total_unmatched}개 미매칭)" if total_unmatched > 0 else "🔗 키워드 매핑"
        with st.expander(label, expanded=(total_unmatched > 0)):
            tab_mapping, tab_saved = st.tabs(["키워드 매핑", "저장된 매핑"])

            with tab_mapping:
                # 드롭다운 옵션 구성
                medium_options = sorted(conv_df['nt_medium'].dropna().unique().tolist()) if conv_df is not None else []
                keyword_options = sorted(conv_df['nt_keyword'].dropna().unique().tolist()) if conv_df is not None else []

                # 모든 광고그룹 목록 구성 (총비용 > 0)
                all_ad_groups = []
                if power_ad_clean is not None:
                    for _, row in power_ad_clean[power_ad_clean['총비용'] > 0].drop_duplicates(subset='광고그룹 이름').iterrows():
                        is_unmatched = row['광고그룹 이름'] in unmatched_pc['광고그룹 이름'].values if len(unmatched_pc) > 0 else False
                        all_ad_groups.append(('파워컨텐츠', row['광고그룹 이름'], row['keyword'], int(row['총비용']), is_unmatched))
                if powerlink_clean is not None:
                    for _, row in powerlink_clean[powerlink_clean['총비용'] > 0].drop_duplicates(subset='광고그룹 이름').iterrows():
                        is_unmatched = row['광고그룹 이름'] in unmatched_pl['광고그룹 이름'].values if len(unmatched_pl) > 0 else False
                        all_ad_groups.append(('파워링크', row['광고그룹 이름'], row['keyword'], int(row['총비용']), is_unmatched))

                if not all_ad_groups:
                    st.caption("광고 데이터가 없습니다.")
                elif not medium_options and not keyword_options:
                    st.caption("ℹ️ 전환 리포트를 업로드하면 매핑할 값을 선택할 수 있습니다.")
                else:
                    all_ad_groups.sort(key=lambda x: (not x[4], x[0], -x[3]))
                    st.caption("💡 medium과 keyword를 동시에 선택하면 두 조건을 모두 만족하는 전환만 집계합니다.")

                    # 헤더
                    h1, h2, h3, h4, h5 = st.columns([3, 1, 2, 2, 1])
                    with h1: st.caption("광고그룹")
                    with h2: st.caption("비용")
                    with h3: st.caption("nt_medium 선택")
                    with h4: st.caption("nt_keyword 선택")
                    with h5: st.caption("")

                    mapping_selections = {}
                    current_type = None
                    for ad_type_label, ag_name, kw, cost, is_unmatched in all_ad_groups:
                        if ad_type_label != current_type:
                            current_type = ad_type_label
                            st.markdown(f"**{ad_type_label}**")
                        marker = "⚠️ " if is_unmatched else ""
                        saved = all_mappings.get((ag_name, ad_type_label), {})
                        saved_med = saved.get('medium', []) if isinstance(saved, dict) else []
                        saved_kw = saved.get('keyword', []) if isinstance(saved, dict) else []

                        c1, c2, c3, c4, c5 = st.columns([3, 1, 2, 2, 1])
                        with c1:
                            st.text(f"{marker}{ag_name[:35]}")
                        with c2:
                            st.caption(f"₩{cost:,}")
                        with c3:
                            sel_med = st.multiselect(
                                "medium",
                                medium_options,
                                default=[m for m in saved_med if m in medium_options],
                                key=f"map_med_{ad_type_label}_{ag_name}",
                                label_visibility="collapsed"
                            )
                        with c4:
                            sel_kw = st.multiselect(
                                "keyword",
                                keyword_options,
                                default=[k for k in saved_kw if k in keyword_options],
                                key=f"map_kw_{ad_type_label}_{ag_name}",
                                label_visibility="collapsed"
                            )
                        with c5:
                            if saved_med or saved_kw:
                                st.caption("✅저장됨")
                        if sel_med or sel_kw:
                            mapping_selections[(ag_name, ad_type_label)] = {'medium': sel_med, 'keyword': sel_kw}

                    if st.button("💾 매핑 저장", key="save_mappings"):
                        if mapping_selections:
                            for (ag_name, at), m in mapping_selections.items():
                                db_save_keyword_mapping(user_id, ag_name, at, m['medium'], m['keyword'])
                            st.success(f"{len(mapping_selections)}건 매핑 저장 완료")
                            st.rerun()
                        else:
                            st.warning("매핑할 항목을 선택해주세요.")

            with tab_saved:
                if not all_mappings:
                    st.caption("저장된 매핑이 없습니다.")
                else:
                    for (ag_name, at), m in all_mappings.items():
                        med_list = m.get('medium', []) if isinstance(m, dict) else []
                        kw_list = m.get('keyword', []) if isinstance(m, dict) else []
                        parts = []
                        if med_list: parts.append(f"medium: {', '.join(med_list)}")
                        if kw_list: parts.append(f"keyword: {', '.join(kw_list)}")
                        c1, c2, c3, c4 = st.columns([3, 3, 1, 1])
                        with c1:
                            st.text(ag_name[:40])
                        with c2:
                            st.text(' / '.join(parts) if parts else '(없음)')
                        with c3:
                            st.caption(at)
                        with c4:
                            if st.button("🗑", key=f"del_map_{at}_{ag_name}", help="매핑 삭제"):
                                db_delete_keyword_mapping(user_id, ag_name, at)
                                st.rerun()

        # ── 현재 분석 메모 session_state 초기화 ──
        if 'current_memos' not in st.session_state:
            st.session_state['current_memos'] = {}  # {(period, keyword, ad_type): memo_str}

        # 기존 CSV에서 현재 기간 메모 로드
        _hist_for_memo = load_history()
        if _hist_for_memo is not None:
            for _, r in _hist_for_memo.iterrows():
                key = (r['분석 기간'], r['keyword'], r['유형'])
                if key not in st.session_state['current_memos'] and pd.notna(r.get('메모', '')) and str(r.get('메모', '')).strip():
                    st.session_state['current_memos'][key] = str(r['메모'])

        def _get_current_memo(period, keyword, ad_type):
            return st.session_state['current_memos'].get((period, keyword, ad_type), '')

        def render_memo_panel(period, keyword, ad_type, panel_id):
            """행 선택 시 표시되는 메모 패널 (조회 + 입력 + 삭제)."""
            cm_key = (period, keyword, ad_type)
            memo_val = _get_current_memo(period, keyword, ad_type)
            memo_list = get_memo_list(memo_val)
            with st.container(border=True):
                st.markdown(f"📝 **{keyword}** 메모 ({period})")
                if memo_list:
                    for i, m in enumerate(memo_list):
                        mcol1, mcol2 = st.columns([9, 1])
                        with mcol1:
                            st.markdown(f"- {m}")
                        with mcol2:
                            if st.button("🗑", key=f"memo_del_{panel_id}_{i}", help="이 메모 삭제"):
                                new_val = delete_memo(memo_val, i)
                                st.session_state['current_memos'][cm_key] = new_val
                                set_memo_in_csv(period, keyword, ad_type, new_val)
                                st.rerun()
                else:
                    st.caption("등록된 메모가 없습니다.")
                st.divider()
                new_memo = st.text_input("새 메모 입력", key=f"memo_new_{panel_id}", placeholder="메모 내용을 입력하세요")
                if st.button("💾 저장", key=f"memo_save_{panel_id}"):
                    if new_memo.strip():
                        old = st.session_state['current_memos'].get(cm_key, '')
                        st.session_state['current_memos'][cm_key] = append_memo(old, new_memo.strip())
                        update_memo_in_csv(period, keyword, ad_type, new_memo.strip())
                        st.rerun()

        # ── 컨트롤 영역 ──
        st.markdown("---")
        ctrl_col1, ctrl_col2, ctrl_col3 = st.columns([1, 1, 1])
        with ctrl_col1:
            hide_zero = st.radio(
                "총비용 0원 항목",
                ["숨김", "표시"],
                index=0,
                horizontal=True,
            ) == "숨김"
        with ctrl_col2:
            show_hidden = st.checkbox("숨긴 광고그룹 포함해서 보기", value=False)
        with ctrl_col3:
            if st.button("📥 주간 데이터 저장"):
                pc_memos = {(p, kw): v for (p, kw, at), v in st.session_state['current_memos'].items() if at == '파워컨텐츠'}
                pl_memos = {(p, kw): v for (p, kw, at), v in st.session_state['current_memos'].items() if at == '파워링크'}
                if result_powercont is not None:
                    save_weekly(result_powercont, '파워컨텐츠', memo_dict=pc_memos if pc_memos else None)
                if result_powerlink is not None:
                    save_weekly(result_powerlink, '파워링크', memo_dict=pl_memos if pl_memos else None)
                st.success(f"✅ [{period_str}] 주간 데이터가 저장되었습니다.")

        # ── 숨김 광고그룹 ID 로드 ──
        hidden_ids = db_load_hidden_adgroups(user_id)

        def _apply_hidden_filter(df):
            if show_hidden or not hidden_ids or df is None or len(df) == 0:
                return df
            if '광고그룹 ID' in df.columns:
                return df[~df['광고그룹 ID'].isin(hidden_ids)].copy()
            return df

        def _render_result_table(result_df, ad_type_label, key_prefix):
            if result_df is None:
                return
            df = result_df[result_df['총비용'] > 0].copy() if hide_zero else result_df.copy()
            df = _apply_hidden_filter(df)
            hidden_count = 0
            if not show_hidden and '광고그룹 ID' in result_df.columns:
                hidden_count = result_df['광고그룹 ID'].isin(hidden_ids).sum()
            header = f"📊 {ad_type_label} 성과 [{period_str}]"
            if hidden_count > 0:
                header += f"  (숨김 {hidden_count}개)"
            st.subheader(header)

            if len(df) == 0:
                st.caption("표시할 광고그룹이 없습니다.")
                return

            fmt = format_result(df)
            memo_src = {(r['분석 기간'], r['keyword']): _get_current_memo(r['분석 기간'], r['keyword'], ad_type_label)
                        for _, r in df.iterrows()}
            fmt = add_memo_column(fmt, memo_src)
            # 광고그룹 ID는 UI에 노출 안 함
            if '광고그룹 ID' in fmt.columns:
                fmt_display = fmt.drop(columns=['광고그룹 ID'])
            else:
                fmt_display = fmt
            styled = fmt_display.style.apply(lambda _: highlight_low_roas(fmt_display, target_roas), axis=None)
            st.caption("💡 행을 클릭하면 메모 및 숨김 처리 가능")
            ev = st.dataframe(styled, use_container_width=True, hide_index=True,
                              on_select="rerun", selection_mode="single-row", key=f"df_{key_prefix}")
            if ev.selection.rows:
                idx = ev.selection.rows[0]
                row = df.iloc[idx]
                ag_id = row.get('광고그룹 ID', '')
                ag_name = row['광고그룹 이름']
                with st.container(border=True):
                    col_a, col_b = st.columns([3, 1])
                    with col_a:
                        st.markdown(f"**{ag_name}**")
                        st.caption(f"ID: `{ag_id}`")
                    with col_b:
                        if ag_id and ag_id in hidden_ids:
                            if st.button("♻️ 숨김 해제", key=f"unhide_{key_prefix}_{ag_id}"):
                                db_unhide_adgroup(user_id, ag_id)
                                st.rerun()
                        elif ag_id:
                            if st.button("🙈 숨김", key=f"hide_{key_prefix}_{ag_id}"):
                                db_hide_adgroup(user_id, ag_id, ag_name, ad_type_label)
                                st.rerun()
                        else:
                            st.caption("(ID 없음)")
                render_memo_panel(row['분석 기간'], row['keyword'], ad_type_label, key_prefix)

        _render_result_table(result_powercont, '파워컨텐츠', 'pc')
        _render_result_table(result_powerlink, '파워링크', 'pl')

        # ── 숨김 목록 관리 ──
        hidden_detail = db_load_hidden_detail(user_id)
        if hidden_detail:
            with st.expander(f"🙈 숨김 목록 ({len(hidden_detail)}개)"):
                for h in hidden_detail:
                    c1, c2, c3, c4 = st.columns([4, 2, 2, 1])
                    with c1:
                        st.text(h.get('ad_group_name') or h.get('adgroup_id', ''))
                    with c2:
                        st.caption(h.get('ad_type', '') or '—')
                    with c3:
                        st.caption((h.get('created_at') or '')[:10])
                    with c4:
                        if st.button("♻️", key=f"unhide_list_{h['adgroup_id']}", help="숨김 해제"):
                            db_unhide_adgroup(user_id, h['adgroup_id'])
                            st.rerun()

    else:
        st.warning("광고 파일(파워컨텐츠 또는 파워링크)을 하나 이상 업로드해주세요.")
else:
    st.info("엑셀/CSV 파일 3개를 한번에 드래그하여 업로드하세요.")

# ──────────────────── 주별 추이 비교 (항상 표시) ────────────────────
st.markdown("---")
st.subheader("📈 주별 추이 비교")
history = load_history()
if history is not None and len(history) > 0:
    ad_type = st.radio("광고 유형", ["파워컨텐츠", "파워링크"], horizontal=True, key="trend_ad_type")
    hist_filtered = history[history['유형'] == ad_type].copy()
    hist_filtered = filter_history_by_dates(hist_filtered, start_date, end_date)

    if len(hist_filtered) > 0:
        hist_hide_zero = st.radio(
            "총비용 0원 항목",
            ["숨김", "표시"],
            index=0,
            horizontal=True,
            key="trend_hide_zero",
        ) == "숨김"
        if hist_hide_zero:
            nonzero_kws = set(
                hist_filtered[hist_filtered['총비용'] > 0]['keyword'].dropna().unique()
            )
            hist_filtered = hist_filtered[hist_filtered['keyword'].isin(nonzero_kws)]

        available_keywords = sorted(hist_filtered['keyword'].dropna().unique().tolist())
        selected_keywords = st.multiselect(
            "키워드 선택", available_keywords, default=available_keywords, key="trend_keywords"
        )

        if selected_keywords:
            chart_data = hist_filtered[hist_filtered['keyword'].isin(selected_keywords)].copy()

            # ── 테이블: 키워드 선택 바로 아래 ──
            st.markdown("#### 주차별 상세 데이터")
            display_hist = chart_data.sort_values(['분석 기간', '총비용'], ascending=[True, False])
            # 14일 기여도 컬럼 유지 (결제금액(+14일기여도추정), ROAS_14일(%))
            fmt_hist = format_history(display_hist)
            # 메모 열: 이미 CSV에 있는 메모 표시
            hist_memo_src = {}
            for _, r in display_hist.iterrows():
                key = (r['분석 기간'], r['keyword'])
                memo_val = r.get('메모', '')
                if pd.notna(memo_val) and str(memo_val).strip():
                    hist_memo_src[key] = str(memo_val)
            fmt_hist = add_memo_column(fmt_hist, hist_memo_src)
            # 유형 컬럼 제거 (표시 불필요)
            if '유형' in fmt_hist.columns:
                fmt_hist = fmt_hist.drop(columns=['유형'])
            styled_hist = fmt_hist.style.apply(
                lambda _: highlight_low_roas(fmt_hist, target_roas), axis=None
            )
            st.caption("💡 행을 클릭하면 메모를 추가/조회할 수 있습니다")
            ev_hist = st.dataframe(styled_hist, use_container_width=True, hide_index=True,
                                   on_select="rerun", selection_mode="single-row", key="df_hist")

            # ── 기간 합계 ──
            sum_cost = int(chart_data['총비용'].fillna(0).sum())
            sum_clk = int(chart_data['클릭수'].fillna(0).sum())
            sum_nt = int(chart_data['nt 클릭수'].fillna(0).sum()) if 'nt 클릭수' in chart_data.columns else 0
            sum_ord = int(chart_data['결제수'].fillna(0).sum())
            sum_amt = int(chart_data['결제금액'].fillna(0).sum())
            sum_amt14 = int(chart_data['결제금액(+14일기여도추정)'].fillna(0).sum()) if '결제금액(+14일기여도추정)' in chart_data.columns else 0
            avg_cpc = int(sum_cost / sum_clk) if sum_clk else 0
            conv_rate = (sum_ord / sum_clk * 100) if sum_clk else 0
            roas = (sum_amt / sum_cost * 100) if sum_cost else 0
            roas14 = (sum_amt14 / sum_cost * 100) if sum_cost else 0
            total_row = {
                '분석 기간': f"기간 합계 ({start_date.strftime('%m/%d')}~{end_date.strftime('%m/%d')})",
                'keyword': f"{len(selected_keywords)}개 키워드",
                '총비용': f"{sum_cost:,}",
                '클릭수': f"{sum_clk:,}",
                '평균CPC': f"{avg_cpc:,}원",
                'nt 클릭수': f"{sum_nt:,}",
                '결제수': f"{sum_ord:,}",
                '결제금액': f"{sum_amt:,}",
                '결제금액(+14일기여도추정)': f"{sum_amt14:,}",
                '전환율(%)': f"{conv_rate:.2f}%",
                'ROAS(%)': f"{int(round(roas)):,}%",
                'ROAS_14일(%)': f"{int(round(roas14)):,}%",
            }
            total_cols = [c for c in fmt_hist.columns if c in total_row]
            total_df = pd.DataFrame([[total_row[c] for c in total_cols]], columns=total_cols)
            st.markdown("##### 📊 기간 합계")
            st.dataframe(total_df, use_container_width=True, hide_index=True)
            if ev_hist.selection.rows:
                idx = ev_hist.selection.rows[0]
                row = display_hist.iloc[idx]
                sel_period = row['분석 기간']
                sel_keyword = row['keyword']
                # 주차별 메모: CSV에서 직접 읽은 메모 + session_state 메모 통합
                memo_val = hist_memo_src.get((sel_period, sel_keyword), '')
                cm_key = (sel_period, sel_keyword, ad_type)
                session_memo = st.session_state.get('current_memos', {}).get(cm_key, '')
                # session_state 메모가 더 최신이면 그쪽 사용
                display_memo = session_memo if len(str(session_memo)) >= len(str(memo_val)) else memo_val
                memo_list = get_memo_list(display_memo)
                with st.container(border=True):
                    st.markdown(f"📝 **{sel_keyword}** 메모 ({sel_period})")
                    if memo_list:
                        for i, m in enumerate(memo_list):
                            mcol1, mcol2 = st.columns([9, 1])
                            with mcol1:
                                st.markdown(f"- {m}")
                            with mcol2:
                                if st.button("🗑", key=f"memo_del_hist_{i}", help="이 메모 삭제"):
                                    new_val = delete_memo(display_memo, i)
                                    if 'current_memos' not in st.session_state:
                                        st.session_state['current_memos'] = {}
                                    st.session_state['current_memos'][cm_key] = new_val
                                    set_memo_in_csv(sel_period, sel_keyword, ad_type, new_val)
                                    st.rerun()
                    else:
                        st.caption("등록된 메모가 없습니다.")
                    st.divider()
                    new_memo = st.text_input("새 메모 입력", key="memo_new_hist", placeholder="메모 내용을 입력하세요")
                    if st.button("💾 저장", key="memo_save_hist"):
                        if new_memo.strip():
                            update_memo_in_csv(sel_period, sel_keyword, ad_type, new_memo.strip())
                            old = st.session_state.get('current_memos', {}).get(cm_key, '')
                            if 'current_memos' not in st.session_state:
                                st.session_state['current_memos'] = {}
                            st.session_state['current_memos'][cm_key] = append_memo(old, new_memo.strip())
                            st.rerun()

            # ── 차트 ──
            # 차트용 메모 데이터 준비
            memo_chart_data = _build_chart_memo_data(chart_data, ad_type)

            st.markdown("#### ROAS(%) 주별 추이")
            st.altair_chart(
                make_line_chart(chart_data, 'ROAS(%)', 'ROAS(%)', memo_data=memo_chart_data),
                use_container_width=True
            )

            st.markdown("#### 총비용 주별 추이")
            st.altair_chart(
                make_bar_chart(chart_data, '총비용', '총비용', memo_data=memo_chart_data),
                use_container_width=True
            )

            st.markdown("#### 평균CPC 주별 추이")
            st.altair_chart(
                make_line_chart(chart_data, '평균CPC', '평균CPC(원)', memo_data=memo_chart_data),
                use_container_width=True
            )
        else:
            st.info("비교할 키워드를 선택해주세요.")
    else:
        st.info(f"선택한 기간({start_date} ~ {end_date})에 {ad_type} 데이터가 없습니다.")
else:
    st.caption("💡 '주간 데이터 저장' 버튼으로 데이터를 누적하면 주별 추이를 비교할 수 있습니다.")
