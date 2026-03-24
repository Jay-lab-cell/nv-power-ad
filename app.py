import streamlit as st
import pandas as pd
import altair as alt
import re
from datetime import date, timedelta, datetime

from db import (get_user_id, db_save_weekly, db_load_history, db_update_memo, db_set_memo,
                db_load_keyword_mappings, db_save_keyword_mapping, db_delete_keyword_mapping)

st.set_page_config(page_title="네이버 광고 ROAS 분석", layout="wide")
st.title("네이버 파워링크 & 파워컨텐츠 ROAS 분석")

# ── 사용자 식별 ──
user_id = get_user_id()

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


def process_ad_data(ad_df, keyword_mappings=None):
    ad_df = ad_df[ad_df['상태'].notna()].copy()
    ad_df['총비용'] = ad_df['총비용(VAT포함,원)'].apply(clean_cost)
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


def process_conversion(conv_df, medium):
    filtered = conv_df[conv_df['nt_medium'] == medium].copy()
    # medium 필터 결과가 비어있으면 전체 데이터로 fallback
    if len(filtered) == 0:
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


def aggregate_mapped_conversions(conv_grouped, mappings_multi):
    """다중 매핑된 nt_keyword들의 전환 데이터를 합산.
    mappings_multi: {ad_group_name: [kw1, kw2, ...]} (2개 이상만)
    첫 번째 keyword를 primary key로 사용."""
    if conv_grouped is None or len(conv_grouped) == 0 or not mappings_multi:
        return conv_grouped

    extra_rows = []
    all_aggregated_kws = set()
    for ag_name, kw_list in mappings_multi.items():
        if len(kw_list) <= 1:
            continue
        primary = kw_list[0]
        matched = conv_grouped[conv_grouped['nt_keyword'].isin(kw_list)]
        if len(matched) == 0:
            continue
        summed = matched.select_dtypes(include='number').sum()
        new_row = {'nt_keyword': primary}
        new_row.update(summed.to_dict())
        extra_rows.append(new_row)
        all_aggregated_kws.update(kw_list)

    if not extra_rows:
        return conv_grouped

    result = conv_grouped[~conv_grouped['nt_keyword'].isin(all_aggregated_kws)].copy()
    extra_df = pd.DataFrame(extra_rows)
    return pd.concat([result, extra_df], ignore_index=True)


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

    result = merged[['분석 기간', '광고그룹 이름', 'keyword', '총비용', '평균CPC',
                      '클릭수', 'nt 클릭수', '결제수', '결제금액', '결제금액(+14일기여도추정)',
                      '전환율(%)', 'ROAS(%)', 'ROAS_14일(%)']].copy()
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

uploaded_files = st.file_uploader(
    "파일을 드래그하여 한번에 업로드 (파워컨텐츠 + 파워링크 + 전환 리포트)",
    type=["xlsx", "csv"],
    accept_multiple_files=True
)

if uploaded_files and len(uploaded_files) >= 1:
    conv_df, power_ad_df, powerlink_df, match_info = classify_files(uploaded_files)

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

        # 전환 데이터 처리 (없으면 빈 DataFrame)
        empty_conv = pd.DataFrame(columns=['nt_keyword', '결제수', '결제금액', '결제금액(+14일기여도추정)', 'nt 클릭수'])

        # 저장된 키워드 매핑 로드 (리스트 형태)
        all_mappings = db_load_keyword_mappings(user_id)
        # 단일값 매핑 (process_ad_data용: 첫 번째 키워드)
        pc_mappings = {name: kw_list[0] for (name, at), kw_list in all_mappings.items() if at == '파워컨텐츠' and kw_list}
        pl_mappings = {name: kw_list[0] for (name, at), kw_list in all_mappings.items() if at == '파워링크' and kw_list}
        # 다중값 매핑 (전환 합산용)
        pc_multi = {name: kw_list for (name, at), kw_list in all_mappings.items() if at == '파워컨텐츠' and len(kw_list) > 1}
        pl_multi = {name: kw_list for (name, at), kw_list in all_mappings.items() if at == '파워링크' and len(kw_list) > 1}

        result_powercont = None
        result_powerlink = None
        unmatched_pc = pd.DataFrame()
        unmatched_pl = pd.DataFrame()

        if power_ad_df is not None:
            power_ad_clean = process_ad_data(power_ad_df, keyword_mappings=pc_mappings)
            conv_powercont = process_conversion(conv_df, 'powercont') if conv_df is not None else empty_conv
            conv_powercont = aggregate_mapped_conversions(conv_powercont, pc_multi)
            unmatched_pc = find_unmatched(power_ad_clean, conv_powercont)
            result_powercont = merge_and_calc(power_ad_clean, conv_powercont, period_str)

        if powerlink_df is not None:
            powerlink_clean = process_ad_data(powerlink_df, keyword_mappings=pl_mappings)
            conv_pl = process_conversion(conv_df, 'pl') if conv_df is not None else empty_conv
            conv_pl = aggregate_mapped_conversions(conv_pl, pl_multi)
            unmatched_pl = find_unmatched(powerlink_clean, conv_pl)
            result_powerlink = merge_and_calc(powerlink_clean, conv_pl, period_str)

        # ── 키워드 매핑 UI (항상 표시) ──
        total_unmatched = len(unmatched_pc) + len(unmatched_pl)
        label = f"🔗 키워드 매핑 ({total_unmatched}개 미매칭)" if total_unmatched > 0 else "🔗 키워드 매핑"
        with st.expander(label, expanded=(total_unmatched > 0)):
            tab_mapping, tab_saved = st.tabs(["키워드 매핑", "저장된 매핑"])

            with tab_mapping:
                # 사용 가능한 nt_keyword 목록 수집
                all_nt_keywords = set()
                if conv_df is not None:
                    all_nt_keywords.update(conv_df['nt_keyword'].dropna().unique())
                # 저장된 매핑에서도 키워드 추가 (전환 파일 없어도 기존 매핑 표시)
                for kw_list in all_mappings.values():
                    all_nt_keywords.update(kw_list)
                nt_options_sorted = sorted(all_nt_keywords)

                # 모든 광고그룹 목록 구성 (총비용 > 0)
                all_ad_groups = []
                if power_ad_df is not None:
                    for _, row in power_ad_clean[power_ad_clean['총비용'] > 0].drop_duplicates(subset='광고그룹 이름').iterrows():
                        is_unmatched = row['광고그룹 이름'] in unmatched_pc['광고그룹 이름'].values if len(unmatched_pc) > 0 else False
                        all_ad_groups.append(('파워컨텐츠', row['광고그룹 이름'], row['keyword'], int(row['총비용']), is_unmatched))
                if powerlink_df is not None:
                    for _, row in powerlink_clean[powerlink_clean['총비용'] > 0].drop_duplicates(subset='광고그룹 이름').iterrows():
                        is_unmatched = row['광고그룹 이름'] in unmatched_pl['광고그룹 이름'].values if len(unmatched_pl) > 0 else False
                        all_ad_groups.append(('파워링크', row['광고그룹 이름'], row['keyword'], int(row['총비용']), is_unmatched))

                if not all_ad_groups:
                    st.caption("광고 데이터가 없습니다.")
                elif not all_nt_keywords:
                    st.caption("ℹ️ 전환 리포트를 업로드하면 nt_keyword를 선택할 수 있습니다.")
                else:
                    # 미매칭 항목을 상단에 배치
                    all_ad_groups.sort(key=lambda x: (not x[4], x[0], -x[3]))

                    mapping_selections = {}
                    current_type = None
                    for ad_type_label, ag_name, kw, cost, is_unmatched in all_ad_groups:
                        if ad_type_label != current_type:
                            current_type = ad_type_label
                            st.markdown(f"**{ad_type_label}**")
                        marker = "⚠️ " if is_unmatched else ""
                        c1, c2, c3, c4 = st.columns([3, 2, 1, 3])
                        with c1:
                            st.text(f"{marker}{ag_name[:38]}")
                        with c2:
                            st.text(f"추출: {kw or '없음'}")
                        with c3:
                            st.text(f"₩{cost:,}")
                        with c4:
                            saved = all_mappings.get((ag_name, ad_type_label), [])
                            default = [k for k in saved if k in all_nt_keywords]
                            sel = st.multiselect(
                                "nt_keyword",
                                nt_options_sorted,
                                default=default,
                                key=f"map_{ad_type_label}_{ag_name}",
                                label_visibility="collapsed"
                            )
                            if sel:
                                mapping_selections[(ag_name, ad_type_label)] = sel

                    if st.button("💾 매핑 저장", key="save_mappings"):
                        if mapping_selections:
                            for (ag_name, at), nt_kw_list in mapping_selections.items():
                                db_save_keyword_mapping(user_id, ag_name, at, nt_kw_list)
                            st.success(f"{len(mapping_selections)}건 매핑 저장 완료")
                            st.rerun()
                        else:
                            st.warning("매핑할 항목을 선택해주세요.")

            with tab_saved:
                if not all_mappings:
                    st.caption("저장된 매핑이 없습니다.")
                else:
                    for (ag_name, at), kw_list in all_mappings.items():
                        c1, c2, c3, c4 = st.columns([3, 3, 1, 1])
                        with c1:
                            st.text(ag_name[:40])
                        with c2:
                            st.text(f"→ {', '.join(kw_list)}")
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
        ctrl_col1, ctrl_col2 = st.columns([1, 1])
        with ctrl_col1:
            hide_zero = st.checkbox("총비용 0원 항목 숨기기", value=True)
        with ctrl_col2:
            if st.button("📥 주간 데이터 저장"):
                pc_memos = {(p, kw): v for (p, kw, at), v in st.session_state['current_memos'].items() if at == '파워컨텐츠'}
                pl_memos = {(p, kw): v for (p, kw, at), v in st.session_state['current_memos'].items() if at == '파워링크'}
                if result_powercont is not None:
                    save_weekly(result_powercont, '파워컨텐츠', memo_dict=pc_memos if pc_memos else None)
                if result_powerlink is not None:
                    save_weekly(result_powerlink, '파워링크', memo_dict=pl_memos if pl_memos else None)
                st.success(f"✅ [{period_str}] 주간 데이터가 저장되었습니다.")

        # ── 성과 테이블: 파워컨텐츠 ──
        if result_powercont is not None:
            display_pc = result_powercont[result_powercont['총비용'] > 0].copy() if hide_zero else result_powercont.copy()
            st.subheader(f"📊 파워컨텐츠 성과 [{period_str}]")
            fmt_pc = format_result(display_pc)
            pc_memo_src = {(r['분석 기간'], r['keyword']): _get_current_memo(r['분석 기간'], r['keyword'], '파워컨텐츠')
                           for _, r in display_pc.iterrows()}
            fmt_pc = add_memo_column(fmt_pc, pc_memo_src)
            styled_pc = fmt_pc.style.apply(lambda _: highlight_low_roas(fmt_pc, target_roas), axis=None)
            st.caption("💡 행을 클릭하면 메모를 추가/조회할 수 있습니다")
            ev_pc = st.dataframe(styled_pc, use_container_width=True, hide_index=True,
                                 on_select="rerun", selection_mode="single-row", key="df_pc")
            if ev_pc.selection.rows:
                idx = ev_pc.selection.rows[0]
                row = display_pc.iloc[idx]
                render_memo_panel(row['분석 기간'], row['keyword'], '파워컨텐츠', 'pc')

        # ── 성과 테이블: 파워링크 ──
        if result_powerlink is not None:
            display_pl = result_powerlink[result_powerlink['총비용'] > 0].copy() if hide_zero else result_powerlink.copy()
            st.subheader(f"📊 파워링크 성과 [{period_str}]")
            fmt_pl = format_result(display_pl)
            pl_memo_src = {(r['분석 기간'], r['keyword']): _get_current_memo(r['분석 기간'], r['keyword'], '파워링크')
                           for _, r in display_pl.iterrows()}
            fmt_pl = add_memo_column(fmt_pl, pl_memo_src)
            styled_pl = fmt_pl.style.apply(lambda _: highlight_low_roas(fmt_pl, target_roas), axis=None)
            st.caption("💡 행을 클릭하면 메모를 추가/조회할 수 있습니다")
            ev_pl = st.dataframe(styled_pl, use_container_width=True, hide_index=True,
                                 on_select="rerun", selection_mode="single-row", key="df_pl")
            if ev_pl.selection.rows:
                idx = ev_pl.selection.rows[0]
                row = display_pl.iloc[idx]
                render_memo_panel(row['분석 기간'], row['keyword'], '파워링크', 'pl')

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
        available_keywords = sorted(hist_filtered['keyword'].dropna().unique().tolist())
        selected_keywords = st.multiselect(
            "키워드 선택", available_keywords, default=available_keywords, key="trend_keywords"
        )

        if selected_keywords:
            chart_data = hist_filtered[hist_filtered['keyword'].isin(selected_keywords)].copy()

            # ── 테이블: 키워드 선택 바로 아래 ──
            st.markdown("#### 주차별 상세 데이터")
            display_hist = chart_data.sort_values(['분석 기간', '총비용'], ascending=[True, False])
            drop_cols = [c for c in ['ROAS_14일(%)', '결제금액(+14일기여도추정)'] if c in display_hist.columns]
            display_hist = display_hist.drop(columns=drop_cols)
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
