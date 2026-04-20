"""네이버 검색광고 API 클라이언트.

광고그룹/통계 데이터를 가져와 app.py의 process_ad_data()와 호환되는
DataFrame을 반환한다.
"""
import os
import time
import hmac
import hashlib
import base64
import json
import re
from datetime import date
from pathlib import Path

import requests
import pandas as pd
import streamlit as st

BASE_URL = "https://api.searchad.naver.com"


def _current_user_id():
    try:
        params = st.query_params
        if "uid" in params:
            return str(params["uid"])
    except Exception:
        pass
    return ""


def _load_credentials():
    """st.secrets 우선 (user_id별 분기 지원), 없으면 ~/.claude/.env에서 로드."""
    uid = _current_user_id()

    def _pick(sec):
        try:
            ak, sk, cid = sec["api_key"], sec["secret_key"], str(sec["customer_id"])
            if ak and sk and cid:
                return ak, sk, cid
        except Exception:
            pass
        return None

    try:
        sec_root = st.secrets["naver_sa"]
        # 1) uid 서브섹션
        if uid:
            try:
                sub = sec_root[uid]
                got = _pick(sub)
                if got:
                    return got
            except Exception:
                pass
        # 2) 루트 섹션의 직접 키 (기본값)
        got = _pick(sec_root)
        if got:
            return got
        # 3) 아무 서브섹션이나 (최후 폴백)
        for k in list(sec_root.keys()):
            try:
                got = _pick(sec_root[k])
                if got:
                    return got
            except Exception:
                continue
    except Exception:
        pass

    # .env 직접 파싱 (load_dotenv override 이슈 회피)
    creds = {"NAVER_SA_API_KEY": "", "NAVER_SA_SECRET_KEY": "", "NAVER_SA_CUSTOMER_ID": ""}
    env_path = Path.home() / ".claude" / ".env"
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            if k in creds:
                creds[k] = v.strip().split("#")[0].strip()

    # .env가 비어있으면 환경변수 폴백
    for k in creds:
        if not creds[k]:
            creds[k] = os.getenv(k, "")

    return creds["NAVER_SA_API_KEY"], creds["NAVER_SA_SECRET_KEY"], creds["NAVER_SA_CUSTOMER_ID"]


def _sign(secret: str, method: str, path: str, timestamp: str) -> str:
    msg = f"{timestamp}.{method}.{path}"
    raw = hmac.new(secret.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(raw).decode("utf-8")


def _headers(method: str, path: str):
    api_key, secret, customer_id = _load_credentials()
    if not (api_key and secret and customer_id):
        raise RuntimeError(
            "네이버 SA API 인증 정보가 없습니다. .env 또는 secrets.toml에 "
            "NAVER_SA_API_KEY / NAVER_SA_SECRET_KEY / NAVER_SA_CUSTOMER_ID를 설정하세요."
        )
    ts = str(int(time.time() * 1000))
    return {
        "Content-Type": "application/json; charset=UTF-8",
        "X-Timestamp": ts,
        "X-API-KEY": api_key,
        "X-Customer": str(customer_id),
        "X-Signature": _sign(secret, method, path, ts),
    }


def _classify_campaign_type(campaign_tp: str, adgroup_id: str) -> str:
    """campaignTp + 광고그룹 ID prefix로 유형 분류.
    파워링크: WEB_SITE / grp-a001-01-
    파워컨텐츠: POWER_CONTENTS / grp-a001-03-
    """
    if campaign_tp:
        if "POWER_CONTENTS" in campaign_tp:
            return "파워컨텐츠"
        if "WEB_SITE" in campaign_tp or "POWER_LINK" in campaign_tp:
            return "파워링크"
    m = re.search(r"grp-a001-(\d+)-", str(adgroup_id))
    if m:
        if m.group(1) == "03":
            return "파워컨텐츠"
        if m.group(1) == "01":
            return "파워링크"
    return "기타"


def list_campaigns() -> pd.DataFrame:
    """전체 캠페인 목록 (nccCampaignId, name, campaignTp)."""
    path = "/ncc/campaigns"
    r = requests.get(BASE_URL + path, headers=_headers("GET", path), timeout=30)
    r.raise_for_status()
    rows = []
    for c in r.json():
        rows.append({
            "campaign_id": c.get("nccCampaignId"),
            "campaign_name": c.get("name"),
            "campaign_type": c.get("campaignTp", ""),
        })
    df = pd.DataFrame(rows)
    if len(df) > 0:
        df["유형"] = df.apply(
            lambda r: _classify_campaign_type(r["campaign_type"], ""),
            axis=1,
        )
    return df


def list_adgroups() -> pd.DataFrame:
    """전체 광고그룹 목록."""
    path = "/ncc/adgroups"
    r = requests.get(BASE_URL + path, headers=_headers("GET", path), timeout=30)
    r.raise_for_status()
    data = r.json()
    rows = []
    for ag in data:
        rows.append({
            "광고그룹 ID": ag.get("nccAdgroupId"),
            "광고그룹 이름": ag.get("name"),
            "상태": ag.get("status"),
            "campaign_id": ag.get("nccCampaignId"),
            "campaign_type": ag.get("campaignTp", ""),
        })
    df = pd.DataFrame(rows)
    if len(df) > 0:
        df["유형"] = df.apply(
            lambda r: _classify_campaign_type(r["campaign_type"], r["광고그룹 ID"]),
            axis=1,
        )
    return df


def fetch_stats(adgroup_ids: list, since: str, until: str) -> pd.DataFrame:
    """광고그룹별 통계 조회 (/stats, 단일 id 모드).
    since/until: 'YYYY-MM-DD'
    반환: 광고그룹 ID, 클릭수, 총비용 (기간 합계)
    """
    if not adgroup_ids:
        return pd.DataFrame(columns=["광고그룹 ID", "클릭수", "총비용"])

    path = "/stats"
    rows = []
    time_range = json.dumps({"since": since, "until": until})
    fields = json.dumps(["impCnt", "clkCnt", "salesAmt"])
    for ag_id in adgroup_ids:
        params = {"id": ag_id, "fields": fields, "timeRange": time_range}
        r = requests.get(
            BASE_URL + path,
            headers=_headers("GET", path),
            params=params,
            timeout=30,
        )
        if r.status_code != 200:
            # 실패 그룹은 0으로 처리 (전체 중단 방지)
            rows.append({"광고그룹 ID": ag_id, "클릭수": 0, "총비용": 0})
            continue
        payload = r.json()
        daily = payload.get("data", []) if isinstance(payload, dict) else []
        clk = sum(int(d.get("clkCnt", 0) or 0) for d in daily)
        cost = sum(int(d.get("salesAmt", 0) or 0) for d in daily)
        rows.append({"광고그룹 ID": ag_id, "클릭수": clk, "총비용": cost})
    return pd.DataFrame(rows)


def fetch_ad_data(start_date: date, end_date: date, campaign_ids: list = None):
    """파워링크/파워컨텐츠 광고결과 조회.
    campaign_ids: None이면 전체. 리스트 주면 해당 캠페인 소속 광고그룹만.
    반환: (powerlink_df, powercont_df)
    """
    since = start_date.strftime("%Y-%m-%d")
    until = end_date.strftime("%Y-%m-%d")

    ag_df = list_adgroups()
    if len(ag_df) == 0:
        empty = pd.DataFrame(columns=["광고그룹 ID", "광고그룹 이름", "상태", "총비용", "클릭수"])
        return empty, empty

    target = ag_df[ag_df["유형"].isin(["파워링크", "파워컨텐츠"])].copy()
    if campaign_ids:
        target = target[target["campaign_id"].isin(campaign_ids)].copy()
    if len(target) == 0:
        empty = pd.DataFrame(columns=["광고그룹 ID", "광고그룹 이름", "상태", "총비용", "클릭수"])
        return empty, empty

    stats = fetch_stats(target["광고그룹 ID"].dropna().astype(str).tolist(), since, until)
    merged = target.merge(stats, on="광고그룹 ID", how="left")
    merged["총비용"] = merged["총비용"].fillna(0).astype(int)
    merged["클릭수"] = merged["클릭수"].fillna(0).astype(int)

    cols = ["광고그룹 ID", "광고그룹 이름", "상태", "총비용", "클릭수"]
    pl = merged[merged["유형"] == "파워링크"][cols].reset_index(drop=True)
    pc = merged[merged["유형"] == "파워컨텐츠"][cols].reset_index(drop=True)
    return pl, pc
