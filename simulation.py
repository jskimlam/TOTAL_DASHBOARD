
# ============================================================
# pip install pandas matplotlib numpy requests openpyxl
# ============================================================
# simulation.py - Iran Risk × ABS/SM 원가 대응 시뮬레이션
# v6.5 - Iran Premium 자동산출 + HTML 롤링 예측용 기준일 저장
# ------------------------------------------------------------
# 핵심 변경:
#   1) 구글시트 최신 실가격(예: 2026-03-06)을 기준 앵커로 CSV 저장
#   2) Iran Premium 자동산출
#      - NAP: WTI로 설명되는 정상값 초과분
#      - SM Cost: 실측 원가의 초과분
#      - ABS Gap: 이론 Gap 대비 방어분
#   3) HTML 롤링 예측용 base 정보 저장
#      - Base_Date, Base_WTI, Base_NAP, ...
#   4) 기존 크래커마진→BD 타이트 구조 유지
# ============================================================

import datetime
import io
import math
import os
from typing import Dict, Tuple, Optional

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm


# ──────────────────────────────────────────────
# 0. 설정
# ──────────────────────────────────────────────
GSHEET_CSV_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vQfp5e4ufXsgu7YvZ5EFEHatkJ7BdgW3vma78THWYn66wHUrau8hYB4q8TY2OXuc9PBguq-v09CkmKZ"
    "/pub?gid=0&single=true&output=csv"
)

COL_MAP = {
    'wti':     'NYMEX Light Sweet Crude Settlement Mo01',
    'nap':     'Naphtha C+F Japan Cargo $/mt (NextGen MOC)',
    'sm_cn':   'Styrene CFR China Marker LC 90 days',
    'sm_fob':  'Styrene Monomer FOB China Marker',
    'et':      'Ethylene CFR NE Asia',
    'bz':      'Benzene FOB Korea Marker',
    'bz_ara':  'Benzene CIF ARA',
    'bz_usg':  'Benzene FOB USG Mo02 cts/gal',
    'pr':      'Propylene Poly Grade CFR China',
    'bd':      'Butadiene CFR China',
    'an':      'ACN CFR FE Asia Weekly',
    'abs_mkt': 'ABS Inj CFR China Weekly',
}

ABS_RATIO       = {'sm': 0.60, 'an': 0.25, 'bd': 0.15}
SM_COST_RATIO   = {'bz': 0.67, 'et': 0.25, 'nap': 0.05, 'fixed': 150}
SM_THEORY_RATIO = {'bz': 0.80, 'et': 0.30, 'fixed': 150}

CRACKER_YIELDS = {"et": 0.30, "pr": 0.15, "bd": 0.045, "bz": 0.06}
CRACKER_OPEX   = 50
BD_TIGHT_SCALE = 0.5
BD_TIGHT_MAX   = 150

DEFAULT_SENS = {
    'bz':        16.81,
    'et':         6.13,
    'sm':        14.75,
    'an':         7.87,
    'bd':        20.48,
    'nap':        6.52,
    'pr':         4.18,
    'bz_ara':    15.20,
    'bz_usg':     0.85,
    'sm_cost':   14.50,
    'sm_margin':  -0.08,
    'abs_cost':  13.89,
    'abs_mkt':    8.12,
    'abs_gap':   -5.76,
}

NAP_SENS_FOR_EQUIV = DEFAULT_SENS['nap']
BZ_USG_TO_MT = 26.42

# 이벤트 직전 기준점을 잡는 규칙
BASE_LOOKBACK_WEEKS = 1      # 최신행 직전 1주를 base로 사용 (예: 3/6 기준이면 2/27)
FALLBACK_AVG_WEEKS  = 4      # 직전 행이 없으면 최근 4주 평균


# ──────────────────────────────────────────────
# 1. 한글 폰트
# ──────────────────────────────────────────────
def setup_font():
    candidates = [
        '/usr/share/fonts/truetype/nanum/NanumGothic.ttf',
        '/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc',
        '/System/Library/Fonts/AppleSDGothicNeo.ttc',
    ]
    for fp in candidates:
        if os.path.exists(fp):
            plt.rcParams['font.family'] = fm.FontProperties(fname=fp).get_name()
            matplotlib.rcParams['axes.unicode_minus'] = False
            print(f"[폰트] {fp}")
            return
    plt.rcParams['font.family'] = 'DejaVu Sans'


# ──────────────────────────────────────────────
# 2. 유틸
# ──────────────────────────────────────────────
def safe_float(v, default=np.nan):
    try:
        if pd.isna(v):
            return default
        return float(v)
    except Exception:
        return default


def calc_cracker_margin(et, pr, bd, bz, nap):
    """나프타 크래커 마진 = ET×0.30 + PR×0.15 + BD×0.045 + BZ×0.06 - NAP - OPEX(50)"""
    return (
        et  * CRACKER_YIELDS['et'] +
        pr  * CRACKER_YIELDS['pr'] +
        bd  * CRACKER_YIELDS['bd'] +
        bz  * CRACKER_YIELDS['bz'] -
        nap - CRACKER_OPEX
    )


def round_or_nan(v, nd=1):
    try:
        if pd.isna(v) or (isinstance(v, float) and math.isnan(v)):
            return np.nan
        return round(float(v), nd)
    except Exception:
        return np.nan


# ──────────────────────────────────────────────
# 3. 구글시트 파싱
# ──────────────────────────────────────────────
def load_gsheet():
    import requests
    print("[구글시트] 데이터 로드 중...")
    try:
        resp = requests.get(GSHEET_CSV_URL, timeout=15)
        resp.raise_for_status()
        df = pd.read_csv(io.StringIO(resp.text))
    except Exception as e:
        print(f"[구글시트] 로드 실패: {e}")
        return None, None, None, None

    date_col = df.columns[0]
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    for col in df.columns[1:]:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df = df[df[COL_MAP['wti']].notna()].copy()
    df = df.sort_values(date_col).reset_index(drop=True)
    if df.empty:
        print("[구글시트] 유효 데이터 없음")
        return None, None, None, None

    # 파생 변수
    df['_sm_cost'] = (
        df[COL_MAP['bz']]  * SM_COST_RATIO['bz']  +
        df[COL_MAP['et']]  * SM_COST_RATIO['et']  +
        df[COL_MAP['nap']] * SM_COST_RATIO['nap'] +
        SM_COST_RATIO['fixed']
    )
    df['_sm_margin'] = df[COL_MAP['sm_cn']] - df['_sm_cost']

    df['_sm_cost_theory'] = (
        df[COL_MAP['bz']] * SM_THEORY_RATIO['bz'] +
        df[COL_MAP['et']] * SM_THEORY_RATIO['et'] +
        SM_THEORY_RATIO['fixed']
    )
    df['_sm_margin_theory'] = df[COL_MAP['sm_cn']] - df['_sm_cost_theory']

    df['_abs_cost'] = (
        df[COL_MAP['sm_cn']] * ABS_RATIO['sm'] +
        df[COL_MAP['an']]    * ABS_RATIO['an'] +
        df[COL_MAP['bd']]    * ABS_RATIO['bd']
    )
    df['_abs_gap'] = df[COL_MAP['abs_mkt']] - df['_abs_cost']

    df['_abs_cost_theory'] = (
        df['_sm_cost_theory'] * ABS_RATIO['sm'] +
        df[COL_MAP['an']]     * ABS_RATIO['an'] +
        df[COL_MAP['bd']]     * ABS_RATIO['bd']
    )
    df['_abs_gap_theory'] = df[COL_MAP['abs_mkt']] - df['_abs_cost_theory']

    df['_cracker_margin'] = (
        df[COL_MAP['et']] * CRACKER_YIELDS['et'] +
        df[COL_MAP['pr']] * CRACKER_YIELDS['pr'] +
        df[COL_MAP['bd']] * CRACKER_YIELDS['bd'] +
        df[COL_MAP['bz']] * CRACKER_YIELDS['bz'] -
        df[COL_MAP['nap']] - CRACKER_OPEX
    )

    df['_bz_spread_ara'] = df[COL_MAP['bz']] - df[COL_MAP['bz_ara']]
    df['_bz_usg_mt'] = df[COL_MAP['bz_usg']] * BZ_USG_TO_MT
    df['_bz_spread_usg'] = df[COL_MAP['bz']] - df['_bz_usg_mt']

    latest = df.iloc[-1]
    hist8 = df.tail(8).copy()

    base_row = choose_base_row(df, date_col)
    cm = latest['_cracker_margin']
    print(f"[구글시트] {len(df)}주 | 최신: {latest[date_col].strftime('%Y-%m-%d')}")
    print(f"  WTI={latest[COL_MAP['wti']]:.2f} | ET={latest[COL_MAP['et']]:.0f} | NAP={latest[COL_MAP['nap']]:.0f} | "
          f"BD={latest[COL_MAP['bd']]:.0f} | PR={latest[COL_MAP['pr']]:.0f} | BZ={latest[COL_MAP['bz']]:.0f}")
    print(f"  ★ 크래커마진={cm:+.0f} | ABS Gap={latest['_abs_gap']:.0f} | SM Margin={latest['_sm_margin']:.0f}")

    if base_row is not None:
        print(f"  [Base] {pd.to_datetime(base_row[date_col]).strftime('%Y-%m-%d')} | "
              f"WTI={base_row[COL_MAP['wti']]:.2f} | NAP={base_row[COL_MAP['nap']]:.0f}")

    return latest, df, hist8, base_row


def choose_base_row(df: pd.DataFrame, date_col: str):
    """최신 행 직전 1주를 기본 base로 사용. 없으면 최근 4주 평균 row 생성."""
    if len(df) >= BASE_LOOKBACK_WEEKS + 1:
        return df.iloc[-(BASE_LOOKBACK_WEEKS + 1)]
    if len(df) >= 2:
        recent = df.iloc[:-1].tail(min(FALLBACK_AVG_WEEKS, len(df) - 1))
        if not recent.empty:
            avg = recent.mean(numeric_only=True)
            avg[date_col] = recent[date_col].max()
            return avg
    return None


# ──────────────────────────────────────────────
# 4. 자동 회귀계수
# ──────────────────────────────────────────────
def calc_regression(df):
    global NAP_SENS_FOR_EQUIV
    wti_col = COL_MAP['wti']
    raw_targets = {
        'bz': COL_MAP['bz'],
        'et': COL_MAP['et'],
        'sm': COL_MAP['sm_cn'],
        'an': COL_MAP['an'],
        'bd': COL_MAP['bd'],
        'nap': COL_MAP['nap'],
        'pr': COL_MAP['pr'],
        'bz_ara': COL_MAP['bz_ara'],
        'bz_usg': COL_MAP['bz_usg'],
    }
    derived_targets = {
        'sm_cost':   '_sm_cost',
        'sm_margin': '_sm_margin',
        'abs_cost':  '_abs_cost',
        'abs_mkt':   COL_MAP['abs_mkt'],
        'abs_gap':   '_abs_gap',
    }

    sens = dict(DEFAULT_SENS)
    r2   = {k: None for k in sens}
    n    = 0
    R2_THRESHOLD = 0.5

    print("\n[ 자동 회귀계수 v6.5 ]")
    for key, col in {**raw_targets, **derived_targets}.items():
        if col not in df.columns:
            continue
        valid = df[[wti_col, col]].dropna()
        n_pts = len(valid)
        if n_pts < 6:
            continue
        x = valid[wti_col].values
        y = valid[col].values
        m, _ = np.polyfit(x, y, 1)
        yp = m * x + np.mean(y - m * x)
        ss_r = np.sum((y - yp) ** 2)
        ss_t = np.sum((y - np.mean(y)) ** 2)
        r2v = round(1 - ss_r / ss_t, 3) if ss_t > 0 else 0
        r2[key] = r2v
        if r2v >= R2_THRESHOLD:
            sens[key] = round(float(m), 3)
            flag = '✓ 자동갱신'
        else:
            flag = '→ 기본값 유지'
        print(f"  {key:14s}: m={m:+7.2f}  R²={r2v:.3f}  n={n_pts}  {flag}")
        n = max(n, n_pts)

    NAP_SENS_FOR_EQUIV = sens['nap']
    print(f"\n  ★ WTI 등가 기준: NAP sens={NAP_SENS_FOR_EQUIV:.2f}")
    return sens, r2, n


# ──────────────────────────────────────────────
# 5. WTI — 구글시트 주간가 그대로 사용
# ──────────────────────────────────────────────
def get_wti(fallback=67.02):
    print(f"[WTI] 구글시트 주간가 사용: ${fallback:.2f}")
    return fallback, "구글시트 주간가"


# ──────────────────────────────────────────────
# 6. 현재 원가 계산
# ──────────────────────────────────────────────
def calc_costs(latest, wti_rt, sens, risk_premium=0, et_override=None):
    wti_gs      = safe_float(latest[COL_MAP['wti']], 0.0)
    bz_act      = safe_float(latest[COL_MAP['bz']], 0.0)
    et_act      = safe_float(latest[COL_MAP['et']], 0.0)
    nap_act     = safe_float(latest[COL_MAP['nap']], 0.0)
    sm_act      = safe_float(latest[COL_MAP['sm_cn']], 0.0)
    bd_act      = safe_float(latest[COL_MAP['bd']], 0.0)
    an_act      = safe_float(latest[COL_MAP['an']], 0.0)
    pr_act      = safe_float(latest[COL_MAP['pr']], 0.0)
    abs_mkt_act = safe_float(latest[COL_MAP['abs_mkt']], 0.0)

    abs_cost_act    = safe_float(latest['_abs_cost'], 0.0)
    abs_gap_act     = safe_float(latest['_abs_gap'], 0.0)
    sm_cost_act     = safe_float(latest['_sm_cost'], 0.0)
    sm_margin_act   = safe_float(latest['_sm_margin'], 0.0)
    sm_cost_th_act  = safe_float(latest['_sm_cost_theory'], 0.0)
    sm_marg_th_act  = safe_float(latest['_sm_margin_theory'], 0.0)
    abs_cost_th_act = safe_float(latest['_abs_cost_theory'], 0.0)
    abs_gap_th_act  = safe_float(latest['_abs_gap_theory'], 0.0)
    cracker_act     = safe_float(latest['_cracker_margin'], 0.0)

    bz_ara_act    = safe_float(latest.get(COL_MAP['bz_ara'], np.nan), np.nan)
    bz_usg_mt_act = safe_float(latest.get('_bz_usg_mt', np.nan), np.nan)

    d_wti = wti_rt - wti_gs
    wti_risk_equiv = risk_premium / NAP_SENS_FOR_EQUIV if NAP_SENS_FOR_EQUIV else 0.0
    d_total = d_wti + wti_risk_equiv

    bz_adj  = round(bz_act  + d_total * sens['bz'], 1)
    et_adj  = round(et_act  + d_total * sens['et'], 1)
    nap_adj = round(nap_act + d_total * sens['nap'], 1)
    sm_adj  = round(sm_act  + d_total * sens['sm'], 1)
    an_adj  = round(an_act  + d_total * sens['an'], 1)
    pr_adj  = round(pr_act  + d_total * sens['pr'], 1)
    bz_ara_adj = round(bz_ara_act + d_total * sens.get('bz_ara', DEFAULT_SENS['bz_ara']), 1) if not np.isnan(bz_ara_act) else np.nan
    bz_usg_adj = round(bz_usg_mt_act + d_total * sens.get('bz_usg', DEFAULT_SENS['bz_usg']) * BZ_USG_TO_MT, 1) if not np.isnan(bz_usg_mt_act) else np.nan

    et_sim = float(et_override) if et_override is not None else et_adj

    cracker_sim = calc_cracker_margin(et_sim, pr_adj, bd_act + d_total * sens['bd'], bz_adj, nap_adj)
    cracker_delta = cracker_sim - cracker_act
    bd_tight_prem = min(max(0, -cracker_delta) * BD_TIGHT_SCALE, BD_TIGHT_MAX)

    bd_base = round(bd_act + d_total * sens['bd'], 1)
    bd_adj  = round(bd_base + bd_tight_prem, 1)

    sm_cost_adj = round(
        bz_adj * SM_COST_RATIO['bz'] +
        et_sim * SM_COST_RATIO['et'] +
        nap_adj * SM_COST_RATIO['nap'] +
        SM_COST_RATIO['fixed'], 1
    )
    sm_margin_adj = round(sm_margin_act + d_wti * sens['sm_margin'], 1)

    sm_cost_th_adj = round(
        bz_adj * SM_THEORY_RATIO['bz'] +
        et_sim * SM_THEORY_RATIO['et'] +
        SM_THEORY_RATIO['fixed'], 1
    )
    sm_marg_th_adj = round(sm_adj - sm_cost_th_adj, 1)

    abs_cost_adj = round(
        sm_adj * ABS_RATIO['sm'] +
        an_adj * ABS_RATIO['an'] +
        bd_adj * ABS_RATIO['bd'], 1
    )

    abs_mkt_adj = round(abs_mkt_act + d_wti * sens['abs_mkt'] + risk_premium, 1)
    abs_gap_adj = round(abs_mkt_adj - abs_cost_adj, 1)

    abs_cost_th_adj = round(
        sm_cost_th_adj * ABS_RATIO['sm'] +
        an_adj * ABS_RATIO['an'] +
        bd_adj * ABS_RATIO['bd'], 1
    )
    abs_gap_th_adj = round(abs_mkt_adj - abs_cost_th_adj, 1)

    bz_spread_ara = round(bz_adj - bz_ara_adj, 1) if not np.isnan(bz_ara_adj) else np.nan
    bz_spread_usg = round(bz_adj - bz_usg_adj, 1) if not np.isnan(bz_usg_adj) else np.nan

    return {
        'WTI_RT': round(wti_rt, 2),
        'WTI_GS': round(wti_gs, 2),
        'WTI_Delta': round(d_wti, 2),
        'WTI_Risk_Equiv': round(wti_risk_equiv, 2),
        'WTI_Total_Delta': round(d_total, 2),
        'Risk_Premium': risk_premium,

        'NAP': nap_adj, 'NAP_Actual': round(nap_act, 1),
        'BZ': bz_adj, 'BZ_Actual': round(bz_act, 1),
        'ET': et_sim, 'ET_Actual': round(et_act, 1),
        'ET_WTI_Adj': et_adj,
        'SM_Market': sm_adj, 'SM_Actual': round(sm_act, 1),
        'BD': bd_adj, 'BD_Actual': round(bd_act, 1),
        'BD_Base': bd_base,
        'BD_Tight_Prem': round(bd_tight_prem, 1),
        'AN': an_adj, 'AN_Actual': round(an_act, 1),
        'PR': pr_adj, 'PR_Actual': round(pr_act, 1),
        'BZ_ARA': bz_ara_adj, 'BZ_ARA_Actual': round_or_nan(bz_ara_act, 1),
        'BZ_USG_MT': bz_usg_adj, 'BZ_USG_Actual': round_or_nan(bz_usg_mt_act, 1),
        'BZ_Spread_ARA': bz_spread_ara,
        'BZ_Spread_USG': bz_spread_usg,

        'Cracker_Margin': round(cracker_sim, 1),
        'Cracker_Margin_Act': round(cracker_act, 1),
        'Cracker_Delta': round(cracker_delta, 1),

        'SM_Cost': round(sm_cost_adj, 1), 'SM_Cost_Actual': round(sm_cost_act, 1),
        'SM_Margin': round(sm_margin_adj, 1), 'SM_Margin_Actual': round(sm_margin_act, 1),
        'SM_Cost_Theory': round(sm_cost_th_adj, 1), 'SM_Cost_Theory_Actual': round(sm_cost_th_act, 1),
        'SM_Margin_Theory': round(sm_marg_th_adj, 1), 'SM_Margin_Theory_Actual': round(sm_marg_th_act, 1),

        'ABS_Market': round(abs_mkt_adj, 1), 'ABS_Mkt_Actual': round(abs_mkt_act, 1),
        'ABS_Cost': round(abs_cost_adj, 1), 'ABS_Cost_Actual': round(abs_cost_act, 1),
        'ABS_Gap': round(abs_gap_adj, 1), 'ABS_Gap_Actual': round(abs_gap_act, 1),
        'ABS_Cost_Theory': round(abs_cost_th_adj, 1), 'ABS_Cost_Theory_Actual': round(abs_cost_th_act, 1),
        'ABS_Gap_Theory': round(abs_gap_th_adj, 1), 'ABS_Gap_Theory_Actual': round(abs_gap_th_act, 1),
    }


# ──────────────────────────────────────────────
# 7. Iran Premium 계산
# ──────────────────────────────────────────────
def build_base_snapshot(base_row) -> Dict[str, float]:
    if base_row is None:
        return {}
    return {
        'WTI': safe_float(base_row[COL_MAP['wti']], np.nan),
        'NAP': safe_float(base_row[COL_MAP['nap']], np.nan),
        'ET': safe_float(base_row[COL_MAP['et']], np.nan),
        'BZ': safe_float(base_row[COL_MAP['bz']], np.nan),
        'SM': safe_float(base_row[COL_MAP['sm_cn']], np.nan),
        'AN': safe_float(base_row[COL_MAP['an']], np.nan),
        'BD': safe_float(base_row[COL_MAP['bd']], np.nan),
        'PR': safe_float(base_row[COL_MAP['pr']], np.nan),
        'ABS_Market': safe_float(base_row[COL_MAP['abs_mkt']], np.nan),
        'SM_Cost': safe_float(base_row['_sm_cost'], np.nan),
        'SM_Margin': safe_float(base_row['_sm_margin'], np.nan),
        'ABS_Cost': safe_float(base_row['_abs_cost'], np.nan),
        'ABS_Gap': safe_float(base_row['_abs_gap'], np.nan),
        'ABS_Cost_Theory': safe_float(base_row['_abs_cost_theory'], np.nan),
        'ABS_Gap_Theory': safe_float(base_row['_abs_gap_theory'], np.nan),
        'Cracker_Margin': safe_float(base_row['_cracker_margin'], np.nan),
    }


def fair_from_wti(base_value: float, base_wti: float, cur_wti: float, sens: float) -> float:
    if any(pd.isna(x) for x in [base_value, base_wti, cur_wti, sens]):
        return np.nan
    return base_value + (cur_wti - base_wti) * sens


def calc_iran_premium(current: Dict[str, float], sens: Dict[str, float], base_snapshot: Dict[str, float]) -> Dict[str, float]:
    """유가로 설명되는 정상값 초과분을 Iran Premium으로 정의."""
    if not base_snapshot or pd.isna(base_snapshot.get('WTI', np.nan)):
        return {}

    base_wti = base_snapshot['WTI']
    cur_wti = current['WTI_GS']

    nap_fair = fair_from_wti(base_snapshot['NAP'], base_wti, cur_wti, sens['nap'])
    et_fair  = fair_from_wti(base_snapshot['ET'],  base_wti, cur_wti, sens['et'])
    bz_fair  = fair_from_wti(base_snapshot['BZ'],  base_wti, cur_wti, sens['bz'])
    sm_fair  = fair_from_wti(base_snapshot['SM'],  base_wti, cur_wti, sens['sm'])
    an_fair  = fair_from_wti(base_snapshot['AN'],  base_wti, cur_wti, sens['an'])
    bd_fair  = fair_from_wti(base_snapshot['BD'],  base_wti, cur_wti, sens['bd'])
    pr_fair  = fair_from_wti(base_snapshot['PR'],  base_wti, cur_wti, sens['pr'])

    sm_cost_fair = fair_from_wti(base_snapshot['SM_Cost'], base_wti, cur_wti, sens['sm_cost'])
    abs_cost_fair = fair_from_wti(base_snapshot['ABS_Cost'], base_wti, cur_wti, sens['abs_cost'])
    abs_gap_fair = fair_from_wti(base_snapshot['ABS_Gap'], base_wti, cur_wti, sens['abs_gap'])

    nap_prem = max(0.0, current['NAP'] - nap_fair) if not pd.isna(nap_fair) else np.nan
    et_prem  = current['ET'] - et_fair if not pd.isna(et_fair) else np.nan
    bz_prem  = current['BZ'] - bz_fair if not pd.isna(bz_fair) else np.nan
    sm_prem  = current['SM_Market'] - sm_fair if not pd.isna(sm_fair) else np.nan
    an_prem  = current['AN'] - an_fair if not pd.isna(an_fair) else np.nan
    bd_prem  = current['BD'] - bd_fair if not pd.isna(bd_fair) else np.nan
    pr_prem  = current['PR'] - pr_fair if not pd.isna(pr_fair) else np.nan

    sm_cost_prem = max(0.0, current['SM_Cost'] - sm_cost_fair) if not pd.isna(sm_cost_fair) else np.nan
    abs_cost_prem = max(0.0, current['ABS_Cost'] - abs_cost_fair) if not pd.isna(abs_cost_fair) else np.nan
    abs_gap_prem = current['ABS_Gap'] - abs_gap_fair if not pd.isna(abs_gap_fair) else np.nan

    sm_margin_impact = (sm_prem - sm_cost_prem) if not any(pd.isna(x) for x in [sm_prem, sm_cost_prem]) else np.nan
    nap_wti_equiv = (nap_prem / NAP_SENS_FOR_EQUIV) if (not pd.isna(nap_prem) and NAP_SENS_FOR_EQUIV) else np.nan

    return {
        'Base_WTI': round_or_nan(base_wti, 2),
        'NAP_Fair': round_or_nan(nap_fair, 1),
        'ET_Fair': round_or_nan(et_fair, 1),
        'BZ_Fair': round_or_nan(bz_fair, 1),
        'SM_Fair': round_or_nan(sm_fair, 1),
        'AN_Fair': round_or_nan(an_fair, 1),
        'BD_Fair': round_or_nan(bd_fair, 1),
        'PR_Fair': round_or_nan(pr_fair, 1),
        'SM_Cost_Fair': round_or_nan(sm_cost_fair, 1),
        'ABS_Cost_Fair': round_or_nan(abs_cost_fair, 1),
        'ABS_Gap_Fair': round_or_nan(abs_gap_fair, 1),

        'IranPremium_NAP': round_or_nan(nap_prem, 1),
        'IranPremium_ET': round_or_nan(et_prem, 1),
        'IranPremium_BZ': round_or_nan(bz_prem, 1),
        'IranPremium_SM': round_or_nan(sm_prem, 1),
        'IranPremium_AN': round_or_nan(an_prem, 1),
        'IranPremium_BD': round_or_nan(bd_prem, 1),
        'IranPremium_PR': round_or_nan(pr_prem, 1),

        'IranPremium_SM_Cost': round_or_nan(sm_cost_prem, 1),
        'IranPremium_ABS_Cost': round_or_nan(abs_cost_prem, 1),
        'IranPremium_ABS_Gap': round_or_nan(abs_gap_prem, 1),
        'IranImpact_SM_Margin': round_or_nan(sm_margin_impact, 1),
        'IranPremium_NAP_WTI_Equiv': round_or_nan(nap_wti_equiv, 2),
    }


# ──────────────────────────────────────────────
# 8. 차트
# ──────────────────────────────────────────────
def generate_report(current, hist8, latest, sens, r2, n_reg, wti_source, iran_premium=None):
    plt.style.use('dark_background')
    fig = plt.figure(figsize=(18, 10), facecolor='#0f172a')

    date_col = hist8.columns[0]
    gs_date = pd.to_datetime(latest[date_col]).strftime('%Y-%m-%d')
    dates = [pd.to_datetime(d).strftime('%m/%d') for d in hist8[date_col]]
    x = range(len(dates))

    def border(ax):
        for sp in ax.spines.values():
            sp.set_edgecolor('#334155')

    # 1 ABS Gap
    ax1 = fig.add_subplot(2, 2, 1); ax1.set_facecolor('#1e293b')
    abs_gap_h = hist8['_abs_gap'].tolist()
    ax1.bar(list(x), abs_gap_h, color=['#10b981' if g >= 0 else '#ef4444' for g in abs_gap_h], alpha=0.85)
    ax1.axhline(0, color='white', linewidth=1, alpha=0.5)
    ax1.set_xticks(list(x)); ax1.set_xticklabels(dates, rotation=30, fontsize=8)
    ax1.set_title(f'ABS Gap | 현재 {current["ABS_Gap"]:+.0f}', fontsize=9, color='#fbbf24')
    border(ax1)

    # 2 SM Margin
    ax2 = fig.add_subplot(2, 2, 2); ax2.set_facecolor('#1e293b')
    sm_margin_h = hist8['_sm_margin'].tolist()
    ax2.bar(list(x), sm_margin_h, color=['#10b981' if g >= 0 else '#ef4444' for g in sm_margin_h], alpha=0.85)
    ax2.axhline(0, color='white', linewidth=1, alpha=0.5)
    ax2.set_xticks(list(x)); ax2.set_xticklabels(dates, rotation=30, fontsize=8)
    ax2.set_title(f'SM Margin | 현재 {current["SM_Margin"]:+.0f}', fontsize=9, color='#60a5fa')
    border(ax2)

    # 3 ET/NAP/BD
    ax3 = fig.add_subplot(2, 2, 3); ax3.set_facecolor('#1e293b')
    ax3.plot(x, hist8[COL_MAP['et']].tolist(), marker='o', label='ET')
    ax3.plot(x, hist8[COL_MAP['nap']].tolist(), marker='^', label='NAP')
    ax3.plot(x, hist8[COL_MAP['bd']].tolist(), marker='s', label='BD')
    ax3.set_xticks(list(x)); ax3.set_xticklabels(dates, rotation=30, fontsize=8)
    ax3.legend(fontsize=8)
    ax3.set_title('ET / NAP / BD', fontsize=9, color='#f97316')
    border(ax3)

    # 4 Iran Premium summary
    ax4 = fig.add_subplot(2, 2, 4); ax4.set_facecolor('#1e293b')
    items = [
        ('NAP', iran_premium.get('IranPremium_NAP', np.nan) if iran_premium else np.nan),
        ('SM Cost', iran_premium.get('IranPremium_SM_Cost', np.nan) if iran_premium else np.nan),
        ('ABS Gap', iran_premium.get('IranPremium_ABS_Gap', np.nan) if iran_premium else np.nan),
    ]
    labels = [k for k, _ in items]
    vals = [0 if pd.isna(v) else v for _, v in items]
    ax4.barh(labels, vals, color=['#94a3b8', '#fb7185', '#34d399'], alpha=0.9)
    ax4.axvline(0, color='white', linewidth=1, alpha=0.5)
    ax4.set_title('Iran Premium (auto)', fontsize=9, color='#fbbf24')
    border(ax4)

    fig.suptitle(
        f'IRAN RISK + CRACKER DASHBOARD v6.5 | 앵커 {gs_date} | {datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")}',
        fontsize=11, fontweight='bold', color='#fbbf24'
    )
    plt.tight_layout(rect=[0, 0.02, 1, 0.95])
    plt.savefig('risk_simulation_report.png', dpi=150, bbox_inches='tight',
                facecolor='#0f172a', edgecolor='none')
    plt.close()
    print("[차트] risk_simulation_report.png 저장 완료 (v6.5)")


# ──────────────────────────────────────────────
# 9. CSV 저장
# ──────────────────────────────────────────────
def save_csv(current, sens, r2, n_reg, wti_source, gs_date, base_row=None, iran_premium=None):
    now = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')
    base_date = ''
    if base_row is not None:
        try:
            base_date = pd.to_datetime(base_row.iloc[0] if hasattr(base_row, 'iloc') else base_row[0]).strftime('%Y-%m-%d')
        except Exception:
            try:
                # Series + first column style
                date_val = base_row[list(base_row.index)[0]]
                base_date = pd.to_datetime(date_val).strftime('%Y-%m-%d')
            except Exception:
                base_date = ''

    row = {
        'UpdateTime': now,
        'WTI_Source': wti_source,
        'GSheet_Date': gs_date,
        'Base_Date': base_date,
        'Reg_N': n_reg,

        'WTI': current['WTI_GS'],
        'WTI_GSheet': current['WTI_GS'],
        'WTI_Delta': current['WTI_Delta'],
        'WTI_Risk_Equiv': current['WTI_Risk_Equiv'],
        'Risk_Premium': current['Risk_Premium'],

        'NAP': current['NAP'],
        'NAP_Actual': current['NAP_Actual'],
        'BZ': current['BZ'], 'BZ_Actual': current['BZ_Actual'],
        'BZ_ARA': current.get('BZ_ARA', ''), 'BZ_ARA_Actual': current.get('BZ_ARA_Actual', ''),
        'BZ_USG_MT': current.get('BZ_USG_MT', ''),
        'BZ_Spread_ARA': current.get('BZ_Spread_ARA', ''),
        'ET': current['ET'], 'ET_Actual': current['ET_Actual'],
        'SM_Market': current['SM_Market'], 'SM_Actual': current['SM_Actual'],
        'PR': current['PR'], 'PR_Actual': current['PR_Actual'],
        'BD': current['BD'], 'BD_Actual': current['BD_Actual'],
        'BD_Tight_Prem': current['BD_Tight_Prem'],
        'AN': current['AN'], 'AN_Actual': current['AN_Actual'],

        'Cracker_Margin': current['Cracker_Margin'],
        'Cracker_Margin_Act': current['Cracker_Margin_Act'],
        'Cracker_Delta': current['Cracker_Delta'],

        'SM_Cost': current['SM_Cost'], 'SM_Cost_Actual': current['SM_Cost_Actual'],
        'SM_Margin': current['SM_Margin'], 'SM_Margin_Actual': current['SM_Margin_Actual'],
        'SM_Cost_Theory': current['SM_Cost_Theory'],
        'SM_Margin_Theory': current['SM_Margin_Theory'],
        'SM_Margin_Theory_Actual': current['SM_Margin_Theory_Actual'],

        'ABS_Market': current['ABS_Market'], 'ABS_Mkt_Actual': current['ABS_Mkt_Actual'],
        'ABS_Cost': current['ABS_Cost'], 'ABS_Cost_Actual': current['ABS_Cost_Actual'],
        'ABS_Gap': current['ABS_Gap'], 'ABS_Gap_Actual': current['ABS_Gap_Actual'],
        'ABS_Cost_Theory': current['ABS_Cost_Theory'],
        'ABS_Gap_Theory': current['ABS_Gap_Theory'],

        'Sens_BZ': sens['bz'], 'R2_BZ': r2.get('bz', ''),
        'Sens_ET': sens['et'], 'R2_ET': r2.get('et', ''),
        'Sens_SM': sens['sm'], 'R2_SM': r2.get('sm', ''),
        'Sens_AN': sens['an'], 'R2_AN': r2.get('an', ''),
        'Sens_BD': sens['bd'], 'R2_BD': r2.get('bd', ''),
        'Sens_NAP': sens['nap'], 'R2_NAP': r2.get('nap', ''),
        'Sens_PR': sens['pr'], 'R2_PR': r2.get('pr', ''),
        'Sens_BZ_ARA': sens.get('bz_ara', DEFAULT_SENS['bz_ara']),
        'Sens_ABS_MKT': sens['abs_mkt'], 'R2_ABS_MKT': r2.get('abs_mkt', ''),
        'Sens_ABS_GAP': sens['abs_gap'], 'R2_ABS_GAP': r2.get('abs_gap', ''),
        'Sens_ABS_COST': sens['abs_cost'],
        'Sens_SM_MARGIN': sens['sm_margin'],
        'Sens_SM_COST': sens['sm_cost'],
        'NAP_Sens_Equiv_Basis': NAP_SENS_FOR_EQUIV,

        'BD_Tight_Scale': BD_TIGHT_SCALE,
        'BD_Tight_Max': BD_TIGHT_MAX,
        'Cracker_Yields_ET': CRACKER_YIELDS['et'],
        'Cracker_Yields_PR': CRACKER_YIELDS['pr'],
        'Cracker_Yields_BD': CRACKER_YIELDS['bd'],
        'Cracker_Yields_BZ': CRACKER_YIELDS['bz'],
    }

    if iran_premium:
        row.update(iran_premium)

    if base_row is not None:
        base_snapshot = build_base_snapshot(base_row)
        row.update({
            'Base_WTI': base_snapshot.get('WTI', ''),
            'Base_NAP': base_snapshot.get('NAP', ''),
            'Base_ET': base_snapshot.get('ET', ''),
            'Base_BZ': base_snapshot.get('BZ', ''),
            'Base_SM': base_snapshot.get('SM', ''),
            'Base_AN': base_snapshot.get('AN', ''),
            'Base_BD': base_snapshot.get('BD', ''),
            'Base_PR': base_snapshot.get('PR', ''),
            'Base_SM_Cost': base_snapshot.get('SM_Cost', ''),
            'Base_ABS_Cost': base_snapshot.get('ABS_Cost', ''),
            'Base_ABS_Gap': base_snapshot.get('ABS_Gap', ''),
        })

    pd.DataFrame([row]).to_csv('simulation_result.csv', index=False)
    print("[CSV] simulation_result.csv 저장 완료 (v6.5)")


# ──────────────────────────────────────────────
# 10. 메인
# ──────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 72)
    print("Iran Risk × ABS/SM 원가 시뮬레이션 v6.5")
    print("Iran Premium 자동산출 | 크래커마진→BD타이트 | HTML 롤링예측 기준일 저장")
    print("=" * 72)

    setup_font()
    latest, df_all, hist8, base_row = load_gsheet()
    if latest is None:
        print("구글시트 로드 실패")
        raise SystemExit(1)

    date_col = hist8.columns[0]
    gs_date = pd.to_datetime(latest[date_col]).strftime('%Y-%m-%d')

    sens, r2, n_reg = calc_regression(df_all)
    wti_rt, wti_src = get_wti(fallback=float(latest[COL_MAP['wti']]))
    current = calc_costs(latest, wti_rt, sens, risk_premium=0)

    base_snapshot = build_base_snapshot(base_row)
    iran_premium = calc_iran_premium(current, sens, base_snapshot)

    print(f"\n{'─'*72}")
    print(f"  GSheet Date   : {gs_date}")
    print(f"  WTI           : ${current['WTI_GS']:.2f} (주간가 기준)")
    print(f"  NAP           : ${current['NAP']:.0f}/t")
    print(f"  ET            : ${current['ET_Actual']:.0f} → ${current['ET']:.0f}/t")
    print(f"  ★ 크래커마진  : 실측 ${current['Cracker_Margin_Act']:+.0f} → ${current['Cracker_Margin']:+.0f}")
    print(f"  ★ BD 타이트   : +${current['BD_Tight_Prem']:.0f}/t")
    print(f"  SM Cost       : ${current['SM_Cost']:.1f}")
    print(f"  ABS Gap       : ${current['ABS_Gap']:+.1f}")
    print(f"{'─'*72}")

    if iran_premium:
        print("[ Iran Premium Auto ]")
        print(f"  Base WTI      : ${iran_premium.get('Base_WTI', np.nan):.2f}")
        print(f"  NAP Fair      : ${iran_premium.get('NAP_Fair', np.nan):.1f} | Premium {iran_premium.get('IranPremium_NAP', np.nan):+.1f}")
        print(f"  SM Cost Fair  : ${iran_premium.get('SM_Cost_Fair', np.nan):.1f} | Premium {iran_premium.get('IranPremium_SM_Cost', np.nan):+.1f}")
        print(f"  ABS Gap Fair  : ${iran_premium.get('ABS_Gap_Fair', np.nan):+.1f} | Premium {iran_premium.get('IranPremium_ABS_Gap', np.nan):+.1f}")
        print(f"  NAP WTI eq    : ${iran_premium.get('IranPremium_NAP_WTI_Equiv', np.nan):+.2f}/bbl")
        print(f"{'─'*72}")

    generate_report(current, hist8, latest, sens, r2, n_reg, wti_src, iran_premium=iran_premium)
    save_csv(current, sens, r2, n_reg, wti_src, gs_date, base_row=base_row, iran_premium=iran_premium)

    print("\n완료 v6.5")
    print("=" * 72)
