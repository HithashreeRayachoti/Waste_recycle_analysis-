"""Seasonality test: is there a month-of-year effect on the rate, with district fixed effects?

rate ~ C(month_of_year) + C(district_id) + C(year), standard errors clustered by district,
joint Wald F-test that all month effects are zero.
"""

from __future__ import annotations

import duckdb
import pandas as pd
import statsmodels.formula.api as smf

MONTH_NAMES = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def load_panel(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return con.execute(
        """select district_id, month, year(month) as year, month(month) as month_of_year,
                  diversion_rate, recycling_rate_core
           from gold.district_month where is_complete"""
    ).df()


def month_effect_test(panel: pd.DataFrame, rate_column: str) -> dict:
    df = panel.dropna(subset=[rate_column]).copy()
    if df["month_of_year"].nunique() < 12 or df["district_id"].nunique() < 2:
        return {"rate": rate_column, "status": "insufficient data", "n": int(len(df))}
    df["rate"] = df[rate_column] * 100  # percentage points, easier to read
    model = smf.ols("rate ~ C(month_of_year) + C(district_id) + C(year)", data=df)
    res = model.fit(cov_type="cluster", cov_kwds={"groups": pd.factorize(df["district_id"])[0]})
    month_terms = [p for p in res.params.index if p.startswith("C(month_of_year)")]
    test = res.f_test(", ".join(f"{t} = 0" for t in month_terms))
    effects = [{"month_of_year": 1, "month": "Jan", "effect_pp": 0.0, "ci_low": 0.0, "ci_high": 0.0}]
    ci = res.conf_int()
    for t in month_terms:
        m = int(t.split("[T.")[1].rstrip("]"))
        effects.append(
            {
                "month_of_year": m,
                "month": MONTH_NAMES[m - 1],
                "effect_pp": float(res.params[t]),
                "ci_low": float(ci.loc[t, 0]),
                "ci_high": float(ci.loc[t, 1]),
            }
        )
    return {
        "rate": rate_column,
        "status": "ok",
        "n": int(res.nobs),
        "districts": int(df["district_id"].nunique()),
        "years": sorted(int(y) for y in df["year"].unique()),
        "f_stat": float(test.fvalue),
        "p_value": float(test.pvalue),
        "df_num": int(test.df_num),
        "df_denom": float(test.df_denom),
        "effects_vs_january_pp": sorted(effects, key=lambda e: e["month_of_year"]),
        "method": "OLS with district and year fixed effects; SEs clustered by district; "
        "joint Wald F-test on the 11 month-of-year terms",
    }


def run(con: duckdb.DuckDBPyConnection) -> dict:
    panel = load_panel(con)
    return {
        "recycling_rate_core": month_effect_test(panel, "recycling_rate_core"),
        "diversion_rate": month_effect_test(panel, "diversion_rate"),
    }
