# -*- coding: utf-8 -*-
"""Compare comdir June 2026 SQL vs 1C report targets."""
from __future__ import annotations

from datetime import date

from comdir.calc_plan_fact_dengi import (
    calc_expected as dengi_exp,
    calc_fact as dengi_fact,
    calc_plan as dengi_plan,
    connect,
    to_1c_dt,
)
from comdir.calc_plan_fact_otgruzki import (
    calc_expected as otg_exp,
    calc_fact as otg_fact,
    calc_mp_plan as otg_plan,
)
from comdir.calc_plan_fact_dogovory import (
    calc_expected as dog_exp,
    calc_fact as dog_fact,
    calc_mp_plan as dog_plan,
)

# Targets from 1C «План-фактный анализ продаж» June 2026 (screenshots)
TARGET = {
    "dengi_plan": 234_424_941,
    "dengi_expected": 3_423_618_625,
    "dengi_fact": 50_760_580.19,
    "otg_plan": 3_453_387_262,
    "otg_fact": 144_919_782.87,
    # otg expected — from screenshot column «Заказы ожидаемые к отгрузке» (need read)
    "dog_fact": 1_206_621_273.92,
}


def main() -> None:
    y, m = 2026, 6
    p0 = to_1c_dt(date(y, m, 1))
    p_next = to_1c_dt(date(y, m + 1, 1))
    p_year = to_1c_dt(date(y, 1, 1))

    cn = connect()
    cur = cn.cursor()
    cur.execute("SET NOCOUNT ON")

    print("=== ДЕНЬГИ ===")
    dp = dengi_plan(cur, p0, p_next)
    df = dengi_fact(cur, p0, p_next)
    de = dengi_exp(cur, p_year, p_next)
    print(f"plan SQL={sum(dp.values()):,.2f}  1C={TARGET['dengi_plan']:,.2f}")
    print(f"fact SQL={sum(df.values()):,.2f}  1C={TARGET['dengi_fact']:,.2f}")
    print(f"exp  SQL={sum(de.values()):,.2f}  1C={TARGET['dengi_expected']:,.2f}")
    print("plan by dept", {k: round(v, 2) for k, v in sorted(dp.items())})
    print("fact by dept", {k: round(v, 2) for k, v in sorted(df.items())})
    print("exp  by dept", {k: round(v, 2) for k, v in sorted(de.items())})

    print("\n=== ОТГРУЗКИ ===")
    op = otg_plan(cur, p0, p_next)
    of = otg_fact(cur, p0, p_next)
    oe = otg_exp(cur, p_next)
    print(f"plan SQL={sum(op.values()):,.2f}  1C={TARGET['otg_plan']:,.2f}")
    print(f"fact SQL={sum(of.values()):,.2f}  1C={TARGET['otg_fact']:,.2f}")
    print(f"exp  SQL={sum(oe.values()):,.2f}")
    print("fact by dept", {k: round(v, 2) for k, v in sorted(of.items())})
    print("exp  by dept", {k: round(v, 2) for k, v in sorted(oe.items())})

    print("\n=== ДОГОВОРЫ ===")
    gp = dog_plan(cur, p0, p_next)
    gf = dog_fact(cur, p0, p_next)
    ge = dog_exp(cur, p0, p_next)
    print(f"plan SQL={sum(gp.values()):,.2f}")
    print(f"fact SQL={sum(gf.values()):,.2f}  1C={TARGET['dog_fact']:,.2f}")
    print(f"exp  SQL={sum(ge.values()):,.2f}")
    print("fact by dept", {k: round(v, 2) for k, v in sorted(gf.items())})
    print("exp  by dept", {k: round(v, 2) for k, v in sorted(ge.items())})

    cn.close()


if __name__ == "__main__":
    main()
