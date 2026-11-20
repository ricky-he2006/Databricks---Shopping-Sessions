#!/usr/bin/env python3
"""Build Shopping Sessions daily reports from the scorer's input directory."""
import csv
import json
import os
from pathlib import Path
import re
from tempfile import TemporaryDirectory

import duckdb


EVENTS = ("event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents")
CLICKS = ("click_id", "user_id", "campaign_id", "ts")
LINKS = ("user_id_a", "user_id_b")
WEEK_MS = 604_800_000


def records(folder, width):
    field = rb'(?:[^",\n]*|"[^",\n]*")'
    pattern = re.compile(b",".join([field] * width))
    for path in sorted(folder.glob("*.csv")):
        with path.open("rb") as source:
            next(source, None)
            for line in source:
                line = line.removesuffix(b"\n")
                if pattern.fullmatch(line):
                    yield line.replace(b'"', b"")


def stage_csv(folder, path, columns):
    # Normalize syntax before DuckDB sees it. Quote fields containing literal CR.
    with path.open("wb") as target:
        target.write((",".join(columns) + "\n").encode())
        for line in records(folder, len(columns)):
            if b"\r" in line:
                line = b",".join(
                    b'"' + field + b'"' if b"\r" in field else field
                    for field in line.split(b",")
                )
            target.write(line + b"\n")


def stage_people(folder, path):
    parents = {}
    sizes = {}

    def root(user):
        while user != parents[user]:
            parents[user] = parents[parents[user]]
            user = parents[user]
        return user

    for line in records(folder, len(LINKS)):
        a, b = line.split(b",")
        if not a or not b:
            continue
        for user in (a, b):
            if user not in parents:
                parents[user] = user
                sizes[user] = 1
        a, b = root(a), root(b)
        if a != b:
            if sizes[a] < sizes[b]:
                a, b = b, a
            parents[b] = a
            sizes[a] += sizes.pop(b)

    smallest = {}
    for user in parents:
        representative = root(user)
        smallest[representative] = min(user, smallest.get(representative, user))

    with path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.writer(output, quoting=csv.QUOTE_ALL, lineterminator="\n")
        writer.writerow(("user_id", "person"))
        for user in parents:
            person = smallest[root(user)]
            if user != person:
                writer.writerow((user.decode(), person.decode()))


def sql_string(value):
    return "'" + str(value).replace("'", "''") + "'"


def csv_scan(path, columns):
    schema = ", ".join(f"'{column}': 'VARCHAR'" for column in columns)
    return (
        f"read_csv({sql_string(path)}, columns = {{{schema}}}, header = true, "
        r"auto_detect = false, delim = ',', new_line = '\n', "
        "quote = '\"')"
    )


def main():
    data = Path(os.environ.get("DATA_DIR", "/data"))
    out = Path(os.environ.get("OUTPUT_DIR", "/output"))
    scratch = os.environ.get("SCRATCH_DIR")
    ncpu = int(os.environ.get("NCPU", min(8, os.cpu_count() or 1)))
    mem_gb = int(os.environ.get("MEM_GB", "8"))
    manifest = json.loads((data / "manifest.json").read_text(encoding="utf-8"))

    with TemporaryDirectory(dir=scratch) as temp, duckdb.connect() as con:
        work = Path(temp)
        stage_people(data / "links", work / "people.csv")
        stage_csv(data / "events", work / "events.csv", EVENTS)
        con.execute(f"SET threads = {ncpu}")
        con.execute(f"SET memory_limit = '{max(1, int(mem_gb * 0.8))}GB'")
        con.execute(f"SET temp_directory = {sql_string(work / 'spill')}")
        con.execute("SET preserve_insertion_order = false")

        con.execute("""
            CREATE MACRO timestamp_ms(value) AS (
                CASE
                    WHEN regexp_full_match(value, '[0-9]+')
                    THEN try_cast(coalesce(nullif(ltrim(value, '0'), ''), '0') AS BIGINT)
                    WHEN regexp_full_match(value,
                        '[0-9]{4}-[0-9]{2}-[0-9]{2}T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])')
                    THEN epoch_ms(try_strptime(left(value, 19), '%Y-%m-%dT%H:%M:%S'))
                         - CASE WHEN right(value, 1) = 'Z' THEN 0 ELSE
                             (cast(substr(value, 21, 2) AS BIGINT) * 60
                              + cast(substr(value, 24, 2) AS BIGINT)) * 60000
                             * CASE WHEN substr(value, 20, 1) = '+' THEN 1 ELSE -1 END
                           END
                END
            )
        """)
        start, end = con.execute(
            "SELECT timestamp_ms(?), timestamp_ms(?)",
            [manifest["window_start"], manifest["window_end"]],
        ).fetchone()
        con.execute(f"""
            CREATE TABLE people AS SELECT * FROM {csv_scan(work / 'people.csv', ('user_id', 'person'))};
            CREATE TABLE products AS SELECT * FROM {csv_scan(data / 'products.csv', ('product_id', 'category'))};
            CREATE TABLE campaigns AS SELECT * FROM {csv_scan(data / 'campaigns.csv', ('campaign_id', 'channel'))};
        """)
        con.execute(f"""
            CREATE TABLE session_events AS
            WITH dedup AS (
                SELECT *
                FROM {csv_scan(work / 'events.csv', EVENTS)}
                WHERE event_id IS NOT NULL
                QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY ingest_ts) = 1
            ), parsed AS (
                SELECT user_id, timestamp_ms(ts) AS ms, timestamp_ms(ingest_ts) AS ingested,
                       event_type, product_id,
                       CASE WHEN event_type = 'purchase' AND regexp_full_match(price_cents, '[0-9]+')
                            THEN try_cast(ltrim(price_cents, '0') AS INTEGER) END AS price
                FROM dedup
                WHERE user_id IS NOT NULL AND product_id IS NOT NULL
                  AND event_type IN ('view', 'cart', 'purchase')
            ), valid AS (
                SELECT coalesce(people.person, parsed.user_id) AS person, ms,
                       CASE WHEN event_type = 'purchase' THEN product_id END AS purchased_product,
                       coalesce(price, 0) AS revenue
                FROM parsed LEFT JOIN people USING (user_id)
                WHERE ms >= {start} AND ms < {end} AND ingested - ms <= 86400000
                  AND (event_type <> 'purchase' OR price BETWEEN 1 AND 10000000)
            ), gaps AS (
                SELECT *, lag(ms) OVER (PARTITION BY person ORDER BY ms) AS prev_ms
                FROM valid
            )
            SELECT person, purchased_product, revenue,
                   max(CASE WHEN prev_ms IS NULL OR ms - prev_ms > 1800000 THEN ms END)
                       OVER (PARTITION BY person ORDER BY ms
                             RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_start
            FROM gaps
        """)
        (work / "events.csv").unlink()
        con.execute("""
            CREATE TABLE sessions AS
            SELECT person, session_start, CAST(make_timestamp_ms(session_start) AS DATE) AS day,
                   count(*) AS events, sum(revenue)::BIGINT AS revenue_cents
            FROM session_events
            GROUP BY person, session_start
        """)
        write_report(con, out, "user_daily", """
            WITH categories AS (
                SELECT person, CAST(make_timestamp_ms(session_start) AS DATE) AS day,
                       coalesce(category, 'unknown') AS category, sum(revenue) AS revenue
                FROM session_events
                LEFT JOIN products ON purchased_product = product_id
                WHERE purchased_product IS NOT NULL
                GROUP BY person, day, coalesce(category, 'unknown')
            ), top_categories AS (
                SELECT person, day, category
                FROM categories
                QUALIFY row_number() OVER (PARTITION BY person, day ORDER BY revenue DESC, category) = 1
            ), daily AS (
                SELECT person, day, count(*) AS sessions, sum(events)::BIGINT AS events,
                       sum(revenue_cents)::BIGINT AS revenue_cents
                FROM sessions GROUP BY person, day
            )
            SELECT person AS user_id, day, sessions, events, revenue_cents, category AS top_category
            FROM daily LEFT JOIN top_categories USING (person, day)
        """)
        con.execute("DROP TABLE session_events")
        stage_csv(data / "clicks", work / "clicks.csv", CLICKS)
        con.execute(f"""
            CREATE TABLE clicks AS
            WITH dedup AS (
                SELECT *
                FROM {csv_scan(work / 'clicks.csv', CLICKS)}
                WHERE click_id IS NOT NULL
                QUALIFY row_number() OVER (PARTITION BY click_id) = 1
            ), parsed AS (
                SELECT click_id, user_id, campaign_id, timestamp_ms(ts) AS ms
                FROM dedup
                WHERE user_id IS NOT NULL
            )
            SELECT click_id, coalesce(people.person, parsed.user_id) AS person, campaign_id, ms
            FROM parsed
            JOIN campaigns USING (campaign_id)
            LEFT JOIN people USING (user_id)
            WHERE ms >= {start - WEEK_MS} AND ms < {end}
        """)
        (work / "clicks.csv").unlink()
        write_report(con, out, "campaign_daily", f"""
            WITH touchpoints AS (
                SELECT person, ms, arg_min(campaign_id, click_id) AS campaign_id
                FROM clicks GROUP BY person, ms
            ), attributed AS (
                SELECT day, revenue_cents,
                       CASE WHEN session_start - ms <= {WEEK_MS} THEN campaign_id
                            ELSE 'organic' END AS campaign_id
                FROM sessions ASOF LEFT JOIN touchpoints
                  ON sessions.person = touchpoints.person AND session_start >= ms
            ), credited AS (
                SELECT campaign_id, day, count(*) AS sessions,
                       count(*) FILTER (WHERE revenue_cents > 0) AS purchasing_sessions,
                       sum(revenue_cents)::BIGINT AS revenue_cents
                FROM attributed GROUP BY campaign_id, day
            ), campaign_clicks AS (
                SELECT DISTINCT person, campaign_id, ms FROM clicks
            ), gaps AS (
                SELECT *, lag(ms) OVER (PARTITION BY person, campaign_id ORDER BY ms) AS prev_ms
                FROM campaign_clicks
            ), coverage AS (
                SELECT *, max(CASE WHEN prev_ms IS NULL OR ms - prev_ms > {WEEK_MS} THEN ms END)
                           OVER (PARTITION BY person, campaign_id ORDER BY ms
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_ms
                FROM gaps
            ), intervals AS (
                -- Merged click windows give each campaign at most one assist per session.
                SELECT person, campaign_id, first_ms, max(ms) + {WEEK_MS} AS last_ms
                FROM coverage GROUP BY person, campaign_id, first_ms
            ), assisted AS (
                SELECT campaign_id, day, count(*) AS assisted_sessions
                FROM sessions JOIN intervals
                  ON sessions.person = intervals.person AND session_start BETWEEN first_ms AND last_ms
                GROUP BY campaign_id, day
            )
            SELECT daily.campaign_id,
                   CASE WHEN daily.campaign_id = 'organic' THEN 'organic' ELSE channel END AS channel, day,
                   coalesce(sessions, 0) AS sessions,
                   coalesce(purchasing_sessions, 0) AS purchasing_sessions,
                   coalesce(revenue_cents, 0) AS revenue_cents,
                   coalesce(assisted_sessions, 0) AS assisted_sessions
            FROM (SELECT * FROM credited FULL JOIN assisted USING (campaign_id, day)) AS daily
            LEFT JOIN campaigns ON daily.campaign_id = campaigns.campaign_id
        """)


def write_report(con, out, table, query):
    folder = out / table
    folder.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY ({query}) TO {sql_string(folder / 'part-0.parquet')} (FORMAT parquet)")


if __name__ == "__main__":
    main()
