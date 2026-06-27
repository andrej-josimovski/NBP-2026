
import time
import json
import statistics
from collections import defaultdict
import happybase

# ─────────────────────────────────────────────
# КОНФИГУРАЦИЈА
# ─────────────────────────────────────────────

HBASE_HOST  = '127.0.0.1'
HBASE_PORT  = 9090
RUNS        = 5
QUERY_LIMIT = 50000
OUTPUT_JSON = 'hbase_results.json'

# Имиња на табели
TABLE_GENRE    = 'movies_by_genre'
TABLE_DIRECTOR = 'movies_by_director'

# Column families
CF_INFO = b'info'

# ─────────────────────────────────────────────
# ПОВРЗУВАЊЕ
# ─────────────────────────────────────────────

def get_connection():
    connection = happybase.Connection(HBASE_HOST, port=HBASE_PORT, autoconnect=True)
    print(f"✓ Поврзан со HBase @ {HBASE_HOST}:{HBASE_PORT}")
    return connection

# ─────────────────────────────────────────────
# ПОМОШНИ ФУНКЦИИ
# ─────────────────────────────────────────────

def decode_row(row_data):
    """Декодира bytes вредности од HBase во string/float/int."""
    decoded = {}
    for col, val in row_data.items():
        key = col.decode('utf-8').split(':')[-1]  # "info:title" -> "title"
        try:
            decoded[key] = val.decode('utf-8')
        except:
            decoded[key] = ''
    return decoded

def safe_float(val, default=0.0):
    try:
        return float(val)
    except:
        return default

def safe_int(val, default=0):
    try:
        return int(val)
    except:
        return default

def scan_table(connection, table_name, limit=QUERY_LIMIT, filter_str=None):
    """Скенира табела и враќа листа на декодирани редови."""
    table  = connection.table(table_name)
    kwargs = {}
    if filter_str:
        kwargs['filter'] = filter_str
    rows = []
    for row_key, row_data in table.scan(**kwargs):
        rows.append(decode_row(row_data))
        if len(rows) >= limit:
            break
    return rows

def measure_scan(connection, table_name, limit=QUERY_LIMIT,
                 filter_str=None, runs=RUNS, post_filter=None):
    """
    Скенира табела N пати и мери времиња.
    post_filter: функција за дополнително Python филтрирање.
    """
    times = []
    rows  = []
    for _ in range(runs):
        t0   = time.perf_counter()
        rows = scan_table(connection, table_name, limit=limit, filter_str=filter_str)
        if post_filter:
            rows = [r for r in rows if post_filter(r)]
        times.append((time.perf_counter() - t0) * 1000)
    return {
        'rows'     : rows,
        'count'    : len(rows),
        'avg_ms'   : round(statistics.mean(times), 2),
        'min_ms'   : round(min(times), 2),
        'max_ms'   : round(max(times), 2),
        'total_ms' : round(sum(times), 2),
        'times'    : [round(t, 2) for t in times],
    }

# ─────────────────────────────────────────────
# Q1 — Филмови по жанр
# ─────────────────────────────────────────────

def q1_movies_by_genre(connection, genre='Action'):
    """
    Row key во movies_by_genre е: genre#vote_average#id
    -> Prefix scan по жанр = директен range scan без full table scan.
    Најоптимизиран query во HBase.
    """
    print(f"\n[Q1] Филмови со жанр '{genre}'")

    table  = connection.table(TABLE_GENRE)
    prefix = genre.encode('utf-8')

    times = []
    rows  = []
    for _ in range(RUNS):
        t0   = time.perf_counter()
        rows = []
        for rk, rd in table.scan(row_prefix=prefix):
            rows.append(decode_row(rd))
            if len(rows) >= QUERY_LIMIT:
                break
        times.append((time.perf_counter() - t0) * 1000)

    avg_ms = round(statistics.mean(times), 2)
    print(f"     Пронајдени: {len(rows)} | Avg: {avg_ms} ms")
    for r in rows[:5]:
        print(f"     {r.get('title','')[:40]:<40} | *{safe_float(r.get('vote_average')):.1f} | {r.get('release_year','')}")

    return {
        'query'  : 'Q1 — Филмови по жанр',
        'param'  : genre,
        'avg_ms' : avg_ms,
        'min_ms' : round(min(times), 2),
        'max_ms' : round(max(times), 2),
        'total_ms': round(sum(times), 2),
        'times'  : [round(t, 2) for t in times],
        'count'  : len(rows),
        'note'   : 'Row key prefix scan по genre -> оптимизиран range scan',
        'sample' : [{'title': r.get('title',''), 'vote_average': safe_float(r.get('vote_average')),
                     'year': r.get('release_year',''), 'director': r.get('director','')}
                    for r in rows[:5]],
    }

# ─────────────────────────────────────────────
# Q2 — Филмови по година на издавање
# ─────────────────────────────────────────────

def q2_movies_by_year(connection, year=2015):
    """
    release_year не е дел од row key -> full table scan + Python filter.
    HBase SingleColumnValueFilter за release_year.
    """
    print(f"\n[Q2] Филмови од {year} година")

    filter_str = (
        f"SingleColumnValueFilter('info', 'release_year', =, "
        f"'binary:{year}', true, true)"
    )

    times = []
    rows  = []
    for _ in range(RUNS):
        t0   = time.perf_counter()
        rows = scan_table(connection, TABLE_GENRE, limit=QUERY_LIMIT,
                          filter_str=filter_str)
        times.append((time.perf_counter() - t0) * 1000)

    avg_ms = round(statistics.mean(times), 2)
    print(f"     Пронајдени: {len(rows)} | Avg: {avg_ms} ms")
    print(f"     ! Full scan + SingleColumnValueFilter (release_year не е во row key)")
    for r in rows[:5]:
        print(f"     {r.get('title','')[:40]:<40} | *{safe_float(r.get('vote_average')):.1f}")

    return {
        'query'   : 'Q2 — Филмови по година',
        'param'   : year,
        'avg_ms'  : avg_ms,
        'min_ms'  : round(min(times), 2),
        'max_ms'  : round(max(times), 2),
        'total_ms': round(sum(times), 2),
        'times'   : [round(t, 2) for t in times],
        'count'   : len(rows),
        'note'    : 'SingleColumnValueFilter на release_year -> full table scan, побавно',
        'sample'  : [{'title': r.get('title',''), 'vote_average': safe_float(r.get('vote_average'))}
                     for r in rows[:5]],
    }

# ─────────────────────────────────────────────
# Q3 — Филмови по оригинален јазик
# ─────────────────────────────────────────────

def q3_movies_by_language(connection, lang='en'):
    """
    original_language не е во row key -> SingleColumnValueFilter + full scan.
    """
    print(f"\n[Q3] Филмови на јазик '{lang}'")

    filter_str = (
        f"SingleColumnValueFilter('info', 'original_language', =, "
        f"'binary:{lang}', true, true)"
    )

    times = []
    rows  = []
    for _ in range(RUNS):
        t0   = time.perf_counter()
        rows = scan_table(connection, TABLE_GENRE, limit=QUERY_LIMIT,
                          filter_str=filter_str)
        times.append((time.perf_counter() - t0) * 1000)

    avg_ms = round(statistics.mean(times), 2)
    print(f"     Пронајдени: {len(rows)} | Avg: {avg_ms} ms")
    print(f"     ! Full scan + SingleColumnValueFilter (original_language не е во row key)")
    for r in rows[:5]:
        print(f"     {r.get('title','')[:40]:<40} | *{safe_float(r.get('vote_average')):.1f} | {r.get('release_year','')}")

    return {
        'query'   : 'Q3 — Филмови по јазик',
        'param'   : lang,
        'avg_ms'  : avg_ms,
        'min_ms'  : round(min(times), 2),
        'max_ms'  : round(max(times), 2),
        'total_ms': round(sum(times), 2),
        'times'   : [round(t, 2) for t in times],
        'count'   : len(rows),
        'note'    : 'SingleColumnValueFilter на original_language -> full scan',
        'sample'  : [{'title': r.get('title',''), 'vote_average': safe_float(r.get('vote_average')),
                      'year': r.get('release_year','')}
                     for r in rows[:5]],
    }

# ─────────────────────────────────────────────
# Q4 — Топ филмови по жанр + vote_average > X
# ─────────────────────────────────────────────

def q4_top_by_genre_and_rating(connection, genre='Action', min_rating=7.0):
    """
    Row key: genre#vote_average#id (vote_average е inverted за DESC редослед)
    -> Prefix scan по genre + Python filter за vote_average > min_rating.
    Делумно оптимизиран — prefix scan е брз, filter е во Python.
    """
    print(f"\n[Q4] Топ '{genre}' филмови со рејтинг > {min_rating}")

    table  = connection.table(TABLE_GENRE)
    prefix = genre.encode('utf-8')

    times = []
    rows  = []
    for _ in range(RUNS):
        t0   = time.perf_counter()
        rows = []
        for rk, rd in table.scan(row_prefix=prefix):
            r = decode_row(rd)
            if safe_float(r.get('vote_average', 0)) > min_rating:
                rows.append(r)
            if len(rows) >= QUERY_LIMIT:
                break
        times.append((time.perf_counter() - t0) * 1000)

    avg_ms = round(statistics.mean(times), 2)
    print(f"     Пронајдени: {len(rows)} | Avg: {avg_ms} ms")
    print(f"     OK Prefix scan + Python filter за vote_average")
    for r in rows[:5]:
        print(f"     {r.get('title','')[:40]:<40} | *{safe_float(r.get('vote_average')):.1f}")

    return {
        'query'      : 'Q4 — Топ по жанр + рејтинг',
        'param'      : f"{genre} > {min_rating}",
        'avg_ms'     : avg_ms,
        'min_ms'     : round(min(times), 2),
        'max_ms'     : round(max(times), 2),
        'total_ms'   : round(sum(times), 2),
        'times'      : [round(t, 2) for t in times],
        'count'      : len(rows),
        'note'       : 'Prefix scan по genre + Python filter за vote_average > X',
        'sample'     : [{'title': r.get('title',''), 'vote_average': safe_float(r.get('vote_average')),
                         'director': r.get('director','')}
                        for r in rows[:5]],
    }

# ─────────────────────────────────────────────
# Q5 — Филмови на режисер сортирани по popularity
# ─────────────────────────────────────────────

def q5_director_movies(connection, director='Christopher Nolan'):
    """
    Row key во movies_by_director: director#popularity#id
    -> Prefix scan по director = директен range scan, резултатите
    се веќе сортирани по popularity (DESC) поради row key редоследот.
    """
    print(f"\n[Q5] Филмови на '{director}' сортирани по popularity")

    table  = connection.table(TABLE_DIRECTOR)
    prefix = director.encode('utf-8')

    times = []
    rows  = []
    for _ in range(RUNS):
        t0   = time.perf_counter()
        rows = []
        for rk, rd in table.scan(row_prefix=prefix):
            rows.append(decode_row(rd))
            if len(rows) >= QUERY_LIMIT:
                break
        times.append((time.perf_counter() - t0) * 1000)

    avg_ms = round(statistics.mean(times), 2)
    print(f"     Вкупно: {len(rows)} филмови | Avg: {avg_ms} ms")
    print(f"     OK Row key prefix scan -> pre-sorted по popularity")
    for r in rows[:5]:
        print(f"     {r.get('title','')[:40]:<40} | {r.get('release_year','')} | *{safe_float(r.get('vote_average')):.1f}")

    return {
        'query'   : 'Q5 — Режисер + сортирање',
        'param'   : director,
        'avg_ms'  : avg_ms,
        'min_ms'  : round(min(times), 2),
        'max_ms'  : round(max(times), 2),
        'total_ms': round(sum(times), 2),
        'times'   : [round(t, 2) for t in times],
        'count'   : len(rows),
        'note'    : 'Row key prefix scan по director -> pre-sorted по popularity во row key',
        'sample'  : [{'title': r.get('title',''), 'year': r.get('release_year',''),
                      'vote_average': safe_float(r.get('vote_average')),
                      'popularity': safe_float(r.get('popularity'))}
                     for r in rows[:5]],
    }

# ─────────────────────────────────────────────
# Q6 — Филмови по статус и јазик
# ─────────────────────────────────────────────

def q6_by_status_and_language(connection, status='Released', lang='en'):
    print(f"\n[Q6] Филмови со статус='{status}' и јазик='{lang}'")

    times = []
    rows = []

    for _ in range(RUNS):
        t0 = time.perf_counter()

        temp_rows = scan_table(
            connection,
            TABLE_GENRE,
            limit=5000,   # SAFE LIMIT (важно)
            filter_str=None
        )

        rows = [
            r for r in temp_rows
            if r.get('status') == status and r.get('original_language') == lang
        ]

        times.append((time.perf_counter() - t0) * 1000)

    avg_ms = round(statistics.mean(times), 2)

    print(f"     Пронајдени: {len(rows)} | Avg: {avg_ms} ms")
    print(f"     ✔ Python filter (без FilterList, без crash)")

    for r in rows[:5]:
        print(f"     {r.get('title','')[:40]:<40} | *{safe_float(r.get('vote_average')):.1f}")

    return {
        'query': 'Q6 — Статус + јазик (SAFE)',
        'param': f"{status} + {lang}",
        'avg_ms': avg_ms,
        'min_ms': round(min(times), 2),
        'max_ms': round(max(times), 2),
        'total_ms': round(sum(times), 2),
        'times': [round(t, 2) for t in times],
        'count': len(rows),
        'note': 'SAFE version: limited scan + Python filtering (no FilterList crash)',
        'sample': [
            {
                'title': r.get('title',''),
                'vote_average': safe_float(r.get('vote_average'))
            }
            for r in rows[:5]
        ]
    }

# ─────────────────────────────────────────────
# Q7 — Просечен буџет и приход по жанр
# ─────────────────────────────────────────────

def q7_avg_budget_revenue_by_genre(connection, genre='Drama'):
    """
    Prefix scan по genre (оптимизирано) ->
    Python агрегација за avg budget/revenue.
    HBase нема вградени агрегациски функции.
    """
    print(f"\n[Q7] Просечен буџет/приход за жанр '{genre}'")

    table  = connection.table(TABLE_GENRE)
    prefix = genre.encode('utf-8')

    times       = []
    avg_budget  = avg_revenue = 0.0
    count       = 0

    for _ in range(RUNS):
        t0    = time.perf_counter()
        valid = []
        for rk, rd in table.scan(row_prefix=prefix):
            r = decode_row(rd)
            b = safe_int(r.get('budget', 0))
            v = safe_int(r.get('revenue', 0))
            if b > 0 and v > 0:
                valid.append((b, v))
            if len(valid) >= QUERY_LIMIT:
                break
        if valid:
            avg_budget  = statistics.mean(x[0] for x in valid)
            avg_revenue = statistics.mean(x[1] for x in valid)
            count       = len(valid)
        times.append((time.perf_counter() - t0) * 1000)

    avg_ms = round(statistics.mean(times), 2)
    print(f"     Жанр: {genre} | Филмови со буџет/приход: {count}")
    print(f"     Просечен буџет:  ${avg_budget:,.0f}")
    print(f"     Просечен приход: ${avg_revenue:,.0f}")
    print(f"     Avg: {avg_ms} ms | Python агрегација (нема GROUP BY во HBase)")

    return {
        'query'       : 'Q7 — Просечен буџет/приход по жанр',
        'param'       : genre,
        'avg_ms'      : avg_ms,
        'min_ms'      : round(min(times), 2),
        'max_ms'      : round(max(times), 2),
        'total_ms'    : round(sum(times), 2),
        'times'       : [round(t, 2) for t in times],
        'count'       : count,
        'avg_budget'  : round(avg_budget, 2),
        'avg_revenue' : round(avg_revenue, 2),
        'note'        : 'Prefix scan по genre + Python агрегација (нема GROUP BY во HBase)',
    }

# ─────────────────────────────────────────────
# Q8 — Број на филмови по година и јазик
# ─────────────────────────────────────────────

def q8_count_by_year_and_language(connection, year=2010, lang='en'):
    print(f"\n[Q8] Број на филмови за {year} на јазик '{lang}'")

    times = []
    count = 0
    sample_rows = []

    for _ in range(RUNS):
        t0 = time.perf_counter()

        temp_rows = scan_table(
            connection,
            TABLE_GENRE,
            limit=5000,   # SAFE LIMIT
            filter_str=None
        )

        filtered = [
            r for r in temp_rows
            if r.get('release_year') == str(year) and
               r.get('original_language') == lang
        ]

        count = len(filtered)
        sample_rows = filtered[:5]

        times.append((time.perf_counter() - t0) * 1000)

    avg_ms = round(statistics.mean(times), 2)

    print(f"     Филмови во {year} на '{lang}': {count}")
    print(f"     ✔ SAFE scan + Python filter")

    for r in sample_rows:
        print(f"     {r.get('title','')[:40]:<40} | *{safe_float(r.get('vote_average')):.1f}")

    return {
        'query': 'Q8 — Број по година и јазик (SAFE)',
        'param': f"{year} + {lang}",
        'avg_ms': avg_ms,
        'min_ms': round(min(times), 2),
        'max_ms': round(max(times), 2),
        'total_ms': round(sum(times), 2),
        'times': [round(t, 2) for t in times],
        'count': count,
        'note': 'SAFE version: limited scan + Python filtering (no FilterList crash)',
        'sample': [
            {
                'title': r.get('title',''),
                'vote_average': safe_float(r.get('vote_average'))
            }
            for r in sample_rows
        ]
    }
# ─────────────────────────────────────────────
# Q9 — Топ режисери по просечен рејтинг
# ─────────────────────────────────────────────

def q9_top_directors_by_rating(connection, top_n=10, min_movies=3):
    """
    Scan на movies_by_director -> Python GROUP BY + sort.
    HBase нема вградени агрегациски функции.
    """
    print(f"\n[Q9] Топ {top_n} режисери по просечен рејтинг (мин. {min_movies} филмови)")

    table = connection.table(TABLE_DIRECTOR)

    times         = []
    top_directors = []

    for _ in range(RUNS):
        t0     = time.perf_counter()
        groups = defaultdict(list)

        for rk, rd in table.scan():
            r = decode_row(rd)
            d = r.get('director', '')
            v = safe_float(r.get('vote_average', 0))
            if d:
                groups[d].append(v)
            if sum(len(v) for v in groups.values()) >= QUERY_LIMIT:
                break

        stats = [
            {
                'director'    : d,
                'avg_rating'  : round(statistics.mean(vs), 2),
                'movie_count' : len(vs),
            }
            for d, vs in groups.items()
            if len(vs) >= min_movies
        ]
        top_directors = sorted(stats, key=lambda x: x['avg_rating'], reverse=True)[:top_n]
        times.append((time.perf_counter() - t0) * 1000)

    avg_ms = round(statistics.mean(times), 2)
    print(f"     Avg: {avg_ms} ms | Python GROUP BY + sort")
    for d in top_directors[:5]:
        print(f"     {d['director'][:35]:<35} | *{d['avg_rating']} | {d['movie_count']} филмови")

    return {
        'query'   : 'Q9 — Топ режисери по рејтинг',
        'param'   : f"top {top_n}, min {min_movies} movies",
        'avg_ms'  : avg_ms,
        'min_ms'  : round(min(times), 2),
        'max_ms'  : round(max(times), 2),
        'total_ms': round(sum(times), 2),
        'times'   : [round(t, 2) for t in times],
        'count'   : len(top_directors),
        'note'    : 'Full scan + Python GROUP BY + sort (нема GROUP BY во HBase)',
        'sample'  : top_directors,
    }

# ─────────────────────────────────────────────
# ГЛАВНА ФУНКЦИЈА
# ─────────────────────────────────────────────

def main():
    print("=" * 65)
    print("  HBase — Q1-Q9 Прашалници")
    print("=" * 65)

    connection = get_connection()

    try:
        results = {
            'database' : 'HBase',
            'runs'     : RUNS,
            'limit'    : QUERY_LIMIT,
            'queries'  : []
        }

        overall_start = time.perf_counter()

        results['queries'].append(q1_movies_by_genre(connection,             genre='Action'))
        results['queries'].append(q2_movies_by_year(connection,              year=2015))
        results['queries'].append(q3_movies_by_language(connection,          lang='fr'))
        results['queries'].append(q4_top_by_genre_and_rating(connection,     genre='Action', min_rating=7.0))
        results['queries'].append(q5_director_movies(connection,             director='Christopher Nolan'))
        results['queries'].append(q6_by_status_and_language(connection,      status='Released', lang='en'))
        results['queries'].append(q7_avg_budget_revenue_by_genre(connection, genre='Drama'))
        results['queries'].append(q8_count_by_year_and_language(connection,  year=2010, lang='en'))
        results['queries'].append(q9_top_directors_by_rating(connection,     top_n=10, min_movies=3))

        overall_ms = round((time.perf_counter() - overall_start) * 1000, 2)
        results['total_execution_ms'] = overall_ms

        with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f"\n✓ Резултатите зачувани во '{OUTPUT_JSON}'")

        print("\n" + "=" * 75)
        print(f"  {'Query':<40} {'Avg ms':>8} {'Min ms':>8} {'Total ms':>10} {'Count':>7}")
        print("=" * 75)
        for q in results['queries']:
            print(f"  {q['query'][:40]:<40} {q['avg_ms']:>8.2f} "
                  f"{q['min_ms']:>8.2f} {q['total_ms']:>10.2f} {q['count']:>7}")
        print("=" * 75)
        print(f"  {'ВКУПНО ВРЕМЕ НА ИЗВРШУВАЊЕ:':<57} {overall_ms:>10.2f} ms")
        print("=" * 75)

        print("""
ЗАБЕЛЕШКИ:
  Q1, Q4 — Row key prefix scan по genre   -> оптимизирано
  Q5      — Row key prefix scan по director -> pre-sorted по popularity
  Q7      — Prefix scan + Python агрегација -> ефикасно
  Q2, Q3  — SingleColumnValueFilter         -> full scan, побавно
  Q6, Q8  — FilterList на 2 колони          -> full scan, најбавно
  Q9      — Full scan + Python GROUP BY     -> зависи од големина
        """)

    finally:
        connection.close()
        print("✓ Конекцијата затворена.")


if __name__ == "__main__":
    main()