import time
import json
import statistics
from collections import defaultdict
import happybase

HBASE_HOST      = '127.0.0.1'
HBASE_PORT      = 9090
RUNS            = 5
QUERY_LIMIT     = 50000
OUTPUT_JSON     = 'hbase_results.json'
SHARED_IDS_FILE = 'shared_ids.json'

TABLE_GENRE    = 'movies_by_genre'
TABLE_DIRECTOR = 'movies_by_director'

with open(SHARED_IDS_FILE, 'r') as f:
    SHARED_IDS = json.load(f)
SHARED_IDS_SET = set(str(i) for i in SHARED_IDS)
print(f"✓ Вчитани {len(SHARED_IDS)} споделени ID-а")

def get_connection():
    connection = happybase.Connection(HBASE_HOST, port=HBASE_PORT, autoconnect=True)
    print(f"✓ Поврзан со HBase @ {HBASE_HOST}:{HBASE_PORT}")
    return connection

def decode_row(row_data):
    decoded = {}
    for col, val in row_data.items():
        key = col.decode('utf-8').split(':')[-1]
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
    table  = connection.table(table_name)
    kwargs = {}
    if filter_str:
        kwargs['filter'] = filter_str
    rows = []
    for row_key, row_data in table.scan(**kwargs):
        r = decode_row(row_data)
        if r.get('id', '') in SHARED_IDS_SET:
            rows.append(r)
        if len(rows) >= limit:
            break
    return rows

def q1_movies_by_genre(connection, genre='Action'):
    print(f"\n[Q1] Филмови со жанр '{genre}'")
    table  = connection.table(TABLE_GENRE)
    prefix = genre.encode('utf-8')
    times = []
    rows  = []
    for _ in range(RUNS):
        t0   = time.perf_counter()
        rows = []
        for rk, rd in table.scan(row_prefix=prefix):
            r = decode_row(rd)
            if r.get('id', '') in SHARED_IDS_SET:
                rows.append(r)
            if len(rows) >= QUERY_LIMIT:
                break
        times.append((time.perf_counter() - t0) * 1000)
    avg_ms = round(statistics.mean(times), 2)
    print(f"     Пронајдени: {len(rows)} | Avg: {avg_ms} ms | Total: {round(sum(times),2)} ms")
    for r in rows[:5]:
        print(f"     {r.get('title','')[:40]:<40} | *{safe_float(r.get('vote_average')):.1f} | {r.get('release_year','')}")
    return {
        'query': 'Q1 — Филмови по жанр', 'param': genre,
        'avg_ms': avg_ms, 'min_ms': round(min(times),2),
        'max_ms': round(max(times),2), 'total_ms': round(sum(times),2),
        'times': [round(t,2) for t in times], 'count': len(rows),
        'note': 'Prefix scan по genre + филтер по споделени ID-а',
        'sample': [{'title': r.get('title',''), 'vote_average': safe_float(r.get('vote_average')),
                    'year': r.get('release_year','')} for r in rows[:5]],
    }

def q2_movies_by_year(connection, year=2015):
    print(f"\n[Q2] Филмови од {year} година")
    filter_str = f"SingleColumnValueFilter('info', 'release_year', =, 'binary:{year}', true, true)"
    times = []
    rows  = []
    for _ in range(RUNS):
        t0   = time.perf_counter()
        rows = scan_table(connection, TABLE_GENRE, limit=QUERY_LIMIT, filter_str=filter_str)
        times.append((time.perf_counter() - t0) * 1000)
    avg_ms = round(statistics.mean(times), 2)
    print(f"     Пронајдени: {len(rows)} | Avg: {avg_ms} ms | Total: {round(sum(times),2)} ms")
    for r in rows[:5]:
        print(f"     {r.get('title','')[:40]:<40} | *{safe_float(r.get('vote_average')):.1f}")
    return {
        'query': 'Q2 — Филмови по година', 'param': year,
        'avg_ms': avg_ms, 'min_ms': round(min(times),2),
        'max_ms': round(max(times),2), 'total_ms': round(sum(times),2),
        'times': [round(t,2) for t in times], 'count': len(rows),
        'note': 'SingleColumnValueFilter + филтер по споделени ID-а',
        'sample': [{'title': r.get('title',''), 'vote_average': safe_float(r.get('vote_average'))} for r in rows[:5]],
    }

def q3_movies_by_language(connection, lang='fr'):
    print(f"\n[Q3] Филмови на јазик '{lang}'")
    filter_str = f"SingleColumnValueFilter('info', 'original_language', =, 'binary:{lang}', true, true)"
    times = []
    rows  = []
    for _ in range(RUNS):
        t0   = time.perf_counter()
        rows = scan_table(connection, TABLE_GENRE, limit=QUERY_LIMIT, filter_str=filter_str)
        times.append((time.perf_counter() - t0) * 1000)
    avg_ms = round(statistics.mean(times), 2)
    print(f"     Пронајдени: {len(rows)} | Avg: {avg_ms} ms | Total: {round(sum(times),2)} ms")
    for r in rows[:5]:
        print(f"     {r.get('title','')[:40]:<40} | *{safe_float(r.get('vote_average')):.1f} | {r.get('release_year','')}")
    return {
        'query': 'Q3 — Филмови по јазик', 'param': lang,
        'avg_ms': avg_ms, 'min_ms': round(min(times),2),
        'max_ms': round(max(times),2), 'total_ms': round(sum(times),2),
        'times': [round(t,2) for t in times], 'count': len(rows),
        'note': 'SingleColumnValueFilter + филтер по споделени ID-а',
        'sample': [{'title': r.get('title',''), 'vote_average': safe_float(r.get('vote_average')),
                    'year': r.get('release_year','')} for r in rows[:5]],
    }

def q4_top_by_genre_and_rating(connection, genre='Action', min_rating=7.0):
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
            if r.get('id', '') in SHARED_IDS_SET and safe_float(r.get('vote_average', 0)) > min_rating:
                rows.append(r)
            if len(rows) >= QUERY_LIMIT:
                break
        times.append((time.perf_counter() - t0) * 1000)
    avg_ms = round(statistics.mean(times), 2)
    print(f"     Пронајдени: {len(rows)} | Avg: {avg_ms} ms | Total: {round(sum(times),2)} ms")
    for r in rows[:5]:
        print(f"     {r.get('title','')[:40]:<40} | *{safe_float(r.get('vote_average')):.1f}")
    return {
        'query': 'Q4 — Топ по жанр + рејтинг', 'param': f"{genre} > {min_rating}",
        'avg_ms': avg_ms, 'min_ms': round(min(times),2),
        'max_ms': round(max(times),2), 'total_ms': round(sum(times),2),
        'times': [round(t,2) for t in times], 'count': len(rows),
        'note': 'Prefix scan + Python filter + споделени ID-а',
        'sample': [{'title': r.get('title',''), 'vote_average': safe_float(r.get('vote_average'))} for r in rows[:5]],
    }

def q5_director_movies(connection, director='Christopher Nolan'):
    print(f"\n[Q5] Филмови на '{director}' сортирани по popularity")
    table  = connection.table(TABLE_DIRECTOR)
    prefix = director.encode('utf-8')
    times = []
    rows  = []
    for _ in range(RUNS):
        t0   = time.perf_counter()
        rows = []
        for rk, rd in table.scan(row_prefix=prefix):
            r = decode_row(rd)
            if r.get('id', '') in SHARED_IDS_SET:
                rows.append(r)
            if len(rows) >= QUERY_LIMIT:
                break
        times.append((time.perf_counter() - t0) * 1000)
    avg_ms = round(statistics.mean(times), 2)
    print(f"     Вкупно: {len(rows)} | Avg: {avg_ms} ms | Total: {round(sum(times),2)} ms")
    for r in rows[:5]:
        print(f"     {r.get('title','')[:40]:<40} | {r.get('release_year','')} | *{safe_float(r.get('vote_average')):.1f}")
    return {
        'query': 'Q5 — Режисер + сортирање', 'param': director,
        'avg_ms': avg_ms, 'min_ms': round(min(times),2),
        'max_ms': round(max(times),2), 'total_ms': round(sum(times),2),
        'times': [round(t,2) for t in times], 'count': len(rows),
        'note': 'Prefix scan по director → pre-sorted по popularity',
        'sample': [{'title': r.get('title',''), 'year': r.get('release_year',''),
                    'vote_average': safe_float(r.get('vote_average'))} for r in rows[:5]],
    }

def q6_by_status_and_language(connection, status='Released', lang='en'):
    print(f"\n[Q6] Филмови со статус='{status}' и јазик='{lang}'")
    times = []
    rows  = []
    for _ in range(RUNS):
        t0        = time.perf_counter()
        temp_rows = scan_table(connection, TABLE_GENRE, limit=QUERY_LIMIT)
        rows      = [r for r in temp_rows
                     if r.get('status') == status and r.get('original_language') == lang]
        times.append((time.perf_counter() - t0) * 1000)
    avg_ms = round(statistics.mean(times), 2)
    print(f"     Пронајдени: {len(rows)} | Avg: {avg_ms} ms | Total: {round(sum(times),2)} ms")
    for r in rows[:5]:
        print(f"     {r.get('title','')[:40]:<40} | *{safe_float(r.get('vote_average')):.1f}")
    return {
        'query': 'Q6 — Статус + јазик', 'param': f"{status} + {lang}",
        'avg_ms': avg_ms, 'min_ms': round(min(times),2),
        'max_ms': round(max(times),2), 'total_ms': round(sum(times),2),
        'times': [round(t,2) for t in times], 'count': len(rows),
        'note': 'Full scan + Python filter по status и language + споделени ID-а',
        'sample': [{'title': r.get('title',''), 'vote_average': safe_float(r.get('vote_average'))} for r in rows[:5]],
    }

def q7_avg_budget_revenue_by_genre(connection, genre='Drama'):
    print(f"\n[Q7] Просечен буџет/приход за жанр '{genre}'")
    table  = connection.table(TABLE_GENRE)
    prefix = genre.encode('utf-8')
    times = []
    avg_budget = avg_revenue = 0.0
    count = 0
    for _ in range(RUNS):
        t0    = time.perf_counter()
        valid = []
        for rk, rd in table.scan(row_prefix=prefix):
            r = decode_row(rd)
            if r.get('id', '') not in SHARED_IDS_SET:
                continue
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
    print(f"     Жанр: {genre} | Филми: {count}")
    print(f"     Просечен буџет:  ${avg_budget:,.0f}")
    print(f"     Просечен приход: ${avg_revenue:,.0f}")
    print(f"     Avg: {avg_ms} ms | Total: {round(sum(times),2)} ms")
    return {
        'query': 'Q7 — Просечен буџет/приход по жанр', 'param': genre,
        'avg_ms': avg_ms, 'min_ms': round(min(times),2),
        'max_ms': round(max(times),2), 'total_ms': round(sum(times),2),
        'times': [round(t,2) for t in times], 'count': count,
        'avg_budget': round(avg_budget,2), 'avg_revenue': round(avg_revenue,2),
        'note': 'Prefix scan + Python агрегација + споделени ID-а',
    }

def q8_count_by_year_and_language(connection, year=2010, lang='en'):
    print(f"\n[Q8] Број на филмови за {year} на јазик '{lang}'")
    times = []
    count = 0
    sample_rows = []
    for _ in range(RUNS):
        t0        = time.perf_counter()
        temp_rows = scan_table(connection, TABLE_GENRE, limit=QUERY_LIMIT)
        filtered  = [r for r in temp_rows
                     if r.get('release_year') == str(year)
                     and r.get('original_language') == lang]
        count       = len(filtered)
        sample_rows = filtered[:5]
        times.append((time.perf_counter() - t0) * 1000)
    avg_ms = round(statistics.mean(times), 2)
    print(f"     Филмови во {year} на '{lang}': {count}")
    print(f"     Avg: {avg_ms} ms | Total: {round(sum(times),2)} ms")
    for r in sample_rows:
        print(f"     {r.get('title','')[:40]:<40} | *{safe_float(r.get('vote_average')):.1f}")
    return {
        'query': 'Q8 — Број по година и јазик', 'param': f"{year} + {lang}",
        'avg_ms': avg_ms, 'min_ms': round(min(times),2),
        'max_ms': round(max(times),2), 'total_ms': round(sum(times),2),
        'times': [round(t,2) for t in times], 'count': count,
        'note': 'Full scan + Python filter по година и јазик + споделени ID-а',
        'sample': [{'title': r.get('title',''), 'vote_average': safe_float(r.get('vote_average'))} for r in sample_rows],
    }

def q9_top_directors_by_rating(connection, top_n=10, min_movies=3):
    print(f"\n[Q9] Топ {top_n} режисери по просечен рејтинг (мин. {min_movies} филмови)")
    table = connection.table(TABLE_DIRECTOR)
    times = []
    top_directors = []
    for _ in range(RUNS):
        t0     = time.perf_counter()
        groups = defaultdict(list)
        count  = 0
        for rk, rd in table.scan():
            r = decode_row(rd)
            if r.get('id', '') not in SHARED_IDS_SET:
                continue
            d = r.get('director', '')
            v = safe_float(r.get('vote_average', 0))
            if d:
                groups[d].append(v)
            count += 1
            if count >= QUERY_LIMIT:
                break
        stats = [
            {'director': d, 'avg_rating': round(statistics.mean(vs), 2), 'movie_count': len(vs)}
            for d, vs in groups.items() if len(vs) >= min_movies
        ]
        top_directors = sorted(stats, key=lambda x: x['avg_rating'], reverse=True)[:top_n]
        times.append((time.perf_counter() - t0) * 1000)
    avg_ms = round(statistics.mean(times), 2)
    print(f"     Avg: {avg_ms} ms | Total: {round(sum(times),2)} ms")
    for d in top_directors[:5]:
        print(f"     {d['director'][:35]:<35} | *{d['avg_rating']} | {d['movie_count']} филмови")
    return {
        'query': 'Q9 — Топ режисери по рејтинг', 'param': f"top {top_n}, min {min_movies}",
        'avg_ms': avg_ms, 'min_ms': round(min(times),2),
        'max_ms': round(max(times),2), 'total_ms': round(sum(times),2),
        'times': [round(t,2) for t in times], 'count': len(top_directors),
        'note': 'Full scan + Python GROUP BY + sort + споделени ID-а',
        'sample': top_directors,
    }

def main():
    print("=" * 65)
    print("  HBase — Q1-Q9 Прашалници")
    print("=" * 65)

    connection = get_connection()

    try:
        results = {
            'database': 'HBase', 'runs': RUNS,
            'limit': QUERY_LIMIT, 'queries': []
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

    finally:
        connection.close()
        print("✓ Конекцијата затворена.")

if __name__ == "__main__":
    main()