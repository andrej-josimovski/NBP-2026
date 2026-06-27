import time
import json
import statistics
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy

# ─────────────────────────────────────────────
# КОНФИГУРАЦИЈА
# ─────────────────────────────────────────────

CASSANDRA_HOST = '127.0.0.1'
KEYSPACE       = 'movies'
RUNS           = 5
QUERY_LIMIT    = 50000
OUTPUT_JSON    = 'cassandra_results.json'
SHARED_IDS_FILE = 'shared_ids.json'

# ─────────────────────────────────────────────
# ВЧИТАЈ СПОДЕЛЕНИ ID-А
# ─────────────────────────────────────────────

with open(SHARED_IDS_FILE, 'r') as f:
    SHARED_IDS = json.load(f)
SHARED_IDS_SET = set(SHARED_IDS)
print(f"✓ Вчитани {len(SHARED_IDS)} споделени ID-а")

# ─────────────────────────────────────────────
# ПОВРЗУВАЊЕ
# ─────────────────────────────────────────────

def get_session():
    cluster = Cluster(
        [CASSANDRA_HOST],
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1')
    )
    session = cluster.connect(KEYSPACE)
    print(f"✓ Поврзан со Cassandra @ {CASSANDRA_HOST} | keyspace: {KEYSPACE}")
    return cluster, session

# ─────────────────────────────────────────────
# ПОМОШНА ФУНКЦИЈА ЗА МЕРЕЊЕ
# ─────────────────────────────────────────────

def measure(session, cql, params=None, runs=RUNS, post_filter=None):
    times = []
    rows  = []
    for _ in range(runs):
        t0 = time.perf_counter()
        if params is None:
            rows = list(session.execute(cql))
        else:
            rows = list(session.execute(cql, params))
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

def q1_movies_by_genre(session, genre='Action'):
    print(f"\n[Q1] Филмови со жанр '{genre}'")

    cql = f"""
        SELECT id, title, vote_average, release_year, director, genres_list
        FROM movies_by_genre
        WHERE genre = %s
        LIMIT {QUERY_LIMIT}
    """
    m = measure(session, cql, params=(genre,),
                post_filter=lambda r: int(r.id) in SHARED_IDS_SET)

    result = {
        'query'    : 'Q1 — Филмови по жанр',
        'param'    : genre,
        'avg_ms'   : m['avg_ms'],
        'min_ms'   : m['min_ms'],
        'max_ms'   : m['max_ms'],
        'total_ms' : m['total_ms'],
        'times'    : m['times'],
        'count'    : m['count'],
        'note'     : f'Partition key scan по genre + филтер по споделени ID-а',
        'sample'   : [{'title': r.title, 'vote_average': r.vote_average,
                       'year': r.release_year, 'director': r.director}
                      for r in m['rows'][:5]],
    }

    print(f"     Пронајдени: {result['count']} | Avg: {result['avg_ms']} ms | Total: {result['total_ms']} ms")
    for r in m['rows'][:5]:
        print(f"     {r.title[:40]:<40} | {r.vote_average:.1f} | {r.release_year}")
    return result

# ─────────────────────────────────────────────
# Q2 — Филмови по година
# ─────────────────────────────────────────────

def q2_movies_by_year(session, year=2015):
    print(f"\n[Q2] Филмови од {year} година")

    cql = f"""
        SELECT id, title, vote_average, director, genres_list
        FROM movies_by_genre
        WHERE release_year = %s
        LIMIT {QUERY_LIMIT}
        ALLOW FILTERING
    """
    m = measure(session, cql, params=(year,),
                post_filter=lambda r: int(r.id) in SHARED_IDS_SET)

    result = {
        'query'    : 'Q2 — Филмови по година',
        'param'    : year,
        'avg_ms'   : m['avg_ms'],
        'min_ms'   : m['min_ms'],
        'max_ms'   : m['max_ms'],
        'total_ms' : m['total_ms'],
        'times'    : m['times'],
        'count'    : m['count'],
        'note'     : 'ALLOW FILTERING — release_year не е PK',
        'sample'   : [{'title': r.title, 'vote_average': r.vote_average,
                       'director': r.director}
                      for r in m['rows'][:5]],
    }

    print(f"     Пронајдени: {result['count']} | Avg: {result['avg_ms']} ms | Total: {result['total_ms']} ms")
    for r in m['rows'][:5]:
        print(f"     {r.title[:40]:<40} | {r.vote_average:.1f} | {r.director}")
    return result

# ─────────────────────────────────────────────
# Q3 — Филмови по јазик
# ─────────────────────────────────────────────

def q3_movies_by_language(session, lang='fr'):
    print(f"\n[Q3] Филмови на јазик '{lang}'")

    cql = f"""
        SELECT id, title, vote_average, director, release_year
        FROM movies_by_genre
        WHERE original_language = %s
        LIMIT {QUERY_LIMIT}
        ALLOW FILTERING
    """
    m = measure(session, cql, params=(lang,),
                post_filter=lambda r: int(r.id) in SHARED_IDS_SET)

    result = {
        'query'    : 'Q3 — Филмови по јазик',
        'param'    : lang,
        'avg_ms'   : m['avg_ms'],
        'min_ms'   : m['min_ms'],
        'max_ms'   : m['max_ms'],
        'total_ms' : m['total_ms'],
        'times'    : m['times'],
        'count'    : m['count'],
        'note'     : 'ALLOW FILTERING — original_language не е PK',
        'sample'   : [{'title': r.title, 'vote_average': r.vote_average,
                       'year': r.release_year}
                      for r in m['rows'][:5]],
    }

    print(f"     Пронајдени: {result['count']} | Avg: {result['avg_ms']} ms | Total: {result['total_ms']} ms")
    for r in m['rows'][:5]:
        print(f"     {r.title[:40]:<40} | {r.vote_average:.1f} | {r.release_year}")
    return result

# ─────────────────────────────────────────────
# Q4 — Топ по жанр + рејтинг
# ─────────────────────────────────────────────

def q4_top_by_genre_and_rating(session, genre='Action', min_rating=7.0):
    print(f"\n[Q4] Топ '{genre}' филми со рејтинг > {min_rating}")

    cql = f"""
        SELECT id, title, vote_average, director, genres_list, cast_list
        FROM movies_by_genre
        WHERE genre = %s AND vote_average > %s
        LIMIT {QUERY_LIMIT}
    """
    m = measure(session, cql, params=(genre, min_rating),
                post_filter=lambda r: int(r.id) in SHARED_IDS_SET)

    result = {
        'query'      : 'Q4 — Топ по жанр + рејтинг',
        'param'      : f"{genre} > {min_rating}",
        'avg_ms'     : m['avg_ms'],
        'min_ms'     : m['min_ms'],
        'max_ms'     : m['max_ms'],
        'total_ms'   : m['total_ms'],
        'times'      : m['times'],
        'count'      : m['count'],
        'note'       : 'PK + CK → целосно оптимизиран, без ALLOW FILTERING',
        'sample'     : [{'title': r.title, 'vote_average': r.vote_average,
                         'director': r.director}
                        for r in m['rows'][:5]],
    }

    print(f"     Пронајдени: {result['count']} | Avg: {result['avg_ms']} ms | Total: {result['total_ms']} ms")
    for r in m['rows'][:5]:
        print(f"     {r.title[:40]:<40} | {r.vote_average:.1f}")
    return result

# ─────────────────────────────────────────────
# Q5 — Режисер по popularity
# ─────────────────────────────────────────────

def q5_director_movies(session, director='Christopher Nolan'):
    print(f"\n[Q5] Филмови на '{director}' сортирани по popularity")

    cql = f"""
        SELECT id, title, release_year, vote_average, budget, revenue, popularity
        FROM movies_by_director
        WHERE director = %s
        LIMIT {QUERY_LIMIT}
    """
    m = measure(session, cql, params=(director,),
                post_filter=lambda r: int(r.id) in SHARED_IDS_SET)

    result = {
        'query'    : 'Q5 — Режисер + сортирање',
        'param'    : director,
        'avg_ms'   : m['avg_ms'],
        'min_ms'   : m['min_ms'],
        'max_ms'   : m['max_ms'],
        'total_ms' : m['total_ms'],
        'times'    : m['times'],
        'count'    : m['count'],
        'note'     : 'director е PK, popularity CK DESC → pre-sorted',
        'sample'   : [{'title': r.title, 'year': r.release_year,
                       'vote_average': r.vote_average, 'popularity': r.popularity}
                      for r in m['rows'][:5]],
    }

    print(f"     Вкупно: {result['count']} | Avg: {result['avg_ms']} ms | Total: {result['total_ms']} ms")
    for r in m['rows'][:5]:
        print(f"     {r.title[:40]:<40} | {r.release_year} | ★{r.vote_average:.1f}")
    return result

# ─────────────────────────────────────────────
# Q6 — Статус + јазик
# ─────────────────────────────────────────────

def q6_by_status_and_language(session, status='Released', lang='en'):
    print(f"\n[Q6] Филмови со статус='{status}' и јазик='{lang}'")

    cql = f"""
        SELECT id, title, vote_average, release_year, director
        FROM movies_by_genre
        WHERE status = %s AND original_language = %s
        LIMIT {QUERY_LIMIT}
        ALLOW FILTERING
    """
    m = measure(session, cql, params=(status, lang),
                post_filter=lambda r: int(r.id) in SHARED_IDS_SET)

    result = {
        'query'    : 'Q6 — Статус + јазик (ALLOW FILTERING)',
        'param'    : f"{status} + {lang}",
        'avg_ms'   : m['avg_ms'],
        'min_ms'   : m['min_ms'],
        'max_ms'   : m['max_ms'],
        'total_ms' : m['total_ms'],
        'times'    : m['times'],
        'count'    : m['count'],
        'note'     : 'ALLOW FILTERING на 2 непартиционирани колони → full scan',
        'sample'   : [{'title': r.title, 'vote_average': r.vote_average,
                       'year': r.release_year}
                      for r in m['rows'][:5]],
    }

    print(f"     Пронајдени: {result['count']} | Avg: {result['avg_ms']} ms | Total: {result['total_ms']} ms")
    print(f"     ⚠ ALLOW FILTERING — побавно!")
    return result

# ─────────────────────────────────────────────
# Q7 — Просечен буџет и приход по жанр
# ─────────────────────────────────────────────

def q7_avg_budget_revenue_by_genre(session, genre='Drama'):
    print(f"\n[Q7] Просечен буџет/приход за жанр '{genre}'")

    cql = f"""
        SELECT id, budget, revenue, genres_list
        FROM movies_by_genre
        WHERE genre = %s
        LIMIT {QUERY_LIMIT}
    """

    times = []
    avg_budget = avg_revenue = 0.0
    count = 0
    for _ in range(RUNS):
        t0   = time.perf_counter()
        rows = list(session.execute(cql, (genre,)))
        filt = [r for r in rows
                if int(r.id) in SHARED_IDS_SET
                and r.budget > 0 and r.revenue > 0]
        if filt:
            avg_budget  = statistics.mean(r.budget  for r in filt)
            avg_revenue = statistics.mean(r.revenue for r in filt)
            count       = len(filt)
        times.append((time.perf_counter() - t0) * 1000)

    result = {
        'query'       : 'Q7 — Просечен буџет/приход по жанр',
        'param'       : genre,
        'avg_ms'      : round(statistics.mean(times), 2),
        'min_ms'      : round(min(times), 2),
        'max_ms'      : round(max(times), 2),
        'total_ms'    : round(sum(times), 2),
        'times'       : [round(t, 2) for t in times],
        'count'       : count,
        'avg_budget'  : round(avg_budget, 2),
        'avg_revenue' : round(avg_revenue, 2),
        'note'        : 'Partition scan + Python агрегација, филтер по споделени ID-а',
    }

    print(f"     Жанр: {genre} | Филми: {count}")
    print(f"     Просечен буџет:  ${avg_budget:,.0f}")
    print(f"     Просечен приход: ${avg_revenue:,.0f}")
    print(f"     Avg: {result['avg_ms']} ms | Total: {result['total_ms']} ms")
    return result

# ─────────────────────────────────────────────
# Q8 — Број по година и јазик
# ─────────────────────────────────────────────

def q8_count_by_year_and_language(session, year=2010, lang='en'):
    print(f"\n[Q8] Број на филми за {year} на јазик '{lang}'")

    cql = f"""
        SELECT id, title, original_language, release_year
        FROM movies_by_genre
        WHERE release_year = %s AND original_language = %s
        LIMIT {QUERY_LIMIT}
        ALLOW FILTERING
    """

    times = []
    count = 0
    rows  = []
    for _ in range(RUNS):
        t0   = time.perf_counter()
        rows = [r for r in session.execute(cql, (year, lang))
                if int(r.id) in SHARED_IDS_SET]
        count = len(rows)
        times.append((time.perf_counter() - t0) * 1000)

    result = {
        'query'    : 'Q8 — Број по година и јазик',
        'param'    : f"{year} + {lang}",
        'avg_ms'   : round(statistics.mean(times), 2),
        'min_ms'   : round(min(times), 2),
        'max_ms'   : round(max(times), 2),
        'total_ms' : round(sum(times), 2),
        'times'    : [round(t, 2) for t in times],
        'count'    : count,
        'note'     : 'ALLOW FILTERING + филтер по споделени ID-а',
    }

    print(f"     Филми во {year} на '{lang}': {count}")
    print(f"     Avg: {result['avg_ms']} ms | Total: {result['total_ms']} ms")
    return result

# ─────────────────────────────────────────────
# Q9 — Топ режисери по рејтинг
# ─────────────────────────────────────────────

def q9_top_directors_by_rating(session, top_n=10, min_movies=3):
    print(f"\n[Q9] Топ {top_n} режисери по просечен рејтинг (мин. {min_movies} филми)")

    cql = f"""
        SELECT id, director, vote_average, title
        FROM movies_by_director
        LIMIT {QUERY_LIMIT}
    """

    times         = []
    top_directors = []
    for _ in range(RUNS):
        t0   = time.perf_counter()
        rows = [r for r in session.execute(cql)
                if int(r.id) in SHARED_IDS_SET]

        from collections import defaultdict
        groups = defaultdict(list)
        for r in rows:
            groups[r.director].append(r.vote_average)

        stats = [
            {'director': d, 'avg_rating': round(statistics.mean(vs), 2),
             'movie_count': len(vs)}
            for d, vs in groups.items()
            if len(vs) >= min_movies
        ]
        top_directors = sorted(stats, key=lambda x: x['avg_rating'], reverse=True)[:top_n]
        times.append((time.perf_counter() - t0) * 1000)

    result = {
        'query'    : 'Q9 — Топ режисери по рејтинг',
        'param'    : f"top {top_n}, min {min_movies}",
        'avg_ms'   : round(statistics.mean(times), 2),
        'min_ms'   : round(min(times), 2),
        'max_ms'   : round(max(times), 2),
        'total_ms' : round(sum(times), 2),
        'times'    : [round(t, 2) for t in times],
        'count'    : len(top_directors),
        'note'     : 'Fetch + Python GROUP BY + sort, филтер по споделени ID-а',
        'sample'   : top_directors,
    }

    print(f"     Avg: {result['avg_ms']} ms | Total: {result['total_ms']} ms")
    for d in top_directors[:5]:
        print(f"     {d['director'][:35]:<35} | ★{d['avg_rating']} | {d['movie_count']} филми")
    return result

# ─────────────────────────────────────────────
# ГЛАВНА ФУНКЦИЈА
# ─────────────────────────────────────────────

def main():
    print("=" * 65)
    print("  Cassandra — Q1–Q9 Прашалници")
    print("=" * 65)

    cluster, session = get_session()

    try:
        results = {
            'database' : 'Cassandra',
            'runs'     : RUNS,
            'limit'    : QUERY_LIMIT,
            'queries'  : []
        }

        overall_start = time.perf_counter()

        results['queries'].append(q1_movies_by_genre(session,             genre='Action'))
        results['queries'].append(q2_movies_by_year(session,              year=2015))
        results['queries'].append(q3_movies_by_language(session,          lang='fr'))
        results['queries'].append(q4_top_by_genre_and_rating(session,     genre='Action', min_rating=7.0))
        results['queries'].append(q5_director_movies(session,             director='Christopher Nolan'))
        results['queries'].append(q6_by_status_and_language(session,      status='Released', lang='en'))
        results['queries'].append(q7_avg_budget_revenue_by_genre(session, genre='Drama'))
        results['queries'].append(q8_count_by_year_and_language(session,  year=2010, lang='en'))
        results['queries'].append(q9_top_directors_by_rating(session,     top_n=10, min_movies=3))

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
        cluster.shutdown()
        print("\n✓ Конекцијата затворена.")

if __name__ == "__main__":
    main()