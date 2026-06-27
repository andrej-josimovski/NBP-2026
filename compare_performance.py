import json
import matplotlib.pyplot as plt
import numpy as np
import os

# ─────────────────────────────────────────────
# ВЧИТУВАЊЕ НА РЕЗУЛТАТИТЕ
# ─────────────────────────────────────────────

with open('cassandra_results.json', 'r', encoding='utf-8') as f:
    cassandra_data = json.load(f)

with open('hbase_results.json', 'r', encoding='utf-8') as f:
    hbase_data = json.load(f)

cassandra_queries = cassandra_data['queries']
hbase_queries = hbase_data['queries']

os.makedirs('charts', exist_ok=True)

# ─────────────────────────────────────────────
# ГРАФИК ЗА СЕКОЕ QUERY ОДДЕЛНО (Q1...Q9)
# ─────────────────────────────────────────────

for i in range(len(cassandra_queries)):
    cq = cassandra_queries[i]
    hq = hbase_queries[i]

    query_name = cq['query']  # пр. "Q1 — Филмови по жанр"

    # Статистики за двете бази
    labels = ['Avg', 'Min', 'Max']
    cassandra_vals = [cq['avg_ms'], cq['min_ms'], cq['max_ms']]
    hbase_vals = [hq['avg_ms'], hq['min_ms'], hq['max_ms']]

    x = np.arange(len(labels))
    width = 0.35

    fig, ax = plt.subplots(figsize=(8, 5))
    bars1 = ax.bar(x - width/2, cassandra_vals, width, label='Cassandra', color='#1f77b4')
    bars2 = ax.bar(x + width/2, hbase_vals, width, label='HBase', color='#ff7f0e')

    ax.set_ylabel('Време (ms)')
    ax.set_title(f'{query_name}\n(Cassandra: {cq["count"]} резултати | HBase: {hq["count"]} резултати)')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()
    ax.grid(axis='y', alpha=0.3)

    # Вредности над секоја колона
    for bars in [bars1, bars2]:
        for bar in bars:
            height = bar.get_height()
            ax.annotate(f'{height:,.0f}',
                         xy=(bar.get_x() + bar.get_width() / 2, height),
                         xytext=(0, 3), textcoords="offset points",
                         ha='center', fontsize=8)

    plt.tight_layout()
    filename = f'charts/q{i+1}_comparison.png'
    plt.savefig(filename, dpi=300)
    plt.close()
    print(f"✓ Зачуван: {filename}")

print(f"\nВкупно {len(cassandra_queries)} графикони зачувани во 'charts/' папка.")