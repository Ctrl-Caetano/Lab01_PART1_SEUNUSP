import pandas as pd
from sqlalchemy import create_engine, text
import os

# =============================
# CONEXÃO COM POSTGRESQL
# =============================
DB_USER = "postgres"
DB_PASS = "Teste"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "nike_lab"

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
print("Conexão com PostgreSQL estabelecida!")

# =============================
# LEITURA DO PARQUET SILVER
# =============================
print("Lendo Parquet Silver...")
df = pd.read_parquet("data/silver/nike_silver.parquet")
print(f"Total de linhas: {len(df):,}")

# =============================
# DIMENSÕES
# =============================
print("\nCriando tabelas de dimensão...")

# dim_produto
dim_produto = df[["product_id", "product_name", "model_number", 
                   "category", "subcategory", "brand_name"]].drop_duplicates()
dim_produto.to_sql("dim_produto", engine, if_exists="replace", index=False)
print(f"dim_produto: {len(dim_produto):,} registros")

# dim_pais
dim_pais = df[["country_code", "currency"]].drop_duplicates()
dim_pais.to_sql("dim_pais", engine, if_exists="replace", index=False)
print(f"dim_pais: {len(dim_pais):,} registros")

# dim_segmento
dim_segmento = df[["gender_segment"]].drop_duplicates().dropna()
dim_segmento.to_sql("dim_segmento", engine, if_exists="replace", index=False)
print(f"dim_segmento: {len(dim_segmento):,} registros")

# dim_tamanho
dim_tamanho = df[["sku", "size_label"]].drop_duplicates()
dim_tamanho.to_sql("dim_tamanho", engine, if_exists="replace", index=False)
print(f"dim_tamanho: {len(dim_tamanho):,} registros")

# =============================
# FATO
# =============================
print("\nCriando tabela fato...")
fato_preco = df[["product_id", "country_code", "gender_segment", 
                  "sku", "price_local", "sale_price_local", 
                  "available", "snapshot_date"]]
fato_preco.to_sql("fato_preco", engine, if_exists="replace", index=False)
print(f"fato_preco: {len(fato_preco):,} registros")

# =============================
# 5 QUERIES DE NEGÓCIO
# =============================
print("\n--- Métricas de Negócio ---")

queries = {
    "1. Categoria com maior volume de produtos": """
        SELECT category, COUNT(*) as total
        FROM fato_preco f
        JOIN dim_produto p ON f.product_id = p.product_id
        GROUP BY category
        ORDER BY total DESC
        LIMIT 5
    """,
    "2. Países com mais produtos indisponíveis": """
        SELECT country_code, COUNT(*) as indisponiveis
        FROM fato_preco
        WHERE available = false
        GROUP BY country_code
        ORDER BY indisponiveis DESC
        LIMIT 5
    """,
    "3. Desconto médio por categoria": """
        SELECT p.category,
               ROUND(AVG(f.price_local - f.sale_price_local)::numeric, 2) as desconto_medio
        FROM fato_preco f
        JOIN dim_produto p ON f.product_id = p.product_id
        WHERE f.sale_price_local > 0
        GROUP BY p.category
        ORDER BY desconto_medio DESC
        LIMIT 5
    """,
    "4. Preço médio por segmento de gênero": """
        SELECT gender_segment,
               ROUND(AVG(price_local)::numeric, 2) as preco_medio
        FROM fato_preco
        WHERE gender_segment IS NOT NULL
        GROUP BY gender_segment
        ORDER BY preco_medio DESC
    """,
    "5. Produtos presentes em mais países": """
        SELECT p.product_name, COUNT(DISTINCT f.country_code) as num_paises
        FROM fato_preco f
        JOIN dim_produto p ON f.product_id = p.product_id
        GROUP BY p.product_name
        ORDER BY num_paises DESC
        LIMIT 5
    """
}

with engine.connect() as conn:
    for titulo, query in queries.items():
        print(f"\n{titulo}")
        resultado = pd.read_sql(text(query), conn)
        print(resultado.to_string(index=False))

print("\nGold concluído!")