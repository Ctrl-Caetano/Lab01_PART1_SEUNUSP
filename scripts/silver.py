import pandas as pd
import os
import glob
import matplotlib.pyplot as plt
import seaborn as sns

# Diretórios
RAW_DIR = "data/raw"
SILVER_DIR = "data/silver"
os.makedirs(SILVER_DIR, exist_ok=True)

# =============================
# 1. LEITURA E CONCATENAÇÃO
# =============================
print("Lendo arquivos por país...")
arquivos = glob.glob(os.path.join(RAW_DIR, "Nike_*.csv"))

dfs = []
for arquivo in arquivos:
    country_code = os.path.basename(arquivo).replace("Nike_", "").replace(".csv", "")
    df_temp = pd.read_csv(arquivo)
    df_temp["country_code"] = country_code
    dfs.append(df_temp)

df = pd.concat(dfs, ignore_index=True)
print(f"Total de linhas concatenadas: {len(df):,}")

# =============================
# 2. RELATÓRIO DE QUALIDADE
# =============================
print("\n--- Relatório de Nulos ---")
nulos = df.isnull().sum()
pct = (nulos / len(df) * 100).round(1)
relatorio = pd.DataFrame({"nulos": nulos, "percentual": pct})
print(relatorio[relatorio["nulos"] > 0].sort_values("percentual", ascending=False))

# =============================
# 3. LIMPEZA
# =============================
print("\nIniciando limpeza...")

# Remove colunas 100% nulas
colunas_vazias = [col for col in df.columns if df[col].isnull().all()]
df = df.drop(columns=colunas_vazias)
print(f"Colunas removidas (100% nulas): {colunas_vazias}")

# Padroniza nomes de colunas para snake_case
df.columns = df.columns.str.lower().str.replace(" ", "_")

# Converte snapshot_date para datetime
df["snapshot_date"] = pd.to_datetime(df["snapshot_date"], errors="coerce")

# Preenche sale_price_local nulo com 0
df["sale_price_local"] = df["sale_price_local"].fillna(0)

# Remove duplicatas
antes = len(df)
df = df.drop_duplicates()
print(f"Duplicatas removidas: {antes - len(df):,}")

# Remove linhas com nulos residuais
df = df.dropna(subset=["size_label", "subcategory", "brand_name", "sku"])
print(f"Total de linhas após limpeza: {len(df):,}")

# =============================
# 4. GRÁFICOS
# =============================
print("\nGerando gráficos...")
os.makedirs("docs", exist_ok=True)

# Gráfico 1 - Top 10 categorias
plt.figure(figsize=(10, 5))
df["category"].value_counts().head(10).plot(kind="bar", color="steelblue")
plt.title("Top 10 Categorias de Produtos")
plt.xlabel("Categoria")
plt.ylabel("Quantidade")
plt.tight_layout()
plt.savefig("docs/grafico1_categorias.png")
plt.close()

# Gráfico 2 - Distribuição de preços
plt.figure(figsize=(10, 5))
df[df["price_local"] < df["price_local"].quantile(0.95)]["price_local"].hist(bins=50, color="steelblue")
plt.title("Distribuição de Preços (sem outliers)")
plt.xlabel("Preço Local")
plt.ylabel("Frequência")
plt.tight_layout()
plt.savefig("docs/grafico2_precos.png")
plt.close()

# Gráfico 3 - Disponibilidade
plt.figure(figsize=(6, 5))
df["available"].value_counts().plot(kind="pie", autopct="%1.1f%%", colors=["steelblue", "salmon"])
plt.title("Disponibilidade dos Produtos")
plt.tight_layout()
plt.savefig("docs/grafico3_disponibilidade.png")
plt.close()

# Gráfico 4 - Top 10 países por volume
plt.figure(figsize=(10, 5))
df["country_code"].value_counts().head(10).plot(kind="bar", color="steelblue")
plt.title("Top 10 Países por Volume de Registros")
plt.xlabel("País")
plt.ylabel("Quantidade")
plt.tight_layout()
plt.savefig("docs/grafico4_paises.png")
plt.close()

# Gráfico 5 - Preço médio por gender segment
plt.figure(figsize=(8, 5))
df.groupby("gender_segment")["price_local"].mean().sort_values().plot(kind="barh", color="steelblue")
plt.title("Preço Médio por Segmento de Gênero")
plt.xlabel("Preço Médio")
plt.tight_layout()
plt.savefig("docs/grafico5_genero.png")
plt.close()

print("Gráficos salvos em docs/")

# =============================
# 5. SALVAR PARQUET
# =============================
caminho_parquet = os.path.join(SILVER_DIR, "nike_silver.parquet")
df.to_parquet(caminho_parquet, index=False)
print(f"\nSilver concluído! Parquet salvo em: {caminho_parquet}")
print(f"Shape final: {df.shape}")