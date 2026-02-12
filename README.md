Mobilidade Urbana BH — Pipeline Medallion (Bronze → Silver → Gold)
📌 Objetivo
Construir um pipeline de dados para mobilidade urbana de Belo Horizonte (PBH) que:

1) Extrai dados públicos (ex.: Mapa de Controle Operacional – MCO / posicionamento).
2) Armazena no data lake (Medallion: Bronze → Silver → Gold).
3) Transforma e agrega (PySpark / Delta Lake).
4) Entrega dados prontos para análise (BI/ML).
5) Possui orquestração, qualidade de dados e documentação.
🏗️ Arquitetura (Visão Geral)
Fonte (Portal PBH / CKAN) → Bronze (raw CSV + parquet) → Silver (Delta limpo, padronizado e enriquecido) → Gold (Delta com KPIs e métricas) → Consumo (Databricks SQL / Power BI / ML).

Plataforma: Azure Databricks (Unity Catalog + Volumes)
Processamento: PySpark
Armazenamento: Bronze = Parquet; Silver/Gold = Delta Lake
Governança: Unity Catalog (Catalog/Schema/Volume)
Orquestração: Databricks Workflows
🧭 Organização no Unity Catalog
Criado uma única vez no início do projeto. Caminho POSIX para leitura/escrita: `/Volumes/<catalog>/<schema>/<volume>`.
Estrutura de pastas recomendada:
/Volumes/<catalog>/<schema>/<volume>/
  bronze/ raw/ & parquet/
  silver/ mco/
  gold/   mco_kpis/
  checkpoints/ (opcional)
  _docs/ (diagramas, dicionário, etc.)
🟫 Bronze — Guardar como veio + metadados mínimos
Objetivo: preservar o dado bruto e disponibilizar cópia otimizada em Parquet.
Passos: (1) baixar CSV do CKAN; (2) salvar bruto em bronze/raw; (3) ler com Spark (atentar ao delimiter ";"); (4) adicionar metadados (ingestion_timestamp, source_file); (5) escrever Parquet em bronze/parquet.
Exemplo (trecho essencial):
import requests, os
from pyspark.sql.functions import current_timestamp, lit

BASE = "/Volumes/bh/mobilidade/data_mobility_bh"
RAW  = f"{BASE}/bronze/raw"
PARQ = f"{BASE}/bronze/parquet"

os.makedirs(RAW, exist_ok=True)
os.makedirs(PARQ, exist_ok=True)

file_name = "mco-09-2025.csv"
local_csv = f"{RAW}/{file_name}"

resp = requests.get(url, timeout=60, headers={"User-Agent":"Mozilla/5.0"})
resp.raise_for_status()
with open(local_csv, "wb") as f: f.write(resp.content)

df_bronze = (spark.read
             .option("header", "true")
             .option("inferSchema", "true")
             .option("delimiter", ";")
             .csv(local_csv)
             .withColumn("ingestion_timestamp", current_timestamp())
             .withColumn("source_file", lit(file_name)))

(df_bronze.write.mode("overwrite").parquet(PARQ))
🟦 Silver — Limpar, padronizar e enriquecer
Objetivo: transformar Bronze em dataset confiável com tipos consistentes e colunas de tempo.
A Silver corrige casos de CSV lido como 1 coluna, cria data_evento quando aplicável, adiciona metadados e salva em Delta.
Exemplo (versão simples e robusta):
from pyspark.sql import functions as F
import os

BASE = "/Volumes/bh/mobilidade/data_mobility_bh"
BRONZE_PARQ = f"{BASE}/bronze/parquet"
SILVER_DIR  = f"{BASE}/silver/mco"

df = spark.read.parquet(BRONZE_PARQ)
if len(df.columns) == 1:
    unico = df.columns[0]
    df = df.withColumn("split", F.split(F.col(unico), ";"))
    max_cols = df.select(F.size("split")).first()[0]
    for i in range(max_cols):
        df = df.withColumn(f"col_{i}", F.col("split")[i])
    df = df.drop("split").drop(unico)

col_data = next((c for c in df.columns if "data" in c.lower()), None)
df = df.withColumn("data_evento", F.to_date(F.col(col_data))) if col_data else df.withColumn("data_evento", F.lit(None).cast("date"))
df = df.withColumn("silver_timestamp", F.current_timestamp())

os.makedirs(SILVER_DIR, exist_ok=True)
(df.write.format("delta").mode("overwrite").save(SILVER_DIR))
🟨 Gold — KPIs e métricas para BI/ML
Objetivo: entregar tabelas agregadas, estáveis e rápidas para consumo. A Gold tenta agrupar por linha e data_evento se existirem; caso contrário, produz métricas mínimas (total_registros).
Exemplo (adaptável às colunas existentes):
from pyspark.sql import functions as F
import os

BASE = "/Volumes/bh/mobilidade/data_mobility_bh"
SILVER_DIR = f"{BASE}/silver/mco"
GOLD_DIR   = f"{BASE}/gold/mco_kpis"

df_silver = spark.read.format("delta").load(SILVER_DIR)
cols = df_silver.columns
has_linha = "linha" in cols
has_data  = "data_evento" in cols
has_placa = "placa" in cols
has_hora  = "data_hora" in cols

if has_linha and has_data:
    aggs = [F.count(F.lit(1)).alias("qtde_registros")]
    if has_hora:  aggs.append(F.approx_count_distinct("data_hora").alias("qtde_instantes"))
    if has_placa: aggs.append(F.approx_count_distinct("placa").alias("veiculos_distintos"))
    df_gold = (df_silver.groupBy("linha", "data_evento").agg(*aggs)
               .withColumn("gold_generated_at_utc", F.current_timestamp()))
else:
    df_gold = (df_silver.agg(F.count(F.lit(1)).alias("total_registros"))
               .withColumn("gold_generated_at_utc", F.current_timestamp()))

os.makedirs(GOLD_DIR, exist_ok=True)
(df_gold.write.format("delta").mode("overwrite").save(GOLD_DIR))
⚙️ Orquestração (Databricks Workflows)
Crie um Job com 3 Tasks: bronze_ingest → silver_ingest → gold_ingest, com dependências nessa ordem, Retries=2, Timeout=30min, e agendamento diário se necessário. Configure alertas com o e-mail em caso de falha.
▶️ Como executar
1) Criar Catalog/Schema/Volume no Unity Catalog (uma vez).
2) Executar Bronze (download + parquet).
3) Executar Silver (Delta limpo).
4) Executar Gold (KPIs).
5) Orquestrar no Databricks Workflows.
6) Validar com sanity check e consultas SQL/Notebook.
📖 Dicionário de Dados — Camada Gold (exemplo)
Tabela: mco_kpis (Delta)
- linha (string, opcional): código da linha de ônibus.
- data_evento (date, opcional): data derivada do evento.
- qtde_registros (long, opcional): total por linha/data.
- qtde_instantes (long, opcional): instantes distintos (se existir data_hora).
- veiculos_distintos (long, opcional): placas distintas (se existir).
- gold_generated_at_utc (timestamp): carimbo de geração.
🧠 Decisões Técnicas
Unity Catalog + Volumes (governança, segurança, substitui dbfs:/), Parquet na Bronze, Delta na Silver/Gold, checks simples de qualidade, particionamento por data_evento, e possibilidade de otimizações (OPTIMIZE/Z-ORDER).
🚀 Próximos Passos
Aprimorar schema explícito na Silver; expectativas com Great Expectations/Delta Live Tables; registrar tabelas no UC usando URI de nuvem do Volume; criar dashboard no Databricks SQL/Power BI; parametrização de notebooks e Z-ORDER na Gold.
