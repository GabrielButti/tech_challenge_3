"""
Extração das bases municipais de População e PIB do IBGE.

Fonte:
Base dos Dados / Google BigQuery

Datasets:
- br_ibge_populacao.municipio
- br_ibge_pib.municipio

Nesta etapa os dados são apenas extraídos e armazenados.
Nenhuma engenharia de atributos ou join é realizado.
"""

import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account


# ============================================================
# CONFIGURAÇÕES
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

ENV_PATH = PROJECT_ROOT / ".env"

OUTPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "external"
    / "ibge"
)

OUTPUT_DIR.mkdir(
    parents=True,
    exist_ok=True
)


# ============================================================
# VARIÁVEIS DE AMBIENTE
# ============================================================

load_dotenv(ENV_PATH)

BILLING_PROJECT_ID = os.getenv(
    "BILLING_PROJECT_ID"
)

GOOGLE_APPLICATION_CREDENTIALS = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS"
)

if not BILLING_PROJECT_ID:
    raise ValueError(
        "BILLING_PROJECT_ID não encontrado no .env"
    )

if not GOOGLE_APPLICATION_CREDENTIALS:
    raise ValueError(
        "GOOGLE_APPLICATION_CREDENTIALS "
        "não encontrado no .env"
    )


# ============================================================
# BIGQUERY
# ============================================================

credentials = (
    service_account
    .Credentials
    .from_service_account_file(
        GOOGLE_APPLICATION_CREDENTIALS
    )
)

client = bigquery.Client(
    project=BILLING_PROJECT_ID,
    credentials=credentials
)


# ============================================================
# QUERIES
# ============================================================

QUERY_POPULACAO = """
SELECT
    ano,
    sigla_uf,
    id_municipio,
    populacao

FROM `basedosdados.br_ibge_populacao.municipio`

WHERE ano BETWEEN 2020 AND 2024

ORDER BY
    ano,
    id_municipio
"""


QUERY_PIB = """
SELECT
    id_municipio,
    ano,
    pib,
    impostos_liquidos,
    va,
    va_agropecuaria,
    va_industria,
    va_servicos,
    va_adespss

FROM `basedosdados.br_ibge_pib.municipio`

WHERE ano BETWEEN 2020 AND 2023

ORDER BY
    ano,
    id_municipio
"""


# ============================================================
# EXTRAÇÃO
# ============================================================

def extract_query(
    query: str,
    nome: str
) -> pd.DataFrame:
    """
    Executa uma consulta no BigQuery.
    """

    print("\n" + "=" * 70)
    print(f"EXTRAINDO: {nome.upper()}")
    print("=" * 70)

    dataframe = (
        client
        .query(query)
        .to_dataframe()
    )

    print(
        f"Registros: {len(dataframe):,}"
    )

    print(
        f"Colunas: {len(dataframe.columns)}"
    )

    return dataframe


# ============================================================
# VALIDAÇÃO
# ============================================================

def validate_data(
    dataframe: pd.DataFrame,
    nome: str
) -> None:
    """
    Executa verificações mínimas sobre os dados extraídos.
    """

    print(f"\nValidação: {nome}")

    chave = [
        "ano",
        "id_municipio"
    ]

    duplicados = (
        dataframe
        .duplicated(
            subset=chave
        )
        .sum()
    )

    print(
        f"Duplicidades ano + município: "
        f"{duplicados:,}"
    )

    if "ano" in dataframe.columns:

        resumo = (
            dataframe
            .groupby("ano")
            .agg(
                registros=("id_municipio", "size"),
                municipios=(
                    "id_municipio",
                    "nunique"
                )
            )
        )

        print("\nCobertura temporal:")
        print(resumo)


# ============================================================
# SALVAMENTO
# ============================================================

def save_parquet(
    dataframe: pd.DataFrame,
    filename: str
) -> None:
    """
    Salva DataFrame em Parquet.
    """

    output_path = (
        OUTPUT_DIR
        / filename
    )

    dataframe.to_parquet(
        output_path,
        index=False
    )

    print(
        f"\nArquivo salvo:\n"
        f"{output_path}"
    )


# ============================================================
# MAIN
# ============================================================

def main():
    """
    Executa todas as extrações do IBGE.
    """

    print("=" * 70)
    print("EXTRAÇÃO IBGE")
    print("=" * 70)

    df_populacao = extract_query(
        QUERY_POPULACAO,
        "População Municipal"
    )

    validate_data(
        df_populacao,
        "População Municipal"
    )

    save_parquet(
        df_populacao,
        "populacao_municipio.parquet"
    )

    df_pib = extract_query(
        QUERY_PIB,
        "PIB Municipal"
    )

    validate_data(
        df_pib,
        "PIB Municipal"
    )

    save_parquet(
        df_pib,
        "pib_municipio.parquet"
    )

    print("\n" + "=" * 70)
    print("EXTRAÇÃO IBGE CONCLUÍDA")
    print("=" * 70)


if __name__ == "__main__":
    main()