"""
Extração dos indicadores do Atlas do Desenvolvimento Humano.

Fonte:
Base dos Dados / Google BigQuery

Dataset:
mundo_onu_adh

Tabela principal:
- municipio

Nesta etapa os dados são extraídos integralmente.

Nenhuma seleção de features, join ou engenharia de atributos
é realizada nesta etapa.
"""

import gc
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
    / "atlas"
)

OUTPUT_DIR.mkdir(
    parents=True,
    exist_ok=True
)


DATASET = "mundo_onu_adh"

TABELA_MUNICIPIO = "municipio"

TABELA_DICIONARIO = "dicionario"


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
# CLIENTE BIGQUERY
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

QUERY_MUNICIPIO = f"""
SELECT *
FROM `basedosdados.{DATASET}.{TABELA_MUNICIPIO}`
ORDER BY
    ano,
    id_municipio
"""


QUERY_SCHEMA = f"""
SELECT
    ordinal_position,
    column_name,
    data_type

FROM
    `basedosdados.{DATASET}.INFORMATION_SCHEMA.COLUMNS`

WHERE
    table_name = '{TABELA_MUNICIPIO}'

ORDER BY
    ordinal_position
"""


# ============================================================
# VERIFICAÇÃO DE TABELAS
# ============================================================

def table_exists(
    table_name: str
) -> bool:
    """
    Verifica se uma tabela existe no dataset do Atlas.
    """

    query = f"""
    SELECT
        COUNT(*) AS quantidade

    FROM
        `basedosdados.{DATASET}.INFORMATION_SCHEMA.TABLES`

    WHERE
        table_name = '{table_name}'
    """

    result = (
        client
        .query(query)
        .to_dataframe()
    )

    return (
        result.loc[0, "quantidade"] > 0
    )


# ============================================================
# ESTIMATIVA DE PROCESSAMENTO
# ============================================================

def estimate_query(
    query: str,
    nome: str
) -> None:
    """
    Estima a quantidade de dados processados pela consulta.

    Caso o BigQuery não disponibilize a estimativa,
    a execução continua normalmente.
    """

    print(
        f"\nEstimando processamento - {nome}..."
    )

    try:

        job_config = bigquery.QueryJobConfig(
            dry_run=True,
            use_query_cache=False
        )

        query_job = client.query(
            query,
            job_config=job_config
        )

        bytes_processados = (
            query_job.total_bytes_processed
        )

        if bytes_processados is None:

            print(
                "Estimativa de processamento "
                "não disponível."
            )

            return

        mb_processados = (
            bytes_processados
            / 1024**2
        )

        gb_processados = (
            bytes_processados
            / 1024**3
        )

        if gb_processados >= 1:

            print(
                f"Dados estimados: "
                f"{gb_processados:,.2f} GB"
            )

        else:

            print(
                f"Dados estimados: "
                f"{mb_processados:,.2f} MB"
            )

    except Exception as erro:

        print(
            "Não foi possível estimar "
            "o processamento."
        )

        print(
            f"Motivo: {erro}"
        )

        print(
            "A extração continuará normalmente."
        )


# ============================================================
# EXTRAÇÃO
# ============================================================

def extract_query(
    query: str,
    nome: str,
    estimate: bool = True
) -> pd.DataFrame:
    """
    Executa uma consulta no BigQuery.
    """

    print("\n" + "=" * 70)
    print(f"EXTRAÇÃO: {nome.upper()}")
    print("=" * 70)

    if estimate:

        estimate_query(
            query,
            nome
        )

    print("\nExecutando consulta...")

    dataframe = (
        client
        .query(query)
        .to_dataframe()
    )

    print(
        f"\nRegistros extraídos: "
        f"{len(dataframe):,}"
    )

    print(
        f"Colunas extraídas: "
        f"{len(dataframe.columns)}"
    )

    return dataframe


# ============================================================
# VALIDAÇÃO - MUNICÍPIO
# ============================================================

def validate_municipio(
    dataframe: pd.DataFrame
) -> None:
    """
    Executa validações estruturais básicas sobre
    os indicadores municipais do Atlas.

    Nenhuma transformação é realizada.
    """

    print("\n" + "=" * 70)
    print("VALIDAÇÃO: ATLAS MUNICIPAL")
    print("=" * 70)

    # --------------------------------------------------------
    # Shape
    # --------------------------------------------------------

    print(
        f"\nShape: "
        f"{dataframe.shape}"
    )

    # --------------------------------------------------------
    # Memória
    # --------------------------------------------------------

    memoria_mb = (
        dataframe
        .memory_usage(
            deep=False
        )
        .sum()
        / 1024**2
    )

    print(
        f"\nMemória aproximada: "
        f"{memoria_mb:,.2f} MB"
    )

    # --------------------------------------------------------
    # Anos
    # --------------------------------------------------------

    if "ano" in dataframe.columns:

        print("\nRegistros por ano:")

        print(
            dataframe[
                "ano"
            ]
            .value_counts(
                dropna=False
            )
            .sort_index()
        )

    # --------------------------------------------------------
    # Municípios
    # --------------------------------------------------------

    if "id_municipio" in dataframe.columns:

        print(
            "\nMunicípios distintos: "
            f"{dataframe['id_municipio'].nunique():,}"
        )

    # --------------------------------------------------------
    # Chave ano + município
    # --------------------------------------------------------

    if {
        "ano",
        "id_municipio"
    }.issubset(
        dataframe.columns
    ):

        quantidade_chaves = (
            dataframe[
                [
                    "ano",
                    "id_municipio"
                ]
            ]
            .drop_duplicates()
            .shape[0]
        )

        print(
            "\nChaves distintas "
            "(ano + id_municipio): "
            f"{quantidade_chaves:,}"
        )

        if quantidade_chaves == len(dataframe):

            print(
                "Chave ano + id_municipio: "
                "única."
            )

        else:

            print(
                "ATENÇÃO: existem múltiplos registros "
                "para ano + id_municipio."
            )

    # --------------------------------------------------------
    # Missing
    # --------------------------------------------------------

    print("\nColunas com maior percentual de valores nulos:")

    missing = []

    for coluna in dataframe.columns:

        quantidade_nulos = (
            len(dataframe)
            - dataframe[coluna].count()
        )

        percentual_nulos = (
            quantidade_nulos
            / len(dataframe)
            * 100
            if len(dataframe) > 0
            else 0
        )

        if quantidade_nulos > 0:

            missing.append(
                {
                    "coluna": coluna,
                    "nulos": quantidade_nulos,
                    "percentual": percentual_nulos
                }
            )

    if missing:

        df_missing = (
            pd.DataFrame(missing)
            .sort_values(
                "percentual",
                ascending=False
            )
            .head(20)
        )

        print(
            df_missing.to_string(
                index=False
            )
        )

    else:

        print(
            "Nenhuma coluna contém valores nulos."
        )


# ============================================================
# VALIDAÇÃO DO DICIONÁRIO
# ============================================================

def validate_dictionary(
    dataframe: pd.DataFrame
) -> None:
    """
    Exibe informações estruturais básicas do
    dicionário, quando disponível.
    """

    print("\n" + "=" * 70)
    print("VALIDAÇÃO: DICIONÁRIO")
    print("=" * 70)

    print(
        f"\nRegistros: "
        f"{len(dataframe):,}"
    )

    print(
        f"Colunas: "
        f"{len(dataframe.columns)}"
    )

    print("\nSchema:")

    for coluna in dataframe.columns:

        print(
            f"- {coluna} "
            f"({dataframe[coluna].dtype})"
        )

    print("\nPrimeiros registros:")

    print(
        dataframe
        .head(20)
        .to_string(
            index=False
        )
    )


# ============================================================
# SALVAMENTO
# ============================================================

def save_parquet(
    dataframe: pd.DataFrame,
    filename: str
) -> None:
    """
    Salva um DataFrame em formato Parquet.
    """

    output_path = (
        OUTPUT_DIR
        / filename
    )

    print(
        f"\nSalvando arquivo: "
        f"{filename}..."
    )

    dataframe.to_parquet(
        output_path,
        index=False,
        compression="snappy"
    )

    tamanho_mb = (
        output_path.stat().st_size
        / 1024**2
    )

    print(
        f"\nArquivo salvo:\n"
        f"{output_path}"
    )

    print(
        f"Tamanho: "
        f"{tamanho_mb:,.2f} MB"
    )


# ============================================================
# MAIN
# ============================================================

def main():
    """
    Executa a extração completa dos dados do
    Atlas do Desenvolvimento Humano.
    """

    print("=" * 70)
    print("EXTRAÇÃO ATLAS DO DESENVOLVIMENTO HUMANO")
    print("=" * 70)

    print(
        f"\nDataset: "
        f"basedosdados.{DATASET}"
    )

    # ========================================================
    # SCHEMA
    # ========================================================

    df_schema = extract_query(
        QUERY_SCHEMA,
        "Schema Município",
        estimate=False
    )

    save_parquet(
        df_schema,
        "schema_municipio.parquet"
    )

    print(
        f"\nVariáveis disponíveis no Atlas: "
        f"{len(df_schema):,}"
    )

    del df_schema
    gc.collect()

    # ========================================================
    # MUNICÍPIO
    # ========================================================

    df_municipio = extract_query(
        QUERY_MUNICIPIO,
        "Indicadores Municipais"
    )

    # Salva primeiro para evitar perder
    # a extração caso a validação falhe.

    save_parquet(
        df_municipio,
        "municipio.parquet"
    )

    validate_municipio(
        df_municipio
    )

    del df_municipio
    gc.collect()

    # ========================================================
    # DICIONÁRIO
    # ========================================================

    if table_exists(
        TABELA_DICIONARIO
    ):

        query_dicionario = f"""
        SELECT *
        FROM `basedosdados.{DATASET}.{TABELA_DICIONARIO}`
        """

        df_dicionario = extract_query(
            query_dicionario,
            "Dicionário"
        )

        save_parquet(
            df_dicionario,
            "dicionario.parquet"
        )

        validate_dictionary(
            df_dicionario
        )

        del df_dicionario
        gc.collect()

    else:

        print("\n" + "=" * 70)
        print("DICIONÁRIO")
        print("=" * 70)

        print(
            "\nTabela 'dicionario' não encontrada "
            "no dataset."
        )

        print(
            "O schema da tabela foi salvo em "
            "'schema_municipio.parquet' como referência."
        )

    # ========================================================
    # FINALIZAÇÃO
    # ========================================================

    print("\n" + "=" * 70)
    print("EXTRAÇÃO ATLAS CONCLUÍDA")
    print("=" * 70)

    print("\nArquivos gerados:")

    print(
        "- municipio.parquet"
    )

    print(
        "- schema_municipio.parquet"
    )

    if table_exists(
        TABELA_DICIONARIO
    ):

        print(
            "- dicionario.parquet"
        )


if __name__ == "__main__":
    main()