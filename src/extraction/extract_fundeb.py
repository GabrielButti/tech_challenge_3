"""
Extração dos Indicadores do FUNDEB.

Fonte:
Base dos Dados / Google BigQuery

Tabelas:
- indicador_municipal
- indicador_estadual
- dicionario

Nesta etapa os dados são extraídos em seu formato tratado
disponibilizado pela Base dos Dados.

Nenhum pivot, join, seleção de indicadores ou engenharia
de atributos é realizado nesta etapa.
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
    / "fundeb"
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

QUERY_MUNICIPAL = """
SELECT *
FROM `basedosdados.br_fnde_fundeb.indicador_municipal`
WHERE ano IN (2022, 2023, 2024)
"""


QUERY_ESTADUAL = """
SELECT *
FROM `basedosdados.br_fnde_fundeb.indicador_estadual`
WHERE ano IN (2022, 2023, 2024)
"""


QUERY_DICIONARIO = """
SELECT *
FROM `basedosdados.br_fnde_fundeb.dicionario`
"""


# ============================================================
# ESTIMATIVA DE PROCESSAMENTO
# ============================================================

def estimate_query(
    query: str,
    nome: str
) -> None:
    """
    Estima a quantidade de dados processados pela consulta
    utilizando o modo dry run do BigQuery.

    Caso o BigQuery não retorne uma estimativa, a execução
    continua normalmente.
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
            "o processamento da consulta."
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
    nome: str
) -> pd.DataFrame:
    """
    Executa uma consulta no BigQuery e retorna
    o resultado como DataFrame.
    """

    print("\n" + "=" * 70)
    print(f"EXTRAÇÃO: {nome.upper()}")
    print("=" * 70)

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
# VALIDAÇÃO GENÉRICA
# ============================================================

def validate_data(
    dataframe: pd.DataFrame,
    nome: str
) -> None:
    """
    Executa verificações estruturais básicas com baixo
    consumo adicional de memória.

    Validações mais pesadas, como análise de duplicidades
    pela chave de negócio, serão executadas posteriormente
    na etapa de qualidade e EDA.
    """

    print("\n" + "=" * 70)
    print(f"VALIDAÇÃO: {nome.upper()}")
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
        f"\nMemória aproximada do DataFrame: "
        f"{memoria_mb:,.2f} MB"
    )

    # --------------------------------------------------------
    # Colunas
    # --------------------------------------------------------

    print("\nColunas:")

    for coluna in dataframe.columns:

        print(
            f"- {coluna} "
            f"({dataframe[coluna].dtype})"
        )

    # --------------------------------------------------------
    # Cobertura temporal
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
    # UFs
    # --------------------------------------------------------

    if "sigla_uf" in dataframe.columns:

        print(
            "\nUFs distintas: "
            f"{dataframe['sigla_uf'].nunique():,}"
        )

    # --------------------------------------------------------
    # Valores nulos
    # --------------------------------------------------------

    print("\nValores nulos por coluna:")

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

        print(
            f"- {coluna}: "
            f"{quantidade_nulos:,} "
            f"({percentual_nulos:.2f}%)"
        )

    # --------------------------------------------------------
    # Duplicidades
    # --------------------------------------------------------

    print(
        "\nDuplicidades completas não verificadas "
        "nesta etapa devido ao volume de dados."
    )

    print(
        "A unicidade será avaliada posteriormente "
        "utilizando a chave de negócio correta."
    )


# ============================================================
# VALIDAÇÃO DO DICIONÁRIO
# ============================================================

def validate_dictionary(
    dataframe: pd.DataFrame
) -> None:
    """
    Exibe informações estruturais básicas sobre
    o dicionário do FUNDEB.
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
# LIBERAÇÃO DE MEMÓRIA
# ============================================================

def release_memory(
    dataframe_name: str
) -> None:
    """
    Executa o garbage collector e informa a liberação
    de memória entre as etapas da extração.
    """

    print(
        f"\nLiberando memória após "
        f"{dataframe_name}..."
    )

    gc.collect()


# ============================================================
# MAIN
# ============================================================

def main():
    """
    Executa a extração das três tabelas do FUNDEB.
    """

    print("=" * 70)
    print("EXTRAÇÃO FUNDEB")
    print("=" * 70)

    print(
        "\nDataset: "
        "basedosdados.br_fnde_fundeb"
    )

    print(
        "\nPeríodo selecionado para os indicadores: "
        "2022 a 2024"
    )

    # ========================================================
    # INDICADORES MUNICIPAIS
    # ========================================================

    df_municipal = extract_query(
        QUERY_MUNICIPAL,
        "Indicadores Municipais"
    )

    # Salvar primeiro para não perder a extração
    # caso alguma validação posterior apresente erro.

    save_parquet(
        df_municipal,
        "indicador_municipal.parquet"
    )

    validate_data(
        df_municipal,
        "Indicadores Municipais"
    )

    del df_municipal

    release_memory(
        "Indicadores Municipais"
    )

    # ========================================================
    # INDICADORES ESTADUAIS
    # ========================================================

    df_estadual = extract_query(
        QUERY_ESTADUAL,
        "Indicadores Estaduais"
    )

    save_parquet(
        df_estadual,
        "indicador_estadual.parquet"
    )

    validate_data(
        df_estadual,
        "Indicadores Estaduais"
    )

    del df_estadual

    release_memory(
        "Indicadores Estaduais"
    )

    # ========================================================
    # DICIONÁRIO
    # ========================================================

    df_dicionario = extract_query(
        QUERY_DICIONARIO,
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

    release_memory(
        "Dicionário"
    )

    # ========================================================
    # RESUMO
    # ========================================================

    print("\n" + "=" * 70)
    print("EXTRAÇÃO FUNDEB CONCLUÍDA")
    print("=" * 70)

    print("\nArquivos gerados:")

    print(
        "- indicador_municipal.parquet"
    )

    print(
        "- indicador_estadual.parquet"
    )

    print(
        "- dicionario.parquet"
    )


if __name__ == "__main__":
    main()