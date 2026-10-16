"""
Extração de dados do Censo Escolar e seu respectivo dicionário.

Fonte:
Base dos Dados / Google BigQuery

Dataset:
br_inep_censo_escolar

Tabelas utilizadas:
- escola
- dicionario

Nesta etapa os dados são apenas extraídos e armazenados.
Nenhum join ou feature engineering é realizado.
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
    / "censo_escolar"
)

OUTPUT_DIR.mkdir(
    parents=True,
    exist_ok=True
)

ESCOLA_OUTPUT_PATH = (
    OUTPUT_DIR
    / "escola.parquet"
)

DICIONARIO_OUTPUT_PATH = (
    OUTPUT_DIR
    / "dicionario.parquet"
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
# QUERY - ESCOLA
# ============================================================

QUERY_ESCOLA = """
SELECT

    -- ========================================================
    -- IDENTIFICAÇÃO
    -- ========================================================

    ano,
    sigla_uf,
    id_municipio,
    id_escola,
    rede,

    -- ========================================================
    -- CARACTERÍSTICAS DA ESCOLA
    -- ========================================================

    tipo_categoria_escola_privada,
    tipo_localizacao,
    tipo_localizacao_diferenciada,
    tipo_situacao_funcionamento,

    -- ========================================================
    -- INFRAESTRUTURA BÁSICA
    -- ========================================================

    agua_potavel,
    agua_rede_publica,
    energia_rede_publica,
    esgoto_rede_publica,
    lixo_servico_coleta,

    -- ========================================================
    -- ESTRUTURA PEDAGÓGICA
    -- ========================================================

    biblioteca,
    biblioteca_sala_leitura,
    sala_leitura,
    laboratorio_ciencias,
    laboratorio_informatica,
    quadra_esportes,
    refeitorio,

    -- ========================================================
    -- ACESSIBILIDADE
    -- ========================================================

    dependencia_pne,
    acessibilidade_inexistente,
    acessibilidade_rampas,

    -- ========================================================
    -- SALAS
    -- ========================================================

    quantidade_sala_existente,
    quantidade_sala_utilizada,
    quantidade_sala_utilizada_climatizada,
    quantidade_sala_utilizada_acessivel,

    -- ========================================================
    -- TECNOLOGIA
    -- ========================================================

    internet,
    internet_alunos,
    internet_aprendizagem,
    banda_larga,
    quantidade_computador_aluno,
    quantidade_computador_portatil_aluno,
    quantidade_tablet_aluno,
    quantidade_equipamento_lousa_digital,
    quantidade_equipamento_multimidia,

    -- ========================================================
    -- PROFISSIONAIS
    -- ========================================================

    quantidade_funcionario,
    quantidade_profissional_bibliotecario,
    quantidade_profissional_coordenador,
    quantidade_profissional_pedagogia,
    quantidade_profissional_psicologo,
    quantidade_profissional_assistente_social,

    -- ========================================================
    -- MATERIAIS PEDAGÓGICOS
    -- ========================================================

    material_pedagogico_multimidia,
    material_pedagogico_jogo,
    material_pedagogico_nenhum,

    -- ========================================================
    -- ORGANIZAÇÃO ESCOLAR
    -- ========================================================

    organizacao_serie_ano,
    etapa_ensino_fundamental_anos_iniciais,

    -- ========================================================
    -- MATRÍCULAS
    -- ========================================================

    quantidade_matricula_infantil_pre_escola,
    quantidade_matricula_fundamental_anos_iniciais,
    quantidade_matricula_fundamental_2_ano,

    quantidade_matricula_zona_residencia_urbana,
    quantidade_matricula_zona_residencia_rural,
    quantidade_matricula_utiliza_transporte_publico,

    -- ========================================================
    -- DOCENTES
    -- ========================================================

    quantidade_docente_fundamental_anos_iniciais,

    -- ========================================================
    -- TURMAS
    -- ========================================================

    quantidade_turma_fundamental_anos_iniciais,
    quantidade_turma_fundamental_anos_iniciais_integral

FROM
    `basedosdados.br_inep_censo_escolar.escola`

WHERE
    ano IN (2022, 2023, 2024)
"""


# ============================================================
# QUERY - DICIONÁRIO
# ============================================================

QUERY_DICIONARIO = """
SELECT *
FROM `basedosdados.br_inep_censo_escolar.dicionario`
"""


# ============================================================
# FUNÇÃO GENÉRICA DE EXTRAÇÃO
# ============================================================

def extract_query(
    query: str,
    nome: str
) -> pd.DataFrame:
    """
    Executa uma consulta no BigQuery e retorna um DataFrame.
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
        f"\nRegistros extraídos: "
        f"{len(dataframe):,}"
    )

    print(
        f"Colunas extraídas: "
        f"{len(dataframe.columns)}"
    )

    return dataframe


# ============================================================
# VALIDAÇÃO - ESCOLA
# ============================================================

def validate_escola(
    dataframe: pd.DataFrame
) -> None:
    """
    Executa validações básicas sobre a base de escolas.
    """

    print("\n" + "=" * 70)
    print("VALIDAÇÃO - ESCOLA")
    print("=" * 70)

    duplicados = (
        dataframe
        .duplicated(
            subset=[
                "ano",
                "id_escola"
            ]
        )
        .sum()
    )

    print(
        f"\nDuplicidades ano + id_escola: "
        f"{duplicados:,}"
    )

    print("\nCobertura por ano:")

    resumo = (
        dataframe
        .groupby("ano")
        .agg(
            registros=(
                "id_escola",
                "size"
            ),
            escolas=(
                "id_escola",
                "nunique"
            ),
            municipios=(
                "id_municipio",
                "nunique"
            )
        )
    )

    print(resumo)

    print("\nDistribuição por rede:")

    print(
        dataframe[
            "rede"
        ]
        .value_counts(
            dropna=False
        )
    )


# ============================================================
# VALIDAÇÃO - DICIONÁRIO
# ============================================================

def validate_dicionario(
    dataframe: pd.DataFrame
) -> None:
    """
    Executa validações básicas sobre o dicionário.
    """

    print("\n" + "=" * 70)
    print("VALIDAÇÃO - DICIONÁRIO")
    print("=" * 70)

    print(
        f"\nRegistros: "
        f"{len(dataframe):,}"
    )

    print(
        f"Colunas: "
        f"{len(dataframe.columns)}"
    )

    print("\nColunas disponíveis:")

    for coluna in dataframe.columns:
        print(f"- {coluna}")

    print("\nPrimeiros registros:")

    print(
        dataframe
        .head(10)
        .to_string(
            index=False
        )
    )


# ============================================================
# SALVAMENTO
# ============================================================

def save_parquet(
    dataframe: pd.DataFrame,
    output_path: Path
) -> None:
    """
    Salva um DataFrame em formato Parquet.
    """

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
    Executa a extração completa do Censo Escolar.
    """

    print("=" * 70)
    print("EXTRAÇÃO CENSO ESCOLAR")
    print("=" * 70)

    # --------------------------------------------------------
    # Escola
    # --------------------------------------------------------

    df_escola = extract_query(
        QUERY_ESCOLA,
        "Censo Escolar - Escola"
    )

    validate_escola(
        df_escola
    )

    save_parquet(
        df_escola,
        ESCOLA_OUTPUT_PATH
    )

    # --------------------------------------------------------
    # Dicionário
    # --------------------------------------------------------

    df_dicionario = extract_query(
        QUERY_DICIONARIO,
        "Censo Escolar - Dicionário"
    )

    validate_dicionario(
        df_dicionario
    )

    save_parquet(
        df_dicionario,
        DICIONARIO_OUTPUT_PATH
    )

    # --------------------------------------------------------
    # Fim
    # --------------------------------------------------------

    print("\n" + "=" * 70)
    print("EXTRAÇÃO CENSO ESCOLAR CONCLUÍDA")
    print("=" * 70)

    print("\nArquivos gerados:")

    print(
        f"- {ESCOLA_OUTPUT_PATH}"
    )

    print(
        f"- {DICIONARIO_OUTPUT_PATH}"
    )


if __name__ == "__main__":
    main()