from processamento_dados import Dados

# Dados 

path_json = '/home/pipeline-dados/Documentos/pipeline_dados/data_raw/dados_empresaA.json'
path_csv = '/home/pipeline-dados/Documentos/pipeline_dados/data_raw/dados_empresaB.csv'

# Extract

dados_empresa_A = Dados(path_json, 'json')
print(f"Arquivo: {dados_empresa_A.path}, Tipo de estrutura: {dados_empresa_A.tipo_dados}")

dados_empresa_B = Dados(path_csv, 'csv')
print(f"Arquivo: {dados_empresa_B.path}, Tipo de estrutura: {dados_empresa_B.tipo_dados}")

print(dados_empresa_A.dados[0])
print(dados_empresa_B.dados[0])
print(dados_empresa_A.nome_colunas, dados_empresa_B.nome_colunas)
print(dados_empresa_A.qtd_linhas)
print(dados_empresa_B.qtd_linhas)

# Transform

key_mapping = {'Nome do Item': 'Nome do Produto',
               'Classificação do Produto': 'Categoria do Produto',
               'Valor em Reais (R$)': 'Preço do Produto',
               'Quantidade em Estoque': 'Quantidade em Estoque',
               'Nome da Loja': 'Filial',
                'Data da Venda': 'Data da Venda'
              }

dados_empresa_B.rename_columns(key_mapping)
print(f"Validação new name columns: {dados_empresa_B.nome_colunas}")

dados_fusao = Dados.join(dados_empresa_A, dados_empresa_B)
print(dados_fusao.nome_colunas)
print(dados_fusao.qtd_linhas)

# Load

path_dados_combinados = '/home/pipeline-dados/Documentos/pipeline_dados/data_processed/dados_combinados_final_v1.csv'
dados_fusao.salvados_dados(path_dados_combinados)
print(path_dados_combinados)
