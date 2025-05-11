Explorando Estatística com Pandas, Numpy e Python na Engenharia de Software.
Comparação de Desempenho entre Bancos de Dados SQL e NoSQL


Objetivo do Projeto:

O objetivo deste projeto é comparar a performance de um banco de dados relacional (SQL) com um banco de dados não relacional (NoSQL) para operações de leitura e escrita. Usando ferramentas de análise estatística como Pandas e Numpy, realizaremos a medição de tempo (benchmarking) e throughput para entender qual banco de dados é mais eficiente em diferentes cenários de carga e tipos de operações. O projeto permitirá identificar as vantagens e limitações de cada tipo de banco, fornecendo insights valiosos para a escolha da infraestrutura de dados.

Bancos de dados relacionais são baseados no modelo relacional, onde os dados são armazenados em tabelas que se relacionam entre si. Eles seguem um esquema rígido e possuem uma estrutura de dados bem definida, com tabelas, colunas e chaves primárias/estrangeiras.

Bancos de dados não relacionais (NoSQL) são projetados para lidar com dados não estruturados ou semi-estruturados, oferecendo maior flexibilidade no armazenamento e processamento de dados. Não seguem o modelo de tabelas e geralmente não exigem um esquema fixo.


Importância de comparar o desempenho:

A comparação entre bancos de dados relacional e não relacional não é apenas sobre escolher qual banco de dados é mais rápido, mas sobre entender as necessidades do seu sistema e como cada banco de dados se comporta em relação a métricas de desempenho específicas.

Se a consistência de dados e transações complexas são essenciais, bancos SQL podem ser a escolha ideal, mas com potenciais limitações em throughput e escalabilidade.

Se o foco está na escalabilidade horizontal, alta disponibilidade e desempenho em grandes volumes de dados, bancos NoSQL são vantajosos.

Portanto, medir o desempenho e comparar os valores de latência, throughput, escalabilidade e uso de recursos entre SQL e NoSQL é essencial para otimizar a infraestrutura de dados de uma empresa e garantir que ela atenda às exigências de desempenho, confiabilidade e escalabilidade do sistema.




# Preparação do ambiente Linux, utilizando o WSL do Windows para rodar o Docker e utilizar containers do SQL Server e MongoDB


1. Instalação do sistema operacional Linux (Ubuntu). Rodar o comando abaixo no PowerShell do Windows:
# wsl --install -d Ubuntu-22.04

2. Instalação do Docker dentro do Ubuntu. Rodar os comandos abaixo no WSL:
# sudo apt update
# sudo apt install -y ca-certificates curl gnupg lsb-release

# sudo mkdir -p /etc/apt/keyrings
# curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
# sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

# echo \
#  "deb [arch=$(dpkg --print-architecture) \
#  signed-by=/etc/apt/keyrings/docker.gpg] \
#  https://download.docker.com/linux/ubuntu \
#  $(lsb_release -cs) stable" | \
#  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# sudo apt update
# sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

3. Instalar a imagem do SQL Server e criar um container dentro do Docker. Rodar o comando abaixo no WSL:
# docker run -e "ACCEPT_EULA=Y" \
#           -e "SA_PASSWORD=SuaSenha!123" \
#           -p 1433:1433 \
#           --name sqlserver \
#           -d mcr.microsoft.com/mssql/server:2022-latest

4. Instalar a imagem do MongoDB e criar um container dentro do Docker. Rodar o comando abaixo no WSL:
# docker run -d \
#  --name mongodb \
#  -e MONGO_INITDB_ROOT_USERNAME=root \
#  -e MONGO_INITDB_ROOT_PASSWORD=senhamongo \
#  -p 27017:27017 \
#  mongo:7

5. Instalar o pyenv para o Windows (gerenciador de versões do Python). Rodar os comandos abaixo no PowerShell:
# Invoke-WebRequest -UseBasicParsing https://pyenv.run | Invoke-Expression

# $env:PYENV = "$HOME\.pyenv\pyenv-win"
# $env:PATH = "$env:PYENV\bin;$env:PYENV\shims;" + $env:PATH

6. Instalar a versão do Python desejada através do pyenv. Rodar os comandos abaixo no PowerShell:
# pyenv install --list

# pyenv install 3.xx.xx

7. Instalar o poetry (gerenciador de dependências do Python). Rodar os comandos abaixo no PowerShell:
# (Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | python -

# $env:PATH += ";$env:USERPROFILE\AppData\Roaming\Python\Scripts"

8. Inicializar o poetry dentro do projeto. Rodar o comando abaixo no VSCode, dentro da pasta do projeto:
# poetry init

9. Criar um ambiente virtual dentro do projeto, para que as versões das dependências do Python neste projeto não apresentem conflitos com versões de outros projetos. Rodar o comando abaixo no VSCode, dentro da pasta do projeto:
# poetry install