# Facens_Architecture_for_Data_Processing



O objetivo deste projeto é simular em uma escala reduzida a aplicação de tecnologias de Big Data para criação de uma repositório para Sistemas de Recomendação.

Os códigos e comentários deste repositório se referem a parte prática do TCC da pós gradução em Ciências de Dados pela Facens.




# Arquitetura



A arquitetura implementada foi a seguinte:



![Meta arquitetura](images/architecture.png)



## Fonte de Dados



Para este projeto a principal fonte de dados utilizada, trata-se de uma API pública da Steam, com reviews de usuários em relação aos jogos. A API retorna um objeto JSON com as seguintes informações:



![Retorno API](images/steam_api_json_return.png)



A API requer como parâmetro o "appid" do game que se deseja obter as informações, para isso, utilizou-se como ponto de partida um dataset já existente. O dataset em questão foi postado na plataforma do Kaggle pelo usuário Marko M. e contém 21.947.105 registros.



Dataset disponível em: https://www.kaggle.com/datasets/najzeko/steam-reviews-2021



A primeira etapa do trabalho foi carregar todos esses registros para o bucket, na camada Bronze (ingestão).



![Ingestão do dataset base em .csv](images/minio/csv_ingestion.png)



Após isso, foi aplicado um Spark job para levar os dados para a camada Silver, na qual foi aplicado um agrupamento dos dados por APPID e convertido para .parquet.



Desta forma identificou-se inicialmente que no dataset havia 65.580 potenciais jogos. Observou-se também uma redução significativa no volume dos dados.



![Dados do dataset convertidos para .parquet na camada Silver](images/minio/csv_partition.png)



Os dados ficaram organizados da seguinte forma neste bucket.

![Exibindo organização dos arquivos na camada Silver](images/minio/appid_on_silver.png)



Após isso, foi realizado um mapeamento dos nomes e dos id's de recomendação localizados na base, toda essa informação foi disponibilizada em uma coleção no MongoDB.



Desta forma é possível ter alguns meta dados sobre os dados armazenamos no bucket.



![Metadados dos jogos no MongoDB](images/metadata.png)



Realizado este mapeamento a próxima etapa foi filtrar os jogos de interesse para levar para a bucket Gold, onde os dados estão na formatação final desejada.



Foram selecionados inicialmente 320 jogos para serem enviados a camada Gold e iniciar o processo de coleta de dados via API.



![Exibindo organização dos arquivos na camada Silver](images/minio/bucket_gold.png)


# Fase 2 - Ingestão em batch de novos reviews

Após a coleta e tratamento do dataset inicial do projeto, a segunda etapa foi implementar a arquitetura responsavel por coletar os novos reviews do jogos, a partir do ultimo ID identificado anteriormente para os jogos que foram enviados até a camada Gold.

## Kafka

Neste projeto a coleta de review foi implementada com a ferramenta Apache Kafka. 

![Ingestão com Kafka](images/kafka_process.png)










