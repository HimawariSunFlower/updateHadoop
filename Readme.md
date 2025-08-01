本地没装docker,现场环境没有go环境
传到测试环境(linux),使用docker打包,然后发tar包到正式环境运行
docker build -t update-hadoop .
docker save -o updateHadoop.tar update-hadoop

docker load -i updateHadoop.tar
docker run -d update-hadoop


hdfs dfs -rm /safr/2025-07-27.parquet 