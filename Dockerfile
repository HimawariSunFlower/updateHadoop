# 使用alpine作为基础镜像
FROM swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/library/golang:1.23.8-alpine AS builder

# 安装CA证书（用于网络连接）
RUN apk --no-cache add ca-certificates

# 设置工作目录
WORKDIR /root/

# 复制预编译的二进制文件
COPY updateHadoop-linux ./updateHadoop

# 确保二进制文件具有执行权限
RUN chmod +x ./updateHadoop

# 运行应用
CMD ["./updateHadoop"]
