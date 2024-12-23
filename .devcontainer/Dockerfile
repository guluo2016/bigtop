# 使用 CentOS 作为基础镜像
FROM centos:7

# 安装基本工具
RUN yum -y update && yum -y install \
    git \
    vim \
    curl \
    wget \
    bash-completion \
    sudo

# 安装开发工具（可选）
RUN yum groupinstall -y "Development Tools"

# 设置默认用户和工作目录
WORKDIR /workspace
