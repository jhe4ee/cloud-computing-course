# 云计算课程设计 - Flask + Redis Web App on CCE + Spark Analysis

**课程**: 云计算技术 (Cloud Computing Technologies)  
**课程代码**: SCAI004712  
**成员**: 2023112438 邹家豪 / 2023112458 蒋育林  
**分工**: 邹家豪(50%) - 架构设计、代码开发、数据分析、报告撰写 / 蒋育林(50%) - 集群运维、镜像推送、K8s部署验证

---

## 项目结构

```
├── part1/                        # 第一部分：云计算平台搭建
│   ├── backend/                  # Flask 后端 API
│   │   ├── Dockerfile
│   │   ├── app.py
│   │   └── requirements.txt
│   ├── frontend/                 # Nginx 前端
│   │   ├── Dockerfile
│   │   ├── nginx.conf
│   │   └── static/index.html
│   ├── docker-compose.yml        # 本地联调
│   └── k8s/                      # Kubernetes YAML
│       ├── backend-deployment.yaml
│       ├── redis-deployment.yaml
│       ├── frontend-deployment.yaml
│       ├── service.yaml
│       ├── configmap.yaml
│       ├── secret.yaml
│       ├── redis-pvc.yaml
│       └── backend-hpa.yaml
├── part2/                        # 第二部分：并行编程实战
│   └── spark/
│       ├── sparkapplication.yaml
│       ├── wordcount.py
│       ├── data_cleaning.py      # A-1 数据清洗
│       ├── analysis_queries.py   # A-2 Spark SQL 统计分析
│       └── performance_compare.py # A-3 性能对比
├── bonus/                        # 附加题
│   ├── monitoring/               # 附加题1：Prometheus+Grafana
│   │   └── monitoring-values.yaml
│   ├── advanced/                 # 附加题3：前沿专题
│   │   └── k3s_mqtt_sensor.py
│   └── README.md
├── .github/workflows/
│   └── deploy.yml                # 附加题2：CI/CD 流水线
└── README.md
```

## 快速开始

### 1. 本地测试
```bash
cd part1
docker compose up --build
# 访问 http://localhost:5000/api/ping => {"status":"ok"}
```

### 2. 部署到 CCE
```bash
# 替换所有 YAML 中的 cloud-course-ks 为你的 SWR 组织名
# 依次执行:
kubectl apply -f part1/k8s/secret.yaml
kubectl apply -f part1/k8s/configmap.yaml
kubectl apply -f part1/k8s/redis-pvc.yaml
kubectl apply -f part1/k8s/redis-deployment.yaml
kubectl apply -f part1/k8s/backend-deployment.yaml
kubectl apply -f part1/k8s/frontend-deployment.yaml
kubectl apply -f part1/k8s/service.yaml
kubectl apply -f part1/k8s/backend-hpa.yaml
```

### 3. 提交 Spark 作业
```bash
helm install spark-op spark-operator/ -n spark-operator --create-namespace
kubectl apply -f part2/spark/sparkapplication.yaml
```

## 技术栈

| 组件 | 技术 |
|------|------|
| 后端 | Python Flask 3.0 |
| 前端 | Nginx 1.25 Alpine |
| 缓存 | Redis 7 |
| 编排 | Kubernetes (CCE) |
| 大数据 | PySpark 3.4 + Spark Operator |
| 监控 | Prometheus + Grafana |
| CI/CD | GitHub Actions |
