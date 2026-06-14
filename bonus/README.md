# 附加题说明

## 附加题1：监控系统 (+5分)

### 部署步骤（Person B 执行）
```bash
# 1. 加载监控镜像
docker load -i monitoring-all.tar

# 2. 重新打 tag 并推送
SWR="swr.cn-north-4.myhuaweicloud.com/cloud-course-ks"
docker tag swr.cn-east-3.myhuaweicloud.com/cloud-course-2025212245/grafana:12.4.3 ${SWR}/grafana:12.4.3
# ... (其余镜像同理，见离线包 README)
docker push ${SWR}/grafana:12.4.3
# ... (推送所有镜像)

# 3. 替换 monitoring-values.yaml 中的 cloud-course-ks 并安装
helm upgrade --install monitoring kube-prometheus-stack-83.7.0.tgz \
  -n monitoring --create-namespace \
  -f bonus/monitoring/monitoring-values.yaml

# 4. 端口转发访问 Grafana
kubectl port-forward -n monitoring svc/monitoring-grafana 8080:80
# 访问 http://localhost:8080 (admin / admin123456)
```

### Grafana Dashboard 要求
- 节点 CPU 利用率折线图
- 各 Pod 内存使用柱状图
- 报告说明 Prometheus Pull 采集原理 + 3个指标含义

---

## 附加题2：CI/CD 流水线 (+5分)

GitHub Actions 工作流已配置在 `.github/workflows/deploy.yml`。

### 配置步骤（Person B 执行）
1. GitHub 仓库 Settings → Secrets → 添加:
   - `SWR_ORG`: 你的 SWR 组织名
   - `SWR_USERNAME`: SWR 登录用户名
   - `SWR_PASSWORD`: SWR 登录密码（临时Token）
   - `KUBE_CONFIG`: CCE 集群 kubeconfig (base64)
   
2. 推送代码到 main 分支自动触发构建和部署

### 流水线阶段
1. 检出代码
2. 登录 SWR
3. 构建并推送 backend / frontend 镜像
4. 更新 K8s Deployment (滚动更新)

---

## 附加题3：前沿专题 - K3s + MQTT 边缘计算 (+5分)

### 内容要点
- 在本地虚拟机安装 K3s 模拟边缘节点
- 使用 paho-mqtt 将传感器数据通过 MQTT Broker 发布到云端 K8s (Redis 存储)
- 分析 MQTT 协议在弱网环境下的适用性
- 讨论云边协同的延迟挑战
- 不少于 1500 字专题报告

### 代码
见 `bonus/advanced/k3s_mqtt_sensor.py`
