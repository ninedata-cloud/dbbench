# DBBench REST API 端点清单

## 基准测试生命周期

### POST /api/benchmark/init
初始化基准测试引擎
- **请求体**: 无
- **响应**: `ApiResponse<Void>`
```json
{
  "success": true,
  "message": "Engine initialized",
  "status": "READY"
}
```

### POST /api/benchmark/load
异步加载 TPC-C 测试数据
- **请求体**: 无
- **响应**: `ApiResponse<Void>` (HTTP 202 ACCEPTED)
```json
{
  "success": true,
  "message": "Data loading started",
  "status": "LOADING"
}
```

### GET /api/benchmark/load/progress
获取数据加载进度
- **响应**: `LoadProgressResponse`
```json
{
  "loading": true,
  "progress": 50,
  "message": "Loading warehouse 5/10...",
  "status": "LOADING"
}
```

### POST /api/benchmark/load/cancel
取消数据加载
- **请求体**: 无
- **响应**: `ApiResponse<Void>`

### POST /api/benchmark/clean
清理测试数据
- **请求体**: 无
- **响应**: HTTP 204 No Content

### POST /api/benchmark/start
启动基准测试
- **请求体**: 无
- **响应**: `ApiResponse<Void>`

### POST /api/benchmark/stop
停止基准测试
- **请求体**: 无
- **响应**: `ApiResponse<Void>`

### GET /api/benchmark/status
获取当前状态
- **响应**: `BenchmarkStatusResponse`
```json
{
  "status": "RUNNING",
  "running": true,
  "loading": false,
  "loadProgress": null,
  "loadMessage": null
}
```

## 配置管理

### GET /api/benchmark/config
获取当前配置
- **响应**: `Map<String, Object>`

### POST /api/benchmark/config
更新配置
- **请求体**: `ConfigUpdateRequest`
```json
{
  "database": {
    "type": "mysql",
    "jdbcUrl": "jdbc:mysql://localhost:3306/tpcc",
    "username": "root",
    "password": "password",
    "pool": {
      "size": 50,
      "minIdle": 10
    }
  },
  "benchmark": {
    "warehouses": 10,
    "terminals": 50,
    "duration": 60,
    "rampup": 3,
    "thinkTime": false,
    "loadConcurrency": 4,
    "mix": {
      "newOrder": 45,
      "payment": 43,
      "orderStatus": 4,
      "delivery": 4,
      "stockLevel": 4
    }
  }
}
```
- **响应**: `ApiResponse<Map<String, Object>>`

## 连接测试

### POST /api/benchmark/test-connection
测试数据库连接
- **请求体**: `ConnectionTestRequestWrapper` (嵌套格式)
```json
{
  "database": {
    "type": "dameng",
    "jdbcUrl": "jdbc:dm://8.136.122.73:5236/TPCC",
    "username": "test",
    "password": "test12345678"
  }
}
```
- **响应**: `ConnectionTestResponse`
```json
{
  "success": true,
  "message": "Connection successful (123ms)",
  "database": "dameng",
  "jdbcUrl": "jdbc:dm://8.136.122.73:5236/TPCC",
  "responseTime": 123
}
```

### POST /api/benchmark/test-ssh
测试 SSH 连接
- **请求体**: `SshTestRequest`
```json
{
  "ssh": {
    "enabled": true,
    "host": "8.136.122.73",
    "port": 22,
    "username": "root",
    "password": "password"
  },
  "database": {
    "jdbcUrl": "jdbc:dm://8.136.122.73:5236/TPCC"
  }
}
```
- **响应**: `SshTestResponse`

### GET /api/benchmark/database-types
获取支持的数据库类型列表
- **响应**: `List<Map<String, Object>>`

## 日志管理

### GET /api/benchmark/logs?limit=100
获取活动日志
- **参数**: `limit` (默认 100, 最小 1)
- **响应**: `List<Map<String, Object>>`

### DELETE /api/benchmark/logs
清空日志
- **响应**: HTTP 204 No Content

## 配置文件管理

### GET /api/benchmark/profiles
列出所有配置文件
- **响应**: `List<String>`

### GET /api/benchmark/profiles/{name}
加载指定配置文件
- **响应**: `ProfileResponse`

### POST /api/benchmark/profiles/{name}
保存配置文件
- **请求体**: `Map<String, Object>`
- **响应**: `ApiResponse<Void>` (HTTP 201 CREATED)

### DELETE /api/benchmark/profiles/{name}
删除配置文件
- **响应**: HTTP 204 No Content

### POST /api/benchmark/profiles/{name}/apply
应用配置文件
- **响应**: `ApiResponse<Map<String, Object>>`

## 指标数据

### GET /api/metrics/current
获取当前指标
- **响应**: `Map<String, Object>`

### GET /api/metrics/history?limit=0
获取历史指标
- **参数**: `limit` (0 表示全部)
- **响应**: 指标快照列表

### GET /api/metrics/tps-history?limit=0
获取 TPS 历史数据
- **参数**: `limit` (0 表示全部)
- **响应**: TPS 数据点列表

### GET /api/metrics/hardware-info
获取硬件信息
- **响应**: `Map<String, Object>`

## 报告生成

### GET /api/report/markdown
获取 Markdown 格式报告
- **响应**: `String` (text/markdown)

### GET /api/report/download/markdown
下载 Markdown 报告文件
- **响应**: `byte[]` (附件下载)

## WebSocket

### ws://localhost:1929/ws/metrics
实时指标推送
- 连接后自动推送实时指标更新
- 消息类型: metrics, status, progress, log

## 已知问题和修复

1. ✅ **已修复**: ApiResponse 类名简化
2. ✅ **已修复**: test-connection 端点支持嵌套格式
3. ✅ **已修复**: 全局异常处理
4. ✅ **已修复**: 请求验证
5. ✅ **已修复**: CORS 配置

## 验证清单

请测试以下关键端点：
- [ ] POST /api/benchmark/test-connection (数据库连接测试)
- [ ] POST /api/benchmark/config (配置更新)
- [ ] POST /api/benchmark/init (初始化)
- [ ] GET /api/benchmark/status (状态查询)
- [ ] GET /api/metrics/current (当前指标)
