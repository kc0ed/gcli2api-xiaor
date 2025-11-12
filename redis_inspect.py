import os
import sys
import json
from datetime import datetime

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import redis
except ImportError:
    print("❌ 错误: `redis` 库未安装。请运行: pip install redis")
    sys.exit(1)

redis_uri = os.getenv("REDIS_URI")
if not redis_uri:
    print("❌ 错误: REDIS_URI 环境变量未设置。")
    sys.exit(1)

try:
    client = redis.from_url(redis_uri, decode_responses=True)
    client.ping()
    print(f"✅ 成功连接到 Redis: {redis_uri}")
except Exception as e:
    print(f"❌ 连接 Redis 失败: {e}")
    sys.exit(1)

# 读取所有凭证数据
credentials_hash = "gcli2api:credentials"
all_data = client.hgetall(credentials_hash)

if not all_data:
    print(f"⚠️  Redis 哈希表 '{credentials_hash}' 为空。没有凭证数据。")
    sys.exit(0)

print(f"\n📦 发现 {len(all_data)} 条凭证记录：")
print("-" * 80)

for filename, data_str in all_data.items():
    try:
        data = json.loads(data_str)
    except json.JSONDecodeError as e:
        print(f"❌ 解析凭证 '{filename}' 失败: {e}")
        continue

    print(f"\n🔑 凭证文件名: {filename}")
    print("  完整数据:")
    for key, value in data.items():
        # 对时间戳做友好展示
        if "timestamp" in key and isinstance(value, (int, float)):
            dt = datetime.fromtimestamp(value)
            print(f"    {key}: {value}  ({dt})")
        else:
            print(f"    {key}: {value}")
    print("-" * 40)

print("\n✅ 检查完成。")