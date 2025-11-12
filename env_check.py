import os
import sys

try:
    from dotenv import load_dotenv
    print("✅ [1/5] `python-dotenv` 库已安装。")
except ImportError:
    print("❌ [1/5] 错误: `python-dotenv` 库未安装。")
    print("   请在你的虚拟环境中运行: pip install python-dotenv")
    sys.exit(1)

try:
    import redis
    print("✅ [2/5] `redis` 库已安装。")
except ImportError:
    print("❌ [2/5] 错误: `redis` 库未安装。")
    print("   请在你的虚拟环境中运行: pip install redis")
    sys.exit(1)

# 尝试加载 .env 文件
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
    print(f"✅ [3/5] 已加载 .env 文件: {dotenv_path}")
else:
    print("⚠️  [3/5] 警告: 未找到 .env 文件，将仅依赖系统环境变量。")

# 读取 REDIS_URI
redis_uri = os.getenv("REDIS_URI")
if not redis_uri:
    print("❌ [4/5] 致命错误: 环境变量 REDIS_URI 未设置！")
    print("   请在 .env 文件或系统环境变量中设置 REDIS_URI，例如：")
    print("   REDIS_URI=redis://username:password@your-redis-host:6379/0")
    sys.exit(1)

print(f"✅ [4/5] 检测到 REDIS_URI: {redis_uri}")

# 尝试连接 Redis
try:
    client = redis.from_url(redis_uri, decode_responses=True)
    client.ping()
    print("✅ [5/5] Redis 连接成功！")
    print("\n🎉 环境检查全部通过。程序应该能正常连接到远程 Redis。")
except Exception as e:
    print(f"❌ [5/5] Redis 连接失败: {e}")
    print("\n请检查：")
    print("1. REDIS_URI 是否正确（主机、端口、密码、数据库索引）")
    print("2. 网络是否可达（防火墙、云厂商安全组）")
    print("3. Redis 服务是否正常运行")
    sys.exit(1)