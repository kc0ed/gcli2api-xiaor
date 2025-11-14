import asyncio
import json
import os
import toml
from typing import Dict, Any
from dotenv import load_dotenv, find_dotenv

# 强制重新加载 .env 文件
load_dotenv(find_dotenv(), override=True)

# 确保可以从根目录运行此脚本
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.storage.file_storage_manager import FileStorageManager
from src.storage.redis_manager import RedisManager
from log import log

async def migrate_data():
    """
    将本地文件存储的凭证和状态迁移到Redis。
    """
    log.info("开始数据迁移：从本地文件 -> Redis")

    # 1. 初始化源（本地文件）和目标（Redis）管理器
    try:
        # 本地文件管理器
        file_manager = FileStorageManager()
        await file_manager.initialize()
        log.info("本地文件存储管理器初始化成功。")

        # Redis管理器
        redis_manager = RedisManager()
        await redis_manager.initialize()
        log.info("Redis存储管理器初始化成功。")

    except Exception as e:
        log.error(f"初始化存储管理器失败: {e}")
        return

    # 2. 获取所有本地凭证文件名
    try:
        local_cred_files = await file_manager.list_credentials()
        if not local_cred_files:
            log.warning("在本地 'creds' 目录中没有找到任何凭证文件。")
            return
        log.info(f"发现 {len(local_cred_files)} 个本地凭证文件。")
    except Exception as e:
        log.error(f"列出本地凭证文件失败: {e}")
        return

    # 3. 获取所有本地状态
    try:
        local_states = await file_manager.get_all_credential_states()
        log.info(f"成功加载本地 {len(local_states)} 条状态记录。")
    except Exception as e:
        log.error(f"加载本地状态文件 'creds_state.toml' 失败: {e}")
        local_states = {}

    # 4. 开始迁移
    success_count = 0
    fail_count = 0

    for filename in local_cred_files:
        try:
            log.info(f"正在处理文件: {filename}...")

            # a. 获取凭证内容
            credential_data = await file_manager.get_credential(filename)
            if not credential_data:
                log.warning(f"  -> 跳过：无法读取凭证内容。")
                fail_count += 1
                continue

            # b. 获取对应的状态
            # toml文件中的键是规范化的，没有路径
            normalized_filename = os.path.basename(filename)
            state_data = local_states.get(normalized_filename, {})
            if not state_data:
                log.warning(f"  -> 未找到对应的状态记录，将使用空状态。")

            # c. 写入Redis
            # 写入凭证内容
            await redis_manager.store_credential(normalized_filename, credential_data)
            log.info(f"  -> 凭证内容已成功写入Redis。")

            # 写入状态
            if state_data:
                await redis_manager.update_credential_state(normalized_filename, state_data)
                log.info(f"  -> 凭证状态已成功写入Redis。")

            success_count += 1
            log.info(f"  -> 文件 {filename} 迁移成功！")

        except Exception as e:
            log.error(f"  -> 迁移文件 {filename} 失败: {e}")
            fail_count += 1

    # 5. 打印最终报告
    log.info("="*50)
    log.info("数据迁移完成！")
    log.info(f"总计文件: {len(local_cred_files)}")
    log.info(f"成功迁移: {success_count}")
    log.info(f"失败: {fail_count}")
    log.info("="*50)
    
    if fail_count == 0 and success_count > 0:
        log.info("所有数据已成功迁移到Redis。")
        log.info("你可以更新你的 .env 文件，设置 MONGODB_URI 指向你的Redis服务器，然后重启主程序。")
    else:
        log.warning("部分文件迁移失败，请检查上面的日志。")


if __name__ == "__main__":
    # 确保在运行前设置了正确的环境变量，特别是 MONGODB_URI
    if not os.getenv("MONGODB_URI"):
        print("错误：请在运行此脚本前，在 .env 文件或环境变量中设置 MONGODB_URI。")
        print("示例: MONGODB_URI=redis://localhost:6379/0")
    else:
        asyncio.run(migrate_data())