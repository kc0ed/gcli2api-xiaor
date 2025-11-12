import asyncio
import os
import json
import sys
from pathlib import Path

# 将项目根目录加入 Python 路径，以便导入 src 模块
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.storage_adapter import get_storage_adapter
from log import log

CREDS_DIR = Path("creds")

async def main():
    if not CREDS_DIR.exists():
        log.error(f"目录 {CREDS_DIR} 不存在。")
        return

    json_files = list(CREDS_DIR.glob("*.json"))
    if not json_files:
        log.warning(f"在 {CREDS_DIR} 目录下未找到任何 .json 凭证文件。")
        return

    storage = await get_storage_adapter()
    log.info(f"检测到 {len(json_files)} 个凭证文件，开始导入到当前存储后端...")

    for json_path in json_files:
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                credential_data = json.load(f)
            filename = json_path.name
            success = await storage.store_credential(filename, credential_data)
            if success:
                log.info(f"已导入: {filename}")
            else:
                log.error(f"导入失败: {filename}")
        except Exception as e:
            log.error(f"处理文件 {json_path} 时出错: {e}")

    log.info("导入完成。")

if __name__ == "__main__":
    asyncio.run(main())