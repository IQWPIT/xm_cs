# run_all.py
# -*- coding: utf-8 -*-

import subprocess
import sys
import time

def run_script(script_name):
    print(f"\n==============================")
    print(f"开始执行: {script_name}")
    print(f"==============================\n")

    t0 = time.time()
    result = subprocess.run(
        [sys.executable, script_name],
        stdout=sys.stdout,
        stderr=sys.stderr
    )
    elapsed = time.time() - t0

    if result.returncode != 0:
        print(f"\n❌ {script_name} 执行失败（耗时 {elapsed:.1f}s）")
        sys.exit(1)

    print(f"\n✅ {script_name} 执行完成（耗时 {elapsed:.1f}s）\n")


if __name__ == "__main__":
    total_start = time.time()

    # 1. 生成 top50 SKU 列表
    run_script(r"top50_sku.py")

    # 2. 统计 top50 指标，写入 _big_Excel
    run_script(r"top50.py")

    # 3. 统计全量指标，写入 _big_Excel
    run_script(r"plus.py")

    # 4. 统计全量其他指标，写入 _big_Excel
    run_script(r"all.py")

    # 5. 将 _big_Excel 数据回写到 visual_plus（依赖步骤 2/3/4 完成）
    run_script(r"c_plus.py")

    print(f"\n所有任务执行完成（总耗时 {time.time() - total_start:.1f}s）")
