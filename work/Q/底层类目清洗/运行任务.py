# run_all.py
# -*- coding: utf-8 -*-

import subprocess
import sys
import time

def run_script(script_name):
    print(f"\n==============================")
    print(f"开始执行: {script_name}")
    print(f"==============================\n")

    result = subprocess.run(
        [sys.executable, script_name],
        stdout=sys.stdout,
        stderr=sys.stderr
    )

    if result.returncode != 0:
        print(f"\n❌ {script_name} 执行失败")
        sys.exit(1)

    print(f"\n✅ {script_name} 执行完成\n")
    time.sleep(2)


if __name__ == "__main__":

    # 先生成 top50
    run_script(r"D:\gg_xm\Q\看板\top.py")

    # 再统计指标
    run_script(r"D:\gg_xm\Q\底层类目清洗\top50.py")

    # 插入plus
    run_script(r"D:\gg_xm\Q\底层类目清洗\插入plus.py")

    print("\n🎉 所有任务执行完成")
