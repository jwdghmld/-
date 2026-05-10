import os
import random
import time
import itertools

# ================= 配置区 =================
TARGET_MB = 10  # 生成文件大小 (1024MB = 1GB)
TARGET_BYTES = TARGET_MB * 1024 * 1024

FILE_BEHAVIOR_IN = 'user_behavior.csv'   #原始用户行为文件
FILE_BEHAVIOR_OUT = 'UserBehavior.csv'

FILE_COMMENT_IN = 'user_comments.csv'    #原始用户评论文件
FILE_COMMENT_OUT = 'UserComments.csv'

# ================= 三层转化漏斗配置 =================
BEHAVIOR_TYPES = ['pv', 'cart', 'buy']
FUNNELS = [
    [34, 33, 33],  # 效果1【完全平均】
    [90, 8, 2],  # 效果2【PV 倾斜】
    [10, 45, 45]  # 效果3【Cart/Buy 主导】
]

# ================= 真实语境造句引擎 =================
POS_PREFIX = ["发货真快，", "物流很赞，", "包装很用心，", "昨天刚下单今天就到了，", "期待了很久，", ""]
POS_CORE = ["质量很不错", "屏幕非常清晰", "味道特别好吃", "东西真的很好", "做工比想象中好", "给的赠品很多"]
POS_SUFFIX = ["，特别喜欢！", "，非常满意。", "，绝对值得买！", "，全五分好评！", "，下次还来回购。"]

NEG_PREFIX = ["物流太慢了，", "包装都破损了，", "等了半个月才到，", "真是无语，", ""]
NEG_CORE = ["质量太差了", "手机太卡了", "里面已经变质了", "做工很乱", "这东西简直是垃圾", "出了故障问客服没个说法",
            "特别难吃"]
NEG_SUFFIX = ["，让人极其失望。", "，必须退货！", "，直接扔了。", "，根本不行，大家别买！", "，还得自己掏钱返修。"]

MIX_PREFIX = ["物流挺快，", "包装一般般，", "看着还可以，", ""]
MIX_CORE = ["但是质量有点差", "不过客服态度很慢", "稍微有点难看", "价格不是很值得"]
MIX_SUFFIX = ["，勉强凑合用吧。", "，习惯性给个好评。", "，总体一般。"]

SHORT_COMMENTS = ["好评！", "不错，喜欢", "太差了", "退货", "一般般", "还行", "默认好评", "质量好，赞"]


def generate_realistic_comment():
    """根据权重随机生成一条高度真实的自然语言评论"""
    comment_type = random.choices(['POS', 'NEG', 'MIX', 'SHORT'], weights=[70, 10, 10, 10], k=1)[0]

    if comment_type == 'POS':
        res = random.choice(POS_PREFIX) + random.choice(POS_CORE) + random.choice(POS_SUFFIX)
    elif comment_type == 'NEG':
        res = random.choice(NEG_PREFIX) + random.choice(NEG_CORE) + random.choice(NEG_SUFFIX)
    elif comment_type == 'MIX':
        res = random.choice(MIX_PREFIX) + random.choice(MIX_CORE) + random.choice(MIX_SUFFIX)
    else:
        res = random.choice(SHORT_COMMENTS)

    res = res.lstrip('，')
    return res[:25]


# ================= 智能编码探测加载器 =================
def load_seeds(filepath):
    """智能探测文件编码并读取原始数据，彻底杜绝乱码"""
    seeds = []
    print(f"正在分析原始文件: {filepath} ...")

    if not os.path.exists(filepath):
        print(f"找不到原始文件: {filepath}")
        return []

    # 1. 探测编码 (优先尝试 utf-8，失败则切到中国区万能的 gb18030)
    best_encoding = 'utf-8-sig'
    try:
        # 去掉 errors='ignore'，如果是 GBK 文件用 utf-8 读必报错，我们借此捕获它
        with open(filepath, 'r', encoding='utf-8-sig') as f:
            for _ in range(100):
                f.readline()
    except UnicodeDecodeError:
        best_encoding = 'gb18030'  # gb18030 是最全的中文编码集，完美兼容 GBK 和 ANSI

    print(f"探测完毕，使用 [{best_encoding}] 编码加载数据...")

    # 2. 正式读取
    with open(filepath, 'r', encoding=best_encoding, errors='ignore') as f:
        for _ in range(500000):
            line = f.readline()
            if not line: break
            if line.strip(): seeds.append(line.strip())

    print(f"成功加载 {len(seeds)} 条无乱码种子数据！\n")
    return seeds


# ================================================================

def generate_zipf_cum_weights(num_items, skewness=1.3):
    """生成幂律分布权重，制造数据倾斜"""
    print(f"正在生成数据倾斜权重矩阵...")
    weights = [1.0 / (i ** skewness) for i in range(1, num_items + 1)]
    random.shuffle(weights)
    return list(itertools.accumulate(weights))


def build_behavior_stress(seeds):
    """生成行为压测表"""
    if not seeds: return
    print(f"开始生成行为表 {FILE_BEHAVIOR_OUT}，目标: {TARGET_MB}MB...")

    cum_weights = generate_zipf_cum_weights(len(seeds), skewness=1.2)

    current_bytes = 0
    with open(FILE_BEHAVIOR_OUT, 'wb') as f:
        f.write(b'\xef\xbb\xbf')  # 写入 BOM 防 Excel 乱码

        while current_bytes < TARGET_BYTES:
            chunk = []
            samples = random.choices(seeds, cum_weights=cum_weights, k=10000)

            for line in samples:
                parts = line.split(',')
                if len(parts) >= 10:
                    current_funnel_weights = random.choice(FUNNELS)
                    parts[3] = random.choices(BEHAVIOR_TYPES, weights=current_funnel_weights, k=1)[0]
                    try:
                        parts[4] = str(int(parts[4]) + random.randint(-86400 * 3, 86400 * 3))
                    except ValueError:
                        pass

                chunk.append((",".join(parts) + "\n").encode('utf-8'))

            data_to_write = b"".join(chunk)
            f.write(data_to_write)
            current_bytes += len(data_to_write)

            if current_bytes % (50 * 1024 * 1024) < 100000:
                print(f"   -> 行为数据生成进度: {current_bytes / 1024 / 1024:.2f} MB")

    print(f"行为表生成完毕！物理大小: {os.path.getsize(FILE_BEHAVIOR_OUT) / 1024 / 1024:.2f} MB\n")


def build_comment_stress(seeds):
    """生成高拟真自然语言评论表"""
    if not seeds: return
    print(f"开始生成拟真评论表 {FILE_COMMENT_OUT}，目标: {TARGET_MB}MB...")

    cum_weights = generate_zipf_cum_weights(len(seeds), skewness=1.1)

    current_bytes = 0
    with open(FILE_COMMENT_OUT, 'wb') as f:
        f.write(b'\xef\xbb\xbf')  # 写入 BOM 防 Excel 乱码

        while current_bytes < TARGET_BYTES:
            chunk = []
            samples = random.choices(seeds, cum_weights=cum_weights, k=10000)

            for line in samples:
                parts = line.split(',')
                if len(parts) >= 3:
                    user_id, goods_id, category_id = parts[0], parts[1], parts[2]
                    realistic_comment = generate_realistic_comment()
                    chunk.append(f"{user_id},{goods_id},{category_id},{realistic_comment}\n".encode('utf-8'))

            data_to_write = b"".join(chunk)
            f.write(data_to_write)
            current_bytes += len(data_to_write)

            if current_bytes % (50 * 1024 * 1024) < 100000:
                print(f"   -> 评论数据生成进度: {current_bytes / 1024 / 1024:.2f} MB")

    print(f"评论表生成完毕！物理大小: {os.path.getsize(FILE_COMMENT_OUT) / 1024 / 1024:.2f} MB\n")


if __name__ == '__main__':
    start_time = time.time()

    behavior_seeds = load_seeds(FILE_BEHAVIOR_IN)
    build_behavior_stress(behavior_seeds)

    comment_seeds = load_seeds(FILE_COMMENT_IN)
    build_comment_stress(comment_seeds)

    print(f"全链路【智能防乱码+极度拟真】压测集备妥！总耗时: {time.time() - start_time:.2f} 秒")
