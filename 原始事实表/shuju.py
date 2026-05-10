import os
import random
import time
import itertools

# ================= 配置区 =================
TARGET_MB = 1024  # 生成文件大小 (1024MB = 1GB)
TARGET_BYTES = TARGET_MB * 1024 * 1024

FILE_BEHAVIOR_IN = 'UserBehavior--1.csv'
FILE_BEHAVIOR_OUT = 'UserBehavior_stress.csv'

FILE_COMMENT_IN = 'user_comments.csv'
FILE_COMMENT_OUT = 'user_comments_stress.csv'

# 【评论加权词库】
WORDS_WEIGHTS = {
    "的": 120, "了": 100, "很": 90, "东西": 80, "质量": 70, "物流": 60, "买": 60, "感觉": 50, "包装": 50, "快递": 40,
    "好": 90, "不错": 70, "喜欢": 60, "满意": 50, "值得": 40, "快": 40, "赞": 20, "清晰": 15, "很多": 15, "好吃": 15,
    "差": 20, "不行": 15, "慢": 15, "垃圾": 10, "失望": 10, "坏": 8, "卡": 5, "退货": 5, "变质": 1, 
    "一般": 25, "还行": 25, "到了": 20, "今天": 10, "有点": 30, "非常": 30, "客服": 15, "态度": 15
}
WORDS_LIST = list(WORDS_WEIGHTS.keys())
WEIGHTS_LIST = list(WORDS_WEIGHTS.values())

# ================= 核心突破：三层转化漏斗配置 =================
BEHAVIOR_TYPES = ['pv', 'cart', 'buy']

# 定义三种截然不同的转化场景 (顺序对应 pv, cart, buy)
FUNNELS = [
    [34, 33, 33],  # 效果1【完全平均】：三分天下，均衡转化
    [90, 8, 2],    # 效果2【PV 倾斜】：光看不买，典型的高客单价/长尾商品特征
    [10, 45, 45]   # 效果3【Cart/Buy 主导】：疯狂扫货，典型的秒杀/刚需特价特征
]
# ==========================================================

def load_seeds(filepath):
    """读取真实原始数据作为种子池"""
    seeds = []
    print(f"📦 正在加载真实种子数据: {filepath} ...")
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for _ in range(500000):
                line = f.readline()
                if not line: break
                if line.strip(): seeds.append(line.strip())
        print(f"✅ 成功加载 {len(seeds)} 条种子数据！")
        return seeds
    except FileNotFoundError:
        print(f"❌ 找不到种子文件: {filepath}")
        return []

def generate_zipf_cum_weights(num_items, skewness=1.3):
    """生成幂律分布权重，制造数据倾斜 (齐夫定律)"""
    print(f"⚙️ 正在生成数据倾斜权重矩阵...")
    weights = [1.0 / (i ** skewness) for i in range(1, num_items + 1)]
    random.shuffle(weights)
    return list(itertools.accumulate(weights))

def build_behavior_stress(seeds):
    """生成包含【三层动态漏斗】的行为压测表"""
    if not seeds: return
    print(f"🚀 开始生成行为表 {FILE_BEHAVIOR_OUT}，目标: {TARGET_MB}MB...")
    
    cum_weights = generate_zipf_cum_weights(len(seeds), skewness=1.2)
    
    current_bytes = 0
    with open(FILE_BEHAVIOR_OUT, 'wb') as f:
        while current_bytes < TARGET_BYTES:
            chunk = []
            samples = random.choices(seeds, cum_weights=cum_weights, k=10000)
            
            for line in samples:
                parts = line.split(',')
                if len(parts) >= 10:
                    
                    # ---------------------------------------------------------
                    # 核心逻辑：先随机转化效果，再随机数据
                    # 1. 掷骰子决定当前这条数据使用哪一种漏斗效果 (均等概率 1/3)
                    current_funnel_weights = random.choice(FUNNELS)
                    
                    # 2. 按照选中的漏斗效果，去随机生成具体行为 (pv/cart/buy)
                    parts[3] = random.choices(BEHAVIOR_TYPES, weights=current_funnel_weights, k=1)[0]
                    # ---------------------------------------------------------
                    
                    # 增加时间戳扰动
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
                
    print(f"🎉 行为表生成完毕！物理大小: {os.path.getsize(FILE_BEHAVIOR_OUT)/1024/1024:.2f} MB\n")

def build_comment_stress(seeds):
    """生成具备齐夫定律分布和词语加权组合的评论表"""
    if not seeds: return
    print(f"🚀 开始生成评论表 {FILE_COMMENT_OUT}，目标: {TARGET_MB}MB...")
    
    cum_weights = generate_zipf_cum_weights(len(seeds), skewness=1.1)
    
    current_bytes = 0
    with open(FILE_COMMENT_OUT, 'wb') as f:
        while current_bytes < TARGET_BYTES:
            chunk = []
            samples = random.choices(seeds, cum_weights=cum_weights, k=10000)
            
            for line in samples:
                parts = line.split(',')
                if len(parts) >= 3:
                    user_id, goods_id, category_id = parts[0], parts[1], parts[2]
                    
                    chosen_words = random.choices(WORDS_LIST, weights=WEIGHTS_LIST, k=random.randint(2, 8))
                    truncated_comment = "".join(chosen_words)[:25]
                    
                    chunk.append(f"{user_id},{goods_id},{category_id},{truncated_comment}\n".encode('utf-8'))
            
            data_to_write = b"".join(chunk)
            f.write(data_to_write)
            current_bytes += len(data_to_write)
            
            if current_bytes % (50 * 1024 * 1024) < 100000:
                print(f"   -> 评论数据生成进度: {current_bytes / 1024 / 1024:.2f} MB")

    print(f"🎉 评论表生成完毕！物理大小: {os.path.getsize(FILE_COMMENT_OUT)/1024/1024:.2f} MB\n")

if __name__ == '__main__':
    start_time = time.time()
    
    behavior_seeds = load_seeds(FILE_BEHAVIOR_IN)
    build_behavior_stress(behavior_seeds)
    
    comment_seeds = load_seeds(FILE_COMMENT_IN)
    build_comment_stress(comment_seeds)
    
    print(f"🏆 全链路【多态漏斗+数据倾斜】压测集备妥！总耗时: {time.time() - start_time:.2f} 秒")