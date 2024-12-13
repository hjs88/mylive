import requests
from collections import defaultdict
import re
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
import time
import asyncio
import aiohttp
from tqdm import tqdm

# 在文件顶部定义provinces列表
PROVINCES = [
    "浙江", "北京", "上海", "广东", "江苏", "湖南", "山东", 
    "河南", "河北", "安徽", "福建", "重庆", "四川", "贵州", 
    "云南", "陕西", "甘肃", "青海", "内蒙古", "宁夏", "新疆",
    "西藏", "黑龙江", "吉林", "辽宁"
]

def standardize_channel_name(channel_line):
    """标准化频道名称"""
    parts = channel_line.split(',', 1)
    if len(parts) != 2:
        return None
    
    channel_name, url = parts
    
    # 跳过没有名称的频道
    if not channel_name.strip() or channel_name.strip().startswith('http'):
        return None
        
    # 跳过纯数字或者太短的名称
    if channel_name.strip().isdigit() or len(channel_name.strip()) < 2:
        return None
    
    # 标准化CCTV频道名称
    cctv_pattern = r'CCTV-?(\d+).*'
    cctv_match = re.match(cctv_pattern, channel_name, re.IGNORECASE)
    if cctv_match:
        channel_num = cctv_match.group(1)
        # CCTV频道名称对应表
        cctv_names = {
            '1': 'CCTV-1_综合',
            '2': 'CCTV-2_财经',
            '3': 'CCTV-3_综艺',
            '4': 'CCTV-4_中文国际',
            '5': 'CCTV-5_体育',
            '6': 'CCTV-6_电影',
            '7': 'CCTV-7_国防军事',
            '8': 'CCTV-8_电视剧',
            '9': 'CCTV-9_纪录',
            '10': 'CCTV-10_科教',
            '11': 'CCTV-11_戏曲',
            '12': 'CCTV-12_社会与法',
            '13': 'CCTV-13_新闻',
            '14': 'CCTV-14_少儿',
            '15': 'CCTV-15_音乐',
            '16': 'CCTV-16_奥林匹克',
            '17': 'CCTV-17_农业农村'
        }
        channel_name = cctv_names.get(channel_num, f'CCTV-{channel_num}')
    
    # 标准化卫视频道名称
    satellite_pattern = r'(.+)卫视.*'
    satellite_match = re.match(satellite_pattern, channel_name)
    if satellite_match:
        province = satellite_match.group(1)
        channel_name = f'{province}卫视'
    
    return f"{channel_name},{url}"

def categorize_channel(line):
    """根据频道名称对频道进行分类"""
    standardized_channel = standardize_channel_name(line)
    if not standardized_channel:
        return None, None
        
    parts = standardized_channel.split(',', 1)
    if len(parts) != 2:
        return None, None
    
    channel_name = parts[0]
    
    # 定义省份和对应的城市
    province_cities = {
        "浙江": ["浙江", "杭州", "宁波", "温州", "嘉兴", "湖州", "绍兴", "金华", "衢州", "舟山", "台州", "丽水"],
        "北京": ["北京", "BTV"],
        "上海": ["上海", "东方"],
        "广东": ["广东", "广州", "深圳", "珠海", "汕头", "佛山", "韶关", "湛江", "肇庆", "江门", "茂名", "惠州"],
        "江苏": ["江苏", "南京", "苏州", "无锡", "常州", "镇江", "南通", "扬州", "盐城", "徐州", "淮安", "连云港"],
        "湖南": ["湖南", "长沙", "株洲", "湘潭", "衡阳", "邵阳", "岳阳", "常德", "张家界", "益阳", "郴州"],
        "山东": ["山东", "济南", "青岛", "淄博", "枣庄", "东营", "烟台", "潍坊", "济宁", "泰安", "威海", "日照"],
        "河南": ["河南", "郑州", "开封", "洛阳", "平顶山", "安阳", "鹤壁", "新乡", "焦作", "濮阳", "许昌"],
        "河北": ["河北", "石家庄", "唐山", "秦皇岛", "邯郸", "邢台", "保定", "张家口", "承德", "沧州"],
        "安徽": ["安徽", "合肥", "芜湖", "蚌埠", "淮南", "马鞍山", "淮北", "铜陵", "安庆", "黄山"],
        "福建": ["福建", "福州", "厦门", "莆田", "三明", "泉州", "漳州", "南平", "龙岩", "宁德"],
        "重庆": ["重庆"],
        "四川": ["四川", "成都", "自贡", "攀枝花", "泸州", "德阳", "绵阳", "广元", "遂宁", "内江"],
        "贵州": ["贵州", "贵阳", "六盘水", "遵义", "安顺", "毕节", "铜仁"],
        "云南": ["云南", "昆明", "曲靖", "玉溪", "保山", "昭通", "丽江", "普洱", "临沧"],
        "陕西": ["陕西", "西安", "铜川", "宝鸡", "咸阳", "渭南", "延安", "汉中", "榆林"],
        "甘肃": ["甘肃", "兰州", "嘉峪关", "金昌", "白银", "天水", "武威", "张掖", "平凉"],
        "青海": ["青海", "西宁", "海东"],
        "内蒙古": ["内蒙古", "呼和浩特", "包头", "乌海", "赤峰", "通辽", "鄂尔多斯", "呼伦贝尔"],
        "宁夏": ["宁夏", "银川", "石嘴山", "吴忠", "固原", "中卫"],
        "新疆": ["新疆", "乌鲁木齐", "克拉玛依", "吐鲁番", "哈密"],
        "西藏": ["西藏", "拉萨", "日喀则", "昌都", "林芝", "山南"],
        "黑龙江": ["黑龙江", "哈尔滨", "齐齐哈尔", "鸡西", "鹤岗", "双鸭山", "大庆", "伊春"],
        "吉林": ["吉林", "长春", "吉林市", "四平", "辽源", "通化", "白山", "松原"],
        "辽宁": ["辽宁", "沈阳", "大连", "鞍山", "抚顺", "本溪", "丹东", "锦州", "营口"]
    }
    
    # 定义分类规则
    categories = {
        "央视频道": ["CCTV", "央视", "中国中央电视台", "CETV", "CGTN"],
        "卫视频道": ["卫视"],
        "地方频道": [
            "浙江", "北京", "上海", "广东", "深圳", "江苏", "湖南", "山东", 
            "河南", "河北", "安徽", "东方", "东南", "厦门", "重庆", "四川",
            "贵州", "云南", "陕西", "甘肃", "青海", "内蒙古", "宁夏", "新疆",
            "西藏", "黑龙江", "吉林", "辽宁"
        ],
        "港澳台频道": ["香港", "澳门", "台湾", "TVB", "凤凰"],
        "体育频道": ["体育", "SPORT", "ESPN", "NBA"],
        "影视频道": ["电影", "影视", "剧场"],
        "少儿频道": ["少儿", "动画", "卡通"],
        "新闻频道": ["新闻", "NEWS"],
    }
    
    # 先检查是否是卫视频道
    if any(keyword in channel_name for keyword in categories["卫视频道"]):
        return "卫视频道", standardized_channel
    
    # 检查是否属于某个省份的地方频道
    for province, cities in province_cities.items():
        if any(city in channel_name for city in cities):
            return f"{province}频道", standardized_channel
    
    # 检查其他分类
    for category, keywords in categories.items():
        if any(keyword in channel_name for keyword in keywords):
            return category, standardized_channel
    
    return "其他频道", standardized_channel

def standardize_category_name(category):
    """标准化分类名称"""
    # 移除特殊字符和额外的描述
    category = re.sub(r'[•·]', '', category)
    category = re.sub(r'「[^」]*」', '', category)
    category = re.sub(r'\([^\)]*\)', '', category)
    
    # 标准化常见分类名称
    category_mapping = {
        '央视': '央视频道',
        '卫视': '卫视频道',
        '港澳台': '港澳台频道',
        '体育': '体育频道',
        '影视': '影视频道',
        '电影': '影视频道',
        '少儿': '少儿频道',
        '新闻': '新闻频道',
        '其他': '其他频道'
    }
    
    # 处理省份频道的情况
    if any(province in category for province in PROVINCES):
        for province in PROVINCES:
            if province in category:
                return f"{province}频道"
    
    # 使用映射转换标准名称
    for key, value in category_mapping.items():
        if key in category:
            return value
    
    return category.strip()

async def async_check_url(url, timeout=10, max_retries=2):
    """异步检查URL是否有效"""
    for retry in range(max_retries + 1):
        try:
            # 基本URL格式检查
            parsed = urlparse(url)
            if not all([parsed.scheme, parsed.netloc]):
                return False
                
            connector = aiohttp.TCPConnector(force_close=True, enable_cleanup_closed=True)
            async with aiohttp.ClientSession(connector=connector) as session:
                try:
                    # 设置较长的总超时时间，但保持较短的连接超时
                    timeout_obj = aiohttp.ClientTimeout(
                        total=timeout,
                        connect=5,
                        sock_connect=5,
                        sock_read=timeout
                    )
                    
                    headers = {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                        'Connection': 'close'  # 明确告诉服务器关闭连接
                    }
                    
                    # 先尝试HEAD请求
                    try:
                        async with session.head(
                            url, 
                            timeout=timeout_obj,
                            allow_redirects=True,
                            headers=headers
                        ) as response:
                            if response.status == 200:
                                return True
                    except Exception:
                        pass  # 如果HEAD失败，继续尝试GET
                    
                    # 如果HEAD请求失败，尝试GET请求
                    async with session.get(
                        url,
                        timeout=timeout_obj,
                        allow_redirects=True,
                        headers=headers
                    ) as response:
                        return response.status == 200
                        
                except (asyncio.TimeoutError, aiohttp.ClientError):
                    if retry == max_retries:  # 只在最后一次重试失败时返回False
                        return False
                    await asyncio.sleep(1)  # 重试前等待1秒
                    continue
                    
        except Exception as e:
            if retry == max_retries:
                print(f"Error checking URL {url}: {str(e)}")
                return False
            await asyncio.sleep(1)
            continue
    return False

async def async_check_stream_url(channel_line):
    """异步检查直播源是否有效"""
    parts = channel_line.split(',', 1)
    if len(parts) != 2:
        return None
        
    channel_name, url = parts
    url = url.strip()
    
    if await async_check_url(url):
        return channel_line
    return None

async def process_channel_batch_async(channels, max_concurrent=20):
    """异步批量处理频道检查"""
    valid_channels = set()
    tasks = []
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def bounded_check(channel):
        async with semaphore:
            try:
                result = await async_check_stream_url(channel)
                return result
            except Exception as e:
                print(f"Error checking channel: {e}")
                return None
    
    # 分批处理以避免内存问题
    batch_size = 100
    for i in range(0, len(channels), batch_size):
        batch = list(channels)[i:i+batch_size]
        tasks = [bounded_check(channel) for channel in batch]
        
        with tqdm(total=len(batch), desc=f"Checking channels batch {i//batch_size + 1}") as pbar:
            for coro in asyncio.as_completed(tasks):
                try:
                    result = await coro
                    if result:
                        valid_channels.add(result)
                except Exception as e:
                    print(f"Error processing result: {e}")
                finally:
                    pbar.update(1)
        
        # 在批次之间添加短暂延迟
        await asyncio.sleep(1)
    
    return valid_channels

def fetch_and_merge():
    # URL列表
    urls = [
        "https://iptv.b2og.com/txt/fmml_ipv6.txt",
        "https://m3u.ibert.me/txt/y_g.txt",
        "https://ghp.ci/raw.githubusercontent.com/Guovin/iptv-api/gd/output/result.txt",
        "https://iptv.b2og.com/txt/fmml_ipv6.txt",
        "https://ghp.ci/raw.githubusercontent.com/suxuang/myIPTV/main/ipv6.m3u",
        "https://live.zbds.top/tv/iptv6.txt,https://live.zbds.top/tv/iptv4.txt",
        "https://live.fanmingming.com/tv/m3u/ipv6.m3u",
        "https://ghp.ci/https://raw.githubusercontent.com/joevess/IPTV/main/home.m3u8",
        "https://aktv.top/live.txt,http://175.178.251.183:6689/live.txt",
        "https://ghp.ci/https://raw.githubusercontent.com/kimwang1978/collect-tv-txt/main/merged_output.txt",
        "https://m3u.ibert.me/txt/fmml_dv6.txt",
        "https://m3u.ibert.me/txt/o_cn.txt",
        "https://m3u.ibert.me/txt/j_iptv.txt",
        "https://ghp.ci/https://raw.githubusercontent.com/xzw832/cmys/main/S_CCTV.txt",
        "https://ghp.ci/https://raw.githubusercontent.com/xzw832/cmys/main/S_weishi.txt",
        "https://ghp.ci//https://raw.githubusercontent.com/asdjkl6/tv/tv/.m3u/整套直播源/测试/整套直播源/l.txt",
        "https://ghp.ci//https://raw.githubusercontent.com/asdjkl6/tv/tv/.m3u/整套直播源/测试/整套直播源/kk.txt",
        "http://xhztv.top/new.txt",
        "https://live.zbds.top/tv/iptv4.txt",
        "https://4708.kstore.space/zhibo/tv.txt",
        "https://live.iptv365.org/live.txt",
        "https://raw.kkgithub.com/contractduncan/IPTV/main/%E5%B9%BF%E4%B8%9C%E8%B0%B7%E8%B1%86.txt"
    ]
    
    # 使用defaultdict来存储不同分类的频道
    categorized_channels = defaultdict(set)
    
    for url in urls:
        try:
            print(f"\nFetching {url}...")
            response = requests.get(url, timeout=10, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Connection': 'close'
            })
            if response.status_code == 200:
                content = response.text
                lines = content.splitlines()
                current_category = None
                current_batch = set()
                
                for line in lines:
                    stripped_line = line.strip()
                    if not stripped_line:
                        continue
                    
                    # 处理分类标题行
                    if '#genre#' in stripped_line:
                        # 处理前一个分类的频道
                        if current_batch:
                            print(f"\nProcessing {len(current_batch)} channels in {current_category}...")
                            valid_channels = asyncio.run(process_channel_batch_async(current_batch))
                            if valid_channels:
                                categorized_channels[current_category].update(valid_channels)
                            current_batch.clear()
                        
                        current_category = standardize_category_name(stripped_line.split(',')[0])
                        continue
                    
                    # 处理频道行
                    if stripped_line and not stripped_line.startswith('#'):
                        if current_category:
                            # 使用已有分类
                            standardized_channel = standardize_channel_name(stripped_line)
                            if standardized_channel:
                                current_batch.add(standardized_channel)
                        else:
                            # 使用自动分类
                            category, channel = categorize_channel(stripped_line)
                            if category and channel:
                                category = standardize_category_name(category)
                                current_batch.add(channel)
                
                # 处理最后一个分类的频道
                if current_batch:
                    print(f"\nProcessing {len(current_batch)} channels in {current_category or 'uncategorized'}...")
                    valid_channels = asyncio.run(process_channel_batch_async(current_batch))
                    if valid_channels:
                        categorized_channels[current_category or "其他频道"].update(valid_channels)
                
        except Exception as e:
            print(f"Error fetching {url}: {e}")
    
    # 输出统计信息
    print("\n" + "="*50)
    total_channels = sum(len(channels) for channels in categorized_channels.values())
    print(f"\nTotal valid channels found: {total_channels}")
    for category, channels in categorized_channels.items():
        print(f"{category}: {len(channels)} channels")
    print("="*50 + "\n")
    
    # 创建输出目录
    output_dir = os.path.join(os.path.dirname(__file__), "..", "txt")
    os.makedirs(output_dir, exist_ok=True)
    
    # 更新分类顺序
    category_order = [
        "央视频道",
        "卫视频道"
    ]
    # 添加所有省份频道
    category_order.extend([f"{province}频道" for province in PROVINCES])
    # 添加其他分类
    category_order.extend([
        "港澳台频道",
        "体育频道",
        "影视频道",
        "少儿频道",
        "新闻频道",
        "其他频道"
    ])
    
    # 写入分类后的文件
    output_file = os.path.join(output_dir, "merged_file.txt")
    with open(output_file, "w", encoding='utf-8') as file:
        for category in category_order:
            if category in categorized_channels:
                # 写入分类标题
                file.write(f"{category},#genre#\n")
                # 写入该分类下的所有频道
                for channel in sorted(categorized_channels[category]):
                    file.write(channel + "\n")
                # 添加空行分隔不同分类
                file.write("\n")

if __name__ == "__main__":
    start_time = time.time()
    fetch_and_merge()
    end_time = time.time()
    print(f"\nTotal processing time: {end_time - start_time:.2f} seconds")